#!/usr/bin/env bash
#
# Cloud Cache E2E Test
#
# Tests the cloud cache module by using mc CLI to perform operations
# against ESS (which proxies to an upstream MinIO) and verifying
# results on both sides.
#
# Prerequisites:
#   - mc CLI installed and in PATH
#   - ESS running on ESS_S3_PORT (default 9000) with cloud cache configured
#   - MinIO running on MINIO_PORT (default 9100) as upstream
#
# Environment variables:
#   ESS_S3_ENDPOINT     — ESS S3 endpoint (default http://localhost:9000)
#   MINIO_ENDPOINT      — MinIO endpoint (default http://localhost:9100)
#   MINIO_ACCESS_KEY    — MinIO root user (default minioadmin)
#   MINIO_SECRET_KEY    — MinIO root password (default minioadmin)
#   ESS_ACCESS_KEY      — ESS access key (default from E2E_ACCESS_KEY_ID)
#   ESS_SECRET_KEY      — ESS secret key (default from E2E_SECRET_ACCESS_KEY)
#   LOCAL_BUCKET        — ESS bucket name (default cloud-e2e)
#   REMOTE_BUCKET       — MinIO upstream bucket name (default upstream-e2e)

set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────
ESS_S3_ENDPOINT="${ESS_S3_ENDPOINT:-http://localhost:9000}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9100}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
ESS_ACCESS_KEY="${ESS_ACCESS_KEY:-${E2E_ACCESS_KEY_ID:-}}"
ESS_SECRET_KEY="${ESS_SECRET_KEY:-${E2E_SECRET_ACCESS_KEY:-}}"
LOCAL_BUCKET="${LOCAL_BUCKET:-cloud-e2e}"
REMOTE_BUCKET="${REMOTE_BUCKET:-upstream-e2e}"

PASS_COUNT=0
FAIL_COUNT=0

# Temp dir for mc cp uploads (mc pipe doesn't send Authorization headers)
E2E_TMPDIR=$(mktemp -d)
trap 'rm -rf "$E2E_TMPDIR"' EXIT

# ── Helpers ────────────────────────────────────────────────────────
pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  echo "  ✅ PASS: $1"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "  ❌ FAIL: $1"
}

# Upload content string to an mc destination using mc cp (not mc pipe).
# Usage: mc_put "content" "ess/bucket/key"
mc_put() {
  local content="$1" dest="$2"
  local tmpfile
  tmpfile=$(mktemp "$E2E_TMPDIR/upload.XXXXXX")
  echo -n "$content" > "$tmpfile"
  mc cp "$tmpfile" "$dest" 2>&1
  rm -f "$tmpfile"
}

assert_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    pass "$desc"
  else
    fail "$desc (expected: '$expected', got: '$actual')"
  fi
}

assert_contains() {
  local desc="$1" needle="$2" haystack="$3"
  if echo "$haystack" | grep -qF "$needle"; then
    pass "$desc"
  else
    fail "$desc (expected to contain '$needle')"
  fi
}

assert_not_contains() {
  local desc="$1" needle="$2" haystack="$3"
  if echo "$haystack" | grep -qF "$needle"; then
    fail "$desc (expected NOT to contain '$needle')"
  else
    pass "$desc"
  fi
}

assert_file_exists() {
  local desc="$1" alias_bucket="$2" key="$3"
  local listing
  listing=$(mc ls "$alias_bucket" 2>&1 || true)
  if echo "$listing" | grep -qF "$key"; then
    pass "$desc"
  else
    fail "$desc ($key not found in listing)"
  fi
}

assert_file_not_exists() {
  local desc="$1" alias_bucket="$2" key="$3"
  local listing
  listing=$(mc ls "$alias_bucket" 2>&1 || true)
  if echo "$listing" | grep -qF "$key"; then
    fail "$desc ($key still in listing)"
  else
    pass "$desc"
  fi
}

summary() {
  echo ""
  echo "══════════════════════════════════════════════"
  echo "  Results: ${PASS_COUNT} passed, ${FAIL_COUNT} failed"
  echo "══════════════════════════════════════════════"
  if [[ $FAIL_COUNT -gt 0 ]]; then
    exit 1
  fi
}

# ── Setup mc aliases ───────────────────────────────────────────────
echo "═══ Setting up mc aliases ═══"

mc alias set ess "$ESS_S3_ENDPOINT" "$ESS_ACCESS_KEY" "$ESS_SECRET_KEY" --api S3v4 2>&1
mc alias set minio-upstream "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api S3v4 2>&1

echo ""

# ── 1. File Operations ────────────────────────────────────────────
echo "═══ 1. File Operations ═══"

# 1.1 Create file
echo "── 1.1 Create file ──"
mc_put "hello cloud cache" "ess/${LOCAL_BUCKET}/test-file.txt"
sleep 1

listing_ess=$(mc ls "ess/${LOCAL_BUCKET}/" 2>&1)
assert_contains "File visible on ESS" "test-file.txt" "$listing_ess"

listing_minio=$(mc ls "minio-upstream/${REMOTE_BUCKET}/" 2>&1)
assert_contains "File visible on upstream MinIO" "test-file.txt" "$listing_minio"

# 1.2 Download file
echo "── 1.2 Download file ──"
content=$(mc cat "ess/${LOCAL_BUCKET}/test-file.txt" 2>&1)
assert_eq "Downloaded content matches" "hello cloud cache" "$content"

# 1.3 Update file (overwrite)
echo "── 1.3 Update file ──"
mc_put "updated content" "ess/${LOCAL_BUCKET}/test-file.txt"
sleep 1

content=$(mc cat "ess/${LOCAL_BUCKET}/test-file.txt" 2>&1)
assert_eq "Updated content on ESS" "updated content" "$content"

content_upstream=$(mc cat "minio-upstream/${REMOTE_BUCKET}/test-file.txt" 2>&1)
assert_eq "Updated content on upstream" "updated content" "$content_upstream"

# 1.4 Copy file
echo "── 1.4 Copy file ──"
mc cp "ess/${LOCAL_BUCKET}/test-file.txt" "ess/${LOCAL_BUCKET}/test-file-copy.txt" 2>&1
sleep 1

listing_ess=$(mc ls "ess/${LOCAL_BUCKET}/" 2>&1)
assert_contains "Copied file on ESS" "test-file-copy.txt" "$listing_ess"

# 1.5 Move file (rename)
echo "── 1.5 Move file ──"
mc mv "ess/${LOCAL_BUCKET}/test-file-copy.txt" "ess/${LOCAL_BUCKET}/test-file-moved.txt" 2>&1
sleep 1

listing_ess=$(mc ls "ess/${LOCAL_BUCKET}/" 2>&1)
assert_contains "Moved file present on ESS" "test-file-moved.txt" "$listing_ess"
assert_not_contains "Original file gone after move" "test-file-copy.txt" "$listing_ess"

# 1.6 Remove file
echo "── 1.6 Remove file ──"
mc rm "ess/${LOCAL_BUCKET}/test-file-moved.txt" 2>&1
sleep 1

listing_ess=$(mc ls "ess/${LOCAL_BUCKET}/" 2>&1)
assert_not_contains "Removed file gone from ESS" "test-file-moved.txt" "$listing_ess"

listing_minio=$(mc ls "minio-upstream/${REMOTE_BUCKET}/" 2>&1)
assert_not_contains "Removed file gone from upstream" "test-file-moved.txt" "$listing_minio"

# Clean up remaining file
mc rm "ess/${LOCAL_BUCKET}/test-file.txt" 2>&1 || true
sleep 1

echo ""

# ── 2. Directory Operations ───────────────────────────────────────
echo "═══ 2. Directory Operations ═══"

# 2.1 Create directory with files
echo "── 2.1 Create directory with files ──"
mc_put "file-a content" "ess/${LOCAL_BUCKET}/mydir/file-a.txt"
mc_put "file-b content" "ess/${LOCAL_BUCKET}/mydir/file-b.txt"
mc_put "sub file content" "ess/${LOCAL_BUCKET}/mydir/subdir/file-c.txt"
sleep 1

listing_ess=$(mc ls "ess/${LOCAL_BUCKET}/" 2>&1)
assert_contains "Directory visible on ESS" "mydir/" "$listing_ess"

listing_minio=$(mc ls "minio-upstream/${REMOTE_BUCKET}/" 2>&1)
assert_contains "Directory visible on upstream" "mydir/" "$listing_minio"

# 2.2 List directory contents
echo "── 2.2 List directory contents ──"
listing_dir=$(mc ls "ess/${LOCAL_BUCKET}/mydir/" 2>&1)
assert_contains "file-a in dir listing" "file-a.txt" "$listing_dir"
assert_contains "file-b in dir listing" "file-b.txt" "$listing_dir"
assert_contains "subdir in dir listing" "subdir/" "$listing_dir"

# 2.3 Download directory files
echo "── 2.3 Download directory files ──"
content_a=$(mc cat "ess/${LOCAL_BUCKET}/mydir/file-a.txt" 2>&1)
assert_eq "Dir file-a content" "file-a content" "$content_a"

content_c=$(mc cat "ess/${LOCAL_BUCKET}/mydir/subdir/file-c.txt" 2>&1)
assert_eq "Subdir file-c content" "sub file content" "$content_c"

# 2.4 Copy directory (mc cp --recursive)
echo "── 2.4 Copy directory ──"
mc cp --recursive "ess/${LOCAL_BUCKET}/mydir/" "ess/${LOCAL_BUCKET}/mydir-copy/" 2>&1
sleep 1

listing_ess=$(mc ls "ess/${LOCAL_BUCKET}/" 2>&1)
assert_contains "Copied dir on ESS" "mydir-copy/" "$listing_ess"

content_copy=$(mc cat "ess/${LOCAL_BUCKET}/mydir-copy/file-a.txt" 2>&1)
assert_eq "Copied dir file content" "file-a content" "$content_copy"

# 2.5 Move directory
echo "── 2.5 Move directory ──"
mc mv --recursive "ess/${LOCAL_BUCKET}/mydir-copy/" "ess/${LOCAL_BUCKET}/mydir-moved/" 2>&1
sleep 1

listing_ess=$(mc ls "ess/${LOCAL_BUCKET}/" 2>&1)
assert_contains "Moved dir present" "mydir-moved/" "$listing_ess"
assert_not_contains "Original dir gone after move" "mydir-copy/" "$listing_ess"

# 2.6 Remove directory recursively
echo "── 2.6 Remove directory ──"
mc rm --recursive --force "ess/${LOCAL_BUCKET}/mydir-moved/" 2>&1
sleep 1

listing_ess=$(mc ls "ess/${LOCAL_BUCKET}/" 2>&1)
assert_not_contains "Removed dir gone from ESS" "mydir-moved/" "$listing_ess"

# 2.7 Remove original directory
echo "── 2.7 Remove original directory ──"
mc rm --recursive --force "ess/${LOCAL_BUCKET}/mydir/" 2>&1
sleep 1

listing_ess=$(mc ls "ess/${LOCAL_BUCKET}/" 2>&1)
assert_not_contains "Original dir gone from ESS" "mydir/" "$listing_ess"

listing_minio=$(mc ls "minio-upstream/${REMOTE_BUCKET}/" 2>&1)
assert_not_contains "Original dir gone from upstream" "mydir/" "$listing_minio"

echo ""

# ── Summary ───────────────────────────────────────────────────────
summary
