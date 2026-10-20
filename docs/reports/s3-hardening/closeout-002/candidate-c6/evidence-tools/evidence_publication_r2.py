#!/usr/bin/env python3
"""Private C6 publication preparation; never an acceptance verifier.

Do not run review until the coordinating owner confirms serial heavy lanes stopped.
Self-test touches only synthetic, automatically removed temporary fixtures.
Review reads only the fixed evidence scope and explicitly named credential JSONs.
"""
import argparse
import collections
import contextlib
import datetime
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile

CHUNK = 1024 * 1024
MAX_VALUE = 65536
MAX_CREDENTIALS = 4096
MARKER = b"[REDACTED_CREDENTIAL]"
REPORTS = Path("docs/reports/s3-hardening")
CANDIDATE = REPORTS / "closeout-002/candidate-c6"
PUBLICATION = CANDIDATE / "evidence-publication"
C5 = REPORTS / "closeout-002/candidate-c5/evidence-publication/manifest.json"
KEYS = (b"access_key", b"secret_key", b"access_key_id", b"secret_access_key",
        b"aws_access_key_id", b"aws_secret_access_key", b"encrypted_secret",
        b"session_token", b"security_token", b"aws_session_token",
        b"authorization", b"proxy-authorization", b"cookie", b"set-cookie")
KEY_PATTERN = b"(?:" + b"|".join(sorted(KEYS, key=len, reverse=True)) + b")"
# JSON, Python repr and Elixir map/struct quoted credential fields.
FIELDS = re.compile(rb"(?<![A-Za-z0-9_])(?P<key>" + KEY_PATTERN +
                    rb")[\"']?\s*(?::|=>|=)\s*(?P<quote>[\"'])(?P<value>(?:\\.|[^\\\r\n]){0,65536}?)(?P=quote)", re.I)
HEADERS = re.compile(rb"(?im)(?:^|[\r\n])[ \t]*(?P<key>authorization|proxy-authorization|cookie|set-cookie)\s*:\s*(?P<value>[^\r\n]{1,65536})")
QUERIES = re.compile(rb"(?i)(?P<key>X-Amz-(?:Credential|Signature|Security-Token)|AWSAccessKeyId|Signature|SecurityToken)=(?P<value>[^&\s\"'<>]{1,65536})")
AUTH = re.compile(rb"(?P<value>AWS4-HMAC-SHA256[ \t]+Credential=[^\r\n\"']{1,65536})")
PATTERNS = (FIELDS, HEADERS, QUERIES, AUTH)


class ReviewError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise ReviewError(message)


def digest(path, decompressed=False):
    h, size = hashlib.sha256(), 0
    opener = gzip.open if decompressed else open
    with opener(path, "rb") as source:
        while chunk := source.read(CHUNK):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def info(path):
    sha, size = digest(path)
    return {"sha256": sha, "bytes": size}


def assert_regular(path):
    require(not path.is_symlink() and path.is_file(), "Non-regular or symlink input rejected")


def inventory(repo):
    reports = repo / REPORTS
    roots = sorted(reports.glob("closeout002-c6*"))
    if (repo / CANDIDATE).exists():
        roots.append(repo / CANDIDATE)
    require(roots, "No C6 evidence roots found")
    result = []
    for root in roots:
        require(not root.is_symlink(), "Symlink evidence root rejected")
        candidates = [root] if root.is_file() else sorted(root.rglob("*"))
        for path in candidates:
            if path.is_relative_to(repo / PUBLICATION):
                continue
            require(not path.is_symlink(), "Symlink evidence entry rejected")
            if path.is_dir():
                continue
            assert_regular(path)
            result.append(path)
    return roots, sorted(set(result))


def credential_matches(data):
    for pattern in PATTERNS:
        for match in pattern.finditer(data):
            value = match.group("value")
            if value and value != MARKER:
                key = match.groupdict().get("key", b"sigv4_authorization").lower().decode("ascii")
                yield key, value


def chunks(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as source:
        while chunk := source.read(CHUNK):
            yield chunk


def discovered_values(path):
    # Values and header lines are capped; process overlap only once per match.
    tail = b""
    for chunk in chunks(path):
        data = tail + chunk
        cutoff = max(0, len(data) - MAX_VALUE - 1024)
        # Retain an entire boundary-crossing token by splitting at a newline.
        # Huge newline-free records are rejected instead of silently under-scanned.
        if cutoff:
            split = data.rfind(b"\n", 0, cutoff)
            if split < 0:
                raise ReviewError("Oversized newline-free credential scan record")
            for key, value in credential_matches(data[:split + 1]):
                yield key, value
            tail = data[split + 1:]
        else:
            tail = data
        require(len(tail) <= CHUNK + MAX_VALUE + 1024, "Credential scan record too large")
    yield from credential_matches(tail)


def fixture_values(paths, allowed_roots):
    values, rows, fields = set(), [], collections.Counter()
    for path in paths:
        require(path.is_absolute(), "Credential fixture requires exact absolute path")
        require(path.name == "credentials.json" and path.parent.name == "fixture",
                "Only exact retained restore fixture/credentials.json is allowed")
        require(any(path.is_relative_to(root) for root in allowed_roots),
                "Credential fixture is outside explicitly allowed roots")
        # Resolve without permitting symlink traversal to a different tree.
        require(path.resolve() == path, "Credential fixture symlink traversal rejected")
        assert_regular(path)
        require(path.stat().st_size <= CHUNK, "Credential fixture exceeds 1 MiB bound")
        content = json.loads(path.read_bytes())
        local = collections.Counter()

        def walk(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key.encode() in KEYS and isinstance(item, str) and item:
                        encoded = item.encode()
                        require(len(encoded) <= MAX_VALUE, "Credential fixture value exceeds bound")
                        values.add(encoded)
                        local[key] += 1
                    else:
                        walk(item)
            elif isinstance(value, list):
                for item in value:
                    walk(item)
        walk(content)
        require(local, "No recognized credential fields in named fixture")
        fields.update(local)
        rows.append({**info(path), "credential_fields": dict(local)})
    return values, rows, dict(fields)


def exact_occurrences(path, values):
    if not values:
        return 0
    overlap = max(map(len, values)) - 1
    tail, count = b"", 0
    # Count matches by their ending position to avoid overlap double-counting.
    for chunk in chunks(path):
        data = tail + chunk
        for value in values:
            start = 0
            while (position := data.find(value, start)) >= 0:
                if position + len(value) > len(tail):
                    count += 1
                start = position + len(value)
        tail = data[-overlap:] if overlap else b""
    return count


def redact(source, target, values):
    # Streaming substitution preserves bytes outside exact sensitive values.
    pattern = re.compile(b"|".join(re.escape(v) for v in sorted(values, key=len, reverse=True)))
    overlap = max(map(len, values)) - 1
    pending = b""
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(source, "rb") as original, open(target, "xb") as derived:
        while chunk := original.read(CHUNK):
            data = pending + chunk
            cutoff = max(0, len(data) - overlap)
            matches = pattern.finditer(data)
            # Never emit a partial sensitive value at the split.
            for match in matches:
                if match.start() < cutoff < match.end():
                    cutoff = match.start()
                    break
            derived.write(pattern.sub(MARKER, data[:cutoff]))
            pending = data[cutoff:]
        derived.write(pattern.sub(MARKER, pending))


def verify_archive(raw, archive):
    assert_regular(archive)
    require(digest(raw) == digest(archive, decompressed=True),
            "Gzip archive is not lossless against retained raw sample")


def create_archive(raw):
    archive = Path(str(raw) + ".gz")
    if not archive.exists():
        temporary = archive.with_name(archive.name + ".publication-tmp")
        require(not temporary.exists(), "Archive temporary path already exists")
        try:
            with open(raw, "rb") as source, open(temporary, "xb") as output:
                with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed:
                    shutil.copyfileobj(source, compressed, CHUNK)
            verify_archive(raw, temporary)
            # Atomic exclusive publication; never replace an existing archive.
            os.link(temporary, archive)
        finally:
            temporary.unlink(missing_ok=True)
    verify_archive(raw, archive)
    return archive


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def review(args):
    require(args.execute_review and args.terminal_lanes_confirmed,
            "Review requires explicit execution and terminal-lanes confirmation")
    repo = Path(args.repo).resolve()
    publication = repo / PUBLICATION
    require(not publication.exists(), "Publication directory already exists; never overwrite prior review")
    require(re.fullmatch(r"[0-9a-f]{40}", args.candidate_sha or ""), "Exact candidate SHA required")
    require(re.fullmatch(r"[0-9a-f]{64}", args.source_fingerprint or ""), "Exact source fingerprint required")
    allowed = [Path(p) for p in args.allow_private_root]
    permitted = (Path("/fast/build/ess-closeout-c6-acceptance/tmp"), Path("/tmp"))
    require(allowed and all(p.is_absolute() and p.resolve() == p and
            any(p == root or p.is_relative_to(root) for root in permitted) for p in allowed),
            "Allowed private roots must be explicit /tmp or C6 private tmp paths")
    require(args.credential_fixture, "Explicit retained restore credential fixtures required")
    private, fixtures, fixture_fields = fixture_values([Path(p) for p in args.credential_fixture], allowed)
    roots, paths = inventory(repo)
    original = {p: info(p) for p in paths}
    raw = [p for p in paths if p.name == "samples.jsonl"]
    archives = {}
    for path in raw:
        archive = create_archive(path) if args.create_archives else Path(str(path) + ".gz")
        verify_archive(path, archive)
        archives[path] = archive
    # Capture newly created archives only after checking each against its raw file.
    roots, paths = inventory(repo)
    for path in paths:
        original.setdefault(path, info(path))
    values, field_counts, scanned_bytes = set(private), collections.Counter(), 0
    for path in paths:
        if path in archives:
            continue
        for key, value in discovered_values(path):
            # Manually reviewed Python concatenation syntax in the archived r1
            # utility, not a credential value. Bind the exception to exact file
            # bytes and exact token hash; private fixture values remain checked.
            if (str(path.relative_to(repo)) == "docs/reports/s3-hardening/closeout-002/candidate-c6/evidence-tools/evidence_publication.py"
                    and original[path]["sha256"] == "e20d68aef541307942b3746d06371b36b79d010949a3078fe89e6db075e996e6"
                    and key == "secret_key"
                    and hashlib.sha256(value).hexdigest() == "04d48c7c3e60b5680a79d68450157814b5310fb8120b45edcaa63453d1432e04"):
                continue
            values.add(value)
            require(len(values) <= MAX_CREDENTIALS and sum(map(len, values)) <= 8 * CHUNK,
                    "Credential inventory bound exceeded; manual review required")
            field_counts[key] += 1
        scanned_bytes += digest(path, decompressed=path.suffix == ".gz")[1]
    require(all(4 <= len(value) <= MAX_VALUE for value in values),
            "Sensitive value outside safe exact-redaction bounds; manual review required")
    stage = Path(tempfile.mkdtemp(prefix=".c6-publication-", dir=repo / CANDIDATE))
    rows, mappings, excluded, publishable = [], [], [], []
    try:
        for path in paths:
            relative = path.relative_to(repo)
            row = {"original_path": str(relative), "original_sha256": original[path]["sha256"],
                   "original_bytes": original[path]["bytes"]}
            if path in archives:
                archive = archives[path]
                row.update(decision="exclude_raw_use_lossless_gzip", archive_path=str(archive.relative_to(repo)),
                           archive_sha256=original[archive]["sha256"], lossless_gzip_verified=True)
                excluded.append(str(relative))
            else:
                occurrences = exact_occurrences(path, values)
                if occurrences:
                    require(path.suffix == ".log", "Sensitive value in non-log evidence; manual review required")
                    derived_relative = Path("sanitized") / relative.relative_to(REPORTS)
                    derived_relative = derived_relative.with_suffix(".sanitized.log")
                    target = stage / derived_relative
                    redact(path, target, values)
                    require(exact_occurrences(target, values) == 0 and next(discovered_values(target), None) is None,
                            "Derived log privacy verification failed")
                    public = PUBLICATION / derived_relative
                    row.update(decision="exclude_original_use_sanitized", exact_sensitive_value_occurrences=occurrences)
                    excluded.append(str(relative))
                    mappings.append(row)
                    public_info = info(target)
                else:
                    public, public_info = relative, original[path]
                    row["decision"] = "publish_original"
                row.update(publishable_path=str(public), publishable_sha256=public_info["sha256"],
                           publishable_bytes=public_info["bytes"])
                publishable.append(str(public))
            rows.append(row)
        # Exclude only this owned temporary publication tree from re-inventory.
        stage_files = set(stage.rglob("*"))
        current_paths = [p for p in inventory(repo)[1] if p not in stage_files]
        require(current_paths == paths, "Evidence inventory changed during review")
        require(all(info(path) == original[path] for path in paths), "Original evidence changed during review")
        metadata = [str(PUBLICATION / name) for name in
                    ("README.md", "manifest.json", "publishable-paths.json", "exclude-original-paths.json")]
        publishable.extend(metadata)
        write_json(stage / "publishable-paths.json", sorted(publishable))
        write_json(stage / "exclude-original-paths.json", sorted(excluded))
        policy = ("Only exact fixture credentials and quoted credential-record/header/signed-query values are "
                  "replaced with [REDACTED_CREDENTIAL] in derived logs. All other bytes remain unchanged. "
                  "Original logs/samples remain byte-identical. Raw samples are excluded only after lossless "
                  "gzip verification; gzip content is scanned decompressed. No acceptance result is changed.")
        (stage / "README.md").write_text(
            "# C6 evidence publication copies\n\n" + policy + "\n\n"
            "Use publishable-paths.json as the exact payload allowlist and exclude-original-paths.json "
            "as staging exclusions. These lists stage nothing. Future or changed files require a new review. "
            "This bounded credential review does not certify all possible sensitive data or acceptance.\n")
        manifest = {
            "schema": 1, "task": "ESS-S3-HARDENING-CLOSEOUT-002",
            "kind": "credential-only-evidence-publication-manifest",
            "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "candidate_sha": args.candidate_sha, "source_sha256": args.source_fingerprint,
            "scope": "All closeout002-c6* roots including matching regular files, plus candidate-c6; excludes own publication directory.",
            "redaction_policy": policy,
            "manual_public_source_finding": {
                "path": "docs/reports/s3-hardening/closeout-002/candidate-c6/evidence-tools/evidence_publication.py",
                "sha256": "e20d68aef541307942b3746d06371b36b79d010949a3078fe89e6db075e996e6", "line": 368,
                "field": "secret_key", "token_bytes": 15,
                "token_sha256": "04d48c7c3e60b5680a79d68450157814b5310fb8120b45edcaa63453d1432e04",
                "classification": "public Python concatenation syntax in synthetic self-test; no secret or receipt redaction",
                "guard": "exact file path, full file SHA256, field and token SHA256; fixture secrets remain in exact-value inventory"},
            "provenance": {"private_utility": info(Path(__file__)), "c5_policy_manifest": info(repo / C5)},
            "input_counts": {"roots": len(roots), "files": len(paths), "raw_samples_excluded": len(raw),
                             "private_fixture_files": len(fixtures), "private_credential_fields": fixture_fields,
                             "distinct_sensitive_values": len(values), "discovered_credential_fields": dict(field_counts)},
            "private_fixture_provenance": fixtures,
            "scan_results": {"sanitized_original_logs": len(mappings), "publication_payloads": len(publishable),
                             "scanned_candidate_payload_uncompressed_bytes": scanned_bytes,
                             "remaining_exact_sensitive_values": 0, "remaining_value_bearing_credential_fields": 0,
                             "all_original_hashes_rechecked": True, "credential_values_printed": False,
                             "terminal_lanes_confirmed_by_invoker": True},
            "limitations": ["Bounded credential review of this hashed inventory, not future files or all possible personal data.",
                            "Candidate SHA/fingerprint and lane termination are supplied by the coordinating owner, not certified by this utility."],
            "sanitized_mappings": mappings, "files": rows,
            "publication_path_list": str(PUBLICATION / "publishable-paths.json"),
            "exclude_original_path_list": str(PUBLICATION / "exclude-original-paths.json"),
            "publication_metadata_paths": metadata}
        write_json(stage / "manifest.json", manifest)
        for path in stage.iterdir():
            if path.is_file():
                require(exact_occurrences(path, values) == 0 and next(discovered_values(path), None) is None,
                        "Publication metadata privacy verification failed")
        require(not publication.exists(), "Publication directory appeared during review")
        stage.rename(publication)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    print(json.dumps({"status": "passed", "manifest": str(PUBLICATION / "manifest.json"),
                      "files": len(paths), "sanitized_logs": len(mappings), "raw_samples_excluded": len(raw)}))


def self_test():
    # Only tiny synthetic data: no repository evidence, environment or process reads.
    with tempfile.TemporaryDirectory(prefix="ess-publication-synthetic-") as tmp:
        root = Path(tmp)
        source, target = root / "source.log", root / "copy.log"
        secret = b"fixture-secret-123456"
        data = (json.dumps({"secret_key": secret.decode()}, separators=(",", ":")).encode()
                + b"\ncopy=" + secret + b"\nexit=1\n")
        source.write_bytes(data)
        found = {value for _, value in discovered_values(source)}
        require(found == {secret}, "Synthetic field discovery failed")
        redact(source, target, found)
        require(target.read_bytes() == data.replace(secret, MARKER), "Synthetic byte preservation failed")
        require(source.read_bytes() == data, "Synthetic original changed")
        require(exact_occurrences(target, found) == 0, "Synthetic exact privacy check failed")
        raw = root / "samples.jsonl"
        raw.write_bytes(b'{"monotonic":1,"rss":10}\n' * 8)
        before = info(raw)
        archive = create_archive(raw)
        require(info(raw) == before, "Synthetic archive modified original")
        require(digest(raw) == digest(archive, True), "Synthetic archive not lossless")
        archive.write_bytes(gzip.compress(b"different"))
        try:
            verify_archive(raw, archive)
        except ReviewError:
            pass
        else:
            raise ReviewError("Synthetic mismatched archive not rejected")
        boundary = root / "boundary.log"
        boundary.write_bytes(b"\n" * (CHUNK - 7) + data)
        require(secret in {v for _, v in discovered_values(boundary)}, "Synthetic boundary discovery failed")
        require(exact_occurrences(boundary, {secret}) == 2, "Synthetic boundary exact count failed")
        repo = root / "repo"
        (repo / CANDIDATE).mkdir(parents=True)
        prefix_file = repo / REPORTS / "closeout002-c6-root.log"
        prefix_file.write_bytes(b"exit=1\n")
        (repo / CANDIDATE / "receipt.json").write_bytes(b'{"status":"failed"}\n')
        (repo / PUBLICATION).mkdir()
        (repo / PUBLICATION / "ignored.log").write_bytes(b"ignore\n")
        _, paths = inventory(repo)
        require(prefix_file in paths and len(paths) == 2, "Synthetic scope inventory failed")
        # Exercise the complete publishing path only inside this synthetic repo.
        shutil.rmtree(repo / PUBLICATION)
        (repo / C5).parent.mkdir(parents=True)
        (repo / C5).write_bytes(b'{"synthetic_policy":true}\n')
        fixture = root / "fixture/credentials.json"
        fixture.parent.mkdir()
        fixture.write_text(json.dumps({"admin": {"secret_key": secret.decode()}}))
        prefix_file.write_bytes(data)
        sample = repo / CANDIDATE / "samples.jsonl"
        sample.write_bytes(raw.read_bytes())
        before_inputs = {p: info(p) for p in inventory(repo)[1]}
        synthetic_args = argparse.Namespace(
            execute_review=True, terminal_lanes_confirmed=True, repo=str(repo),
            candidate_sha="a" * 40, source_fingerprint="b" * 64,
            allow_private_root=[str(root)], credential_fixture=[str(fixture)], create_archives=True)
        with contextlib.redirect_stdout(io.StringIO()):
            review(synthetic_args)
        manifest = json.loads((repo / PUBLICATION / "manifest.json").read_text())
        require(manifest["scan_results"]["sanitized_original_logs"] == 1,
                "Synthetic full review did not sanitize exactly one log")
        require(all(info(path) == before for path, before in before_inputs.items()),
                "Synthetic full review changed an original")
        allowlist = json.loads((repo / PUBLICATION / "publishable-paths.json").read_text())
        exclusions = json.loads((repo / PUBLICATION / "exclude-original-paths.json").read_text())
        require(str(prefix_file.relative_to(repo)) in exclusions and
                str(sample.relative_to(repo)) in exclusions,
                "Synthetic full review exclusions missing")
        require(str(sample.relative_to(repo)) + ".gz" in allowlist,
                "Synthetic full review missing lossless archive")
        require(json.loads((repo / CANDIDATE / "receipt.json").read_text())["status"] == "failed",
                "Synthetic full review changed failed outcome")
        require(all(exact_occurrences(repo / name, {secret}) == 0 for name in allowlist),
                "Synthetic full publication contains exact credential")
    print(json.dumps({"status": "passed", "synthetic_checks": 16,
                      "real_evidence_scanned": False, "real_archives_created": False}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--execute-review", action="store_true")
    parser.add_argument("--terminal-lanes-confirmed", action="store_true")
    parser.add_argument("--repo", default="/home/gao/Workspace/gsmlg-opt/ex_storage_service")
    parser.add_argument("--candidate-sha")
    parser.add_argument("--source-fingerprint")
    parser.add_argument("--credential-fixture", action="append", default=[])
    parser.add_argument("--allow-private-root", action="append", default=[])
    parser.add_argument("--create-archives", action="store_true",
                        help="Create absent deterministic lossless sample archives; never replace existing archives")
    args = parser.parse_args()
    if args.self_test:
        require(not args.execute_review, "Self-test and real review are mutually exclusive")
        self_test()
    else:
        review(args)


if __name__ == "__main__":
    try:
        main()
    except ReviewError as error:
        print("Publication review blocked: " + str(error), file=sys.stderr)
        sys.exit(1)
    except Exception as error:
        # Never expose exception data, potentially containing private fixture values.
        print("Publication review blocked by " + type(error).__name__ + "; no credential details printed", file=sys.stderr)
        sys.exit(1)
