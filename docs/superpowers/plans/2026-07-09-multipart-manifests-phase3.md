# Multipart Manifests (Git-Style Data Model Phase 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Multipart parts become CAS blobs (deduplicated, no bucket-local part files), CompleteMultipartUpload concatenates with **constant memory** (today it reads whole parts — up to 5 GB each — into memory), and every completed multipart object gets an immutable content-addressed manifest record describing its parts.

**Architecture (deliberate deviation from PRD §12.2/§9.1, to be recorded in the PRD):** Serving stays whole-blob. The e2e suite and real S3 clients depend on `Content-Length` and single-file Range/`send_file` semantics, which multi-file manifest streaming (chunked transfer-encoding) would regress. So CompleteMultipartUpload still materializes one whole-object CAS blob — but via streaming file-to-file concatenation of CAS part blobs — and *additionally* writes a `manifest:sha256:{hash}` record + content-addressed manifest file under `cas/manifests/`. GET/HEAD/Range are untouched. Part blobs become unreferenced after completion and are reclaimed by Phase 4 GC (mpu_part records are GC roots only while an upload is active). Manifest-based serving is deferred to the pack-storage phase.

**Tech Stack:** Same as Phases 1–2. `Engine.put_object/5` already computes SHA-256+MD5 in one pass and commits to CAS — parts reuse it directly.

## Global Constraints

- Same as previous plans (no new deps, `--warnings-as-errors`, format, conventional commits, Req-based S3 tests on `localhost:9001`).
- Public error tuples from `Multipart.complete_upload/3` keep their exact shapes — the handler maps them to S3 errors: `{:error, {:missing_part, pn, reason}}`, `{:error, {:etag_mismatch, pn, expected, actual}}`, `{:error, {:entity_too_small, pn, size, min}}`.
- `Multipart.store_part/4` keeps returning `{:ok, etag}`; `list_parts/2` entries keep `part_number`/`etag`/`size` fields (they gain `hash`).
- `complete_upload/3` return changes from `{:ok, {content_hash, etag, size}}` to `{:ok, {content_hash, etag, size, manifest_hash}}` — update both callers (multipart_handlers.ex, storage/multipart_test.exs).
- The manifest file's canonical form must be deterministic: a JSON **array** (arrays are order-stable; Elixir map key order is not): `["ess-manifest-v1", etag, total_size, [[number, hash, size, etag], ...]]` with parts sorted by number. The manifest hash is the SHA-256 of that canonical JSON.
- S3 multipart suites (`multipart_test.exs`, `multipart_edge_test.exs`) and e2e semantics (completed body equality, abort cleanup) must pass unchanged.

---

### Task 1: `Storage.Manifest` module

**Files:**
- Create: `apps/ex_storage_service/lib/ex_storage_service/storage/manifest.ex`
- Test: Create `apps/ex_storage_service/test/ex_storage_service/storage/manifest_test.exs`

**Interfaces:**
- Consumes: `CAS.data_root/0`, `CAS.reserved_root/0`.
- Produces:
  - `Manifest.create_manifest(parts, total_size, etag) :: {:ok, manifest_hash}` — `parts` is `[%{number: int, hash: sha256_hex, size: int, etag: md5_hex}]` (any order; canonicalized internally). Idempotent: same parts → same hash, file/record rewritten identically.
  - `Manifest.get_manifest(manifest_hash) :: {:ok, map()} | {:error, :not_found}` — the Concord record `%{hash:, format:, parts:, total_size:, etag:, created_at:}`.
  - `Manifest.manifest_path(manifest_hash) :: String.t()` — `{data_root}/cas/manifests/sha256/{p2}/{rest}`.

- [ ] **Step 1: Write the failing tests**

```elixir
# apps/ex_storage_service/test/ex_storage_service/storage/manifest_test.exs
defmodule ExStorageService.Storage.ManifestTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.Manifest

  defp parts do
    [
      %{number: 2, hash: String.duplicate("b", 64), size: 4, etag: "e2"},
      %{number: 1, hash: String.duplicate("a", 64), size: 5_242_880, etag: "e1"}
    ]
  end

  test "create_manifest is deterministic regardless of part order and idempotent" do
    {:ok, h1} = Manifest.create_manifest(parts(), 5_242_884, "combo-2")
    {:ok, h2} = Manifest.create_manifest(Enum.reverse(parts()), 5_242_884, "combo-2")
    assert h1 == h2

    # content-addressed file exists and hashes to its own name
    path = Manifest.manifest_path(h1)
    assert File.exists?(path)
    assert Base.encode16(:crypto.hash(:sha256, File.read!(path)), case: :lower) == h1
  end

  test "get_manifest returns the record with parts sorted by number" do
    {:ok, hash} = Manifest.create_manifest(parts(), 5_242_884, "combo-2")

    assert {:ok, record} = Manifest.get_manifest(hash)
    assert record.format == "ess-manifest-v1"
    assert record.total_size == 5_242_884
    assert record.etag == "combo-2"
    assert [%{number: 1}, %{number: 2}] = record.parts
  end

  test "get_manifest on unknown hash returns not_found" do
    missing = Base.encode16(:crypto.hash(:sha256, "nope-#{System.unique_integer()}"), case: :lower)
    assert {:error, :not_found} = Manifest.get_manifest(missing)
  end
end
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/manifest_test.exs`
Expected: FAIL — module not available

- [ ] **Step 3: Implement**

```elixir
# apps/ex_storage_service/lib/ex_storage_service/storage/manifest.ex
defmodule ExStorageService.Storage.Manifest do
  @moduledoc """
  Immutable, content-addressed manifests describing multipart objects.

  A manifest lists the CAS part blobs a multipart object was assembled
  from. It is stored twice: a canonical JSON file at
  `{data_root}/cas/manifests/sha256/{p2}/{rest}` (the content the hash
  addresses) and a `manifest:sha256:{hash}` Concord record for fast reads.

  Serving does not use manifests (completed multipart objects are
  materialized as whole CAS blobs so Content-Length/Range/sendfile
  semantics are preserved); manifests exist for audit, repair, and future
  replication/pack phases.

  The canonical form is a JSON array — arrays are order-stable, Elixir
  map key order is not:

      ["ess-manifest-v1", etag, total_size, [[number, hash, size, etag], ...]]
  """

  alias ExStorageService.Storage.CAS

  @format "ess-manifest-v1"

  def manifest_path(manifest_hash) do
    <<prefix::binary-size(2), rest::binary>> = manifest_hash
    Path.join([CAS.data_root(), CAS.reserved_root(), "manifests", "sha256", prefix, rest])
  end

  def create_manifest(parts, total_size, etag) do
    sorted = Enum.sort_by(parts, & &1.number)

    canonical =
      JSON.encode!([
        @format,
        etag,
        total_size,
        Enum.map(sorted, fn p -> [p.number, p.hash, p.size, p.etag] end)
      ])

    manifest_hash = Base.encode16(:crypto.hash(:sha256, canonical), case: :lower)

    write_manifest_file(manifest_hash, canonical)

    record = %{
      hash: "sha256:#{manifest_hash}",
      format: @format,
      parts: sorted,
      total_size: total_size,
      etag: etag,
      created_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }

    case Concord.put("manifest:sha256:#{manifest_hash}", record) do
      :ok -> {:ok, manifest_hash}
      {:ok, _} -> {:ok, manifest_hash}
      error -> error
    end
  end

  def get_manifest(manifest_hash) do
    case Concord.get("manifest:sha256:#{manifest_hash}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, record} -> {:ok, record}
      error -> error
    end
  end

  defp write_manifest_file(manifest_hash, canonical) do
    dest = manifest_path(manifest_hash)

    unless File.exists?(dest) do
      File.mkdir_p!(Path.dirname(dest))
      tmp = dest <> ".tmp-#{:erlang.unique_integer([:positive])}"
      File.write!(tmp, canonical)
      File.rename!(tmp, dest)
    end

    :ok
  end
end
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/manifest_test.exs`
Expected: 3 tests, 0 failures

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/manifest.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/manifest_test.exs
git commit -m "feat(core): add content-addressed multipart manifests"
```

---

### Task 2: Parts as CAS blobs; streaming completion

**Files:**
- Modify: `apps/ex_storage_service/lib/ex_storage_service/storage/multipart.ex`
- Modify: `apps/ex_storage_service_s3/lib/ex_storage_service_s3/multipart_handlers.ex` (complete_upload return tuple)
- Test: Modify `apps/ex_storage_service/test/ex_storage_service/storage/multipart_test.exs`

**Interfaces:**
- Consumes: `Engine.put_object/3` (parts → CAS + blob meta), `CAS.blob_path/1`, `CAS.tmp_upload_path/0`, `CAS.commit_blob/2`, `Metadata.ensure_blob_meta/2`, `Manifest.create_manifest/3` (Task 1).
- Produces:
  - `store_part/4` unchanged signature/return; mpu_part records gain `hash` (the part's CAS blob hash); no more part files/dirs.
  - `complete_upload/3` returns `{:ok, {content_hash, etag, size, manifest_hash}}`; validates parts from Concord records; concatenates by streaming CAS part blobs (constant memory).
  - `abort_upload/2` unchanged (record cleanup; legacy part-dir removal kept for uploads started before this change).

- [ ] **Step 1: Update/extend the failing tests**

In `storage/multipart_test.exs`:
- Update the Phase-1 test's completion pattern to the 4-tuple:

```elixir
    assert {:ok, {content_hash, _etag, _size, _manifest_hash}} =
             ExStorageService.Storage.Multipart.complete_upload(bucket, upload_id, parts)
```

- Append these tests:

```elixir
  test "store_part commits the part to the global CAS and records its hash" do
    bucket = "mpu-part-#{:erlang.unique_integer([:positive])}"
    ExStorageService.Metadata.create_bucket(bucket)
    {:ok, upload_id} = Multipart.init_upload(bucket, "obj")

    data = "part-data-#{System.unique_integer()}"
    expected_hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)

    {:ok, _etag} = Multipart.store_part(bucket, upload_id, 1, data)

    assert File.exists?(ExStorageService.Storage.CAS.blob_path(expected_hash))
    assert {:ok, [part]} = Multipart.list_parts(bucket, upload_id)
    assert part.hash == expected_hash
    # no bucket-local part files
    refute File.dir?(Path.join([ExStorageService.Storage.CAS.data_root(), bucket, "multipart", upload_id]))
  end

  test "complete_upload creates a manifest describing the parts" do
    bucket = "mpu-man-#{:erlang.unique_integer([:positive])}"
    ExStorageService.Metadata.create_bucket(bucket)
    {:ok, upload_id} = Multipart.init_upload(bucket, "obj")

    p1 = String.duplicate("x", 5 * 1024 * 1024)
    p2 = "tail-#{System.unique_integer()}"
    {:ok, etag1} = Multipart.store_part(bucket, upload_id, 1, p1)
    {:ok, etag2} = Multipart.store_part(bucket, upload_id, 2, p2)

    assert {:ok, {content_hash, _etag, size, manifest_hash}} =
             Multipart.complete_upload(bucket, upload_id, [{1, etag1}, {2, etag2}])

    assert size == byte_size(p1) + byte_size(p2)
    # whole-object blob equals the concatenation
    assert File.read!(ExStorageService.Storage.CAS.blob_path(content_hash)) == p1 <> p2

    assert {:ok, manifest} = ExStorageService.Storage.Manifest.get_manifest(manifest_hash)
    assert [%{number: 1, etag: ^etag1}, %{number: 2, etag: ^etag2}] = manifest.parts
    assert manifest.total_size == size

    # part records cleaned up after completion
    assert {:ok, []} = Multipart.list_parts(bucket, upload_id)
  end

  test "complete_upload with a never-uploaded part returns missing_part" do
    bucket = "mpu-miss-#{:erlang.unique_integer([:positive])}"
    ExStorageService.Metadata.create_bucket(bucket)
    {:ok, upload_id} = Multipart.init_upload(bucket, "obj")
    {:ok, etag1} = Multipart.store_part(bucket, upload_id, 1, "only-part")

    assert {:error, {:missing_part, 2, _reason}} =
             Multipart.complete_upload(bucket, upload_id, [{1, etag1}, {2, "bogus"}])
  end
```

(Add `alias ExStorageService.Storage.Multipart` at the top if the file aliases it that way already — match existing style.)

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/multipart_test.exs`
Expected: new tests FAIL (no `hash` in part records; 3-tuple return; no manifest)

- [ ] **Step 3: Implement `store_part/4`**

Replace the body of `store_part/4` (and delete `write_part_data/2` both clauses, `part_dir/2`, `part_path/3` — but see Step 4's `cleanup_parts/2` note):

```elixir
  def store_part(bucket, upload_id, part_number, data) do
    case ExStorageService.Storage.Engine.put_object(bucket, "mpu-part", data) do
      {:ok, {hash, etag, size}} ->
        part_meta = %{
          part_number: part_number,
          etag: etag,
          size: size,
          hash: hash,
          uploaded_at: DateTime.utc_now() |> DateTime.to_iso8601()
        }

        Concord.put(mpu_part_key(bucket, upload_id, part_number), part_meta)
        {:ok, etag}

      {:error, reason} ->
        {:error, reason}
    end
  end
```

Update the moduledoc: part data lives in the global CAS (`hash` on the part record); no bucket-local part files.

- [ ] **Step 4: Implement streaming `complete_upload/3`**

Replace the concatenation section of `complete_upload/3` (the `try` block reading part files and everything down to the `{:ok, {content_hash, etag, total_size, upload_meta}}` construction) with record-based validation and streaming concatenation:

```elixir
        sorted_parts = Enum.sort_by(parts, fn {pn, _etag} -> pn end)

        min_part_size =
          Application.get_env(:ex_storage_service, :min_part_size, 5 * 1024 * 1024)

        last_index = length(sorted_parts) - 1

        result =
          with {:ok, part_records} <- resolve_part_records(bucket, upload_id, sorted_parts),
               :ok <- validate_parts(part_records, min_part_size, last_index) do
            concatenate_parts(part_records)
          end

        case result do
          {:ok, {content_hash, etag, total_size, manifest_hash}} ->
            cleanup_parts(bucket, upload_id)

            now = DateTime.utc_now() |> DateTime.to_iso8601()

            completed_meta = %{
              bucket: bucket,
              key: get_in_upload(upload_meta, :key),
              upload_id: upload_id,
              status: :completed,
              content_hash: content_hash,
              manifest_hash: manifest_hash,
              etag: etag,
              size: total_size,
              created_at: get_in_upload(upload_meta, :created_at),
              updated_at: now
            }

            Concord.put(mpu_key(bucket, upload_id), completed_meta)

            {:ok, {content_hash, etag, total_size, manifest_hash}}

          error ->
            error
        end
```

Add the private helpers (below `get_in_upload/2`):

```elixir
  # Look up the Concord part record for each client-requested part and
  # check the client-supplied etags, preserving the historical error shapes.
  defp resolve_part_records(bucket, upload_id, sorted_parts) do
    sorted_parts
    |> Enum.reduce_while({:ok, []}, fn {pn, client_etag}, {:ok, acc} ->
      case Concord.get(mpu_part_key(bucket, upload_id, pn)) do
        {:ok, nil} ->
          {:halt, {:error, {:missing_part, pn, :not_found}}}

        {:ok, record} ->
          if client_etag != "" and record.etag != client_etag do
            {:halt, {:error, {:etag_mismatch, pn, client_etag, record.etag}}}
          else
            {:cont, {:ok, [record | acc]}}
          end

        {:error, reason} ->
          {:halt, {:error, {:missing_part, pn, reason}}}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp validate_parts(part_records, min_part_size, last_index) do
    part_records
    |> Enum.with_index()
    |> Enum.find(fn {record, idx} -> idx < last_index and record.size < min_part_size end)
    |> case do
      nil -> :ok
      {record, _idx} -> {:error, {:entity_too_small, record.part_number, record.size, min_part_size}}
    end
  end

  # Streams each CAS part blob into a tmp file (constant memory), computing
  # the whole-object SHA-256 on the way, then commits the result as a blob
  # and records the manifest.
  defp concatenate_parts(part_records) do
    alias ExStorageService.Storage.{CAS, Manifest}

    tmp_path = CAS.tmp_upload_path()
    out = File.open!(tmp_path, [:write, :raw, :binary])

    try do
      {sha_ctx, total_size} =
        Enum.reduce(part_records, {:crypto.hash_init(:sha256), 0}, fn record, {ctx, size} ->
          part_blob = CAS.blob_path(record.hash)

          ctx =
            part_blob
            |> File.stream!(262_144)
            |> Enum.reduce(ctx, fn chunk, c ->
              :ok = IO.binwrite(out, chunk)
              :crypto.hash_update(c, chunk)
            end)

          {ctx, size + record.size}
        end)

      File.close(out)

      content_hash = sha_ctx |> :crypto.hash_final() |> Base.encode16(case: :lower)

      # S3 multipart etag: MD5 of the concatenated raw part-MD5 digests,
      # suffixed with the part count; part etags are the hex MD5s.
      md5_digests = Enum.map(part_records, &Base.decode16!(&1.etag, case: :mixed))
      combined_md5 = :crypto.hash(:md5, IO.iodata_to_binary(md5_digests))
      etag = "#{Base.encode16(combined_md5, case: :lower)}-#{length(part_records)}"

      :ok = CAS.commit_blob(tmp_path, content_hash)
      ExStorageService.Metadata.ensure_blob_meta(content_hash, total_size)

      manifest_parts =
        Enum.map(part_records, fn r ->
          %{number: r.part_number, hash: r.hash, size: r.size, etag: r.etag}
        end)

      {:ok, manifest_hash} = Manifest.create_manifest(manifest_parts, total_size, etag)

      {:ok, {content_hash, etag, total_size, manifest_hash}}
    rescue
      e ->
        File.close(out)
        File.rm(tmp_path)
        {:error, Exception.message(e)}
    end
  end
```

In `cleanup_parts/2`, keep the `File.rm_rf!` of the legacy part dir (uploads started before this change may still have files there) — inline `part_dir/2`'s path construction there if you removed the helper, or keep `part_dir/2` and delete only `part_path/3` and `write_part_data/2`. Delete the now-dead concatenation `try` block, `data_root/0` if unused, and any unused variables so `--warnings-as-errors` stays clean.

- [ ] **Step 5: Update the caller in `multipart_handlers.ex`**

Change the success match in `complete_multipart_upload/3` from:

```elixir
                case Multipart.complete_upload(bucket, upload_id, parts) do
                  {:ok, {content_hash, etag, size}} ->
```

to:

```elixir
                case Multipart.complete_upload(bucket, upload_id, parts) do
                  {:ok, {content_hash, etag, size, manifest_hash}} ->
```

and extend the version meta with the manifest linkage:

```elixir
                    meta = %{
                      content_hash: content_hash,
                      manifest_hash: manifest_hash,
                      object_type: :blob,
                      size: size,
                      etag: etag,
                      content_type: content_type,
                      metadata: %{},
                      created_at: now,
                      updated_at: now
                    }
```

(`object_type: :blob` is explicit: the object is served as a whole blob; the manifest is bookkeeping.)

- [ ] **Step 6: Run core + S3 suites**

Run: `mix test apps/ex_storage_service/test && mix test apps/ex_storage_service_s3/test`
Expected: all pass — including `multipart_edge_test.exs` (etag mismatch/too-small/missing-part shapes preserved) and the e2e-equivalent body-equality tests.

- [ ] **Step 7: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/multipart.ex \
        apps/ex_storage_service_s3/lib/ex_storage_service_s3/multipart_handlers.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/multipart_test.exs
git commit -m "feat(core): store multipart parts as CAS blobs with streaming completion and manifests"
```

---

### Task 3: PRD sync + full verification

**Files:**
- Modify: `docs/prd/git-style-data-model.md`

- [ ] **Step 1: PRD sync**

- Add revision note 12: "**Phase 3 implementation note (2026-07-09):** parts are CAS blobs and CompleteMultipartUpload streams the concatenation (constant memory) into a whole-object CAS blob *and* records a content-addressed manifest (`manifest:sha256:{hash}` + file under `cas/manifests/`). Serving stays whole-blob — manifest-streaming GET was deliberately not implemented because chunked transfer-encoding would drop `Content-Length` and complicate Range, regressing S3 client compatibility (§12.4). Manifest-based serving is deferred to Phase 6 (packs). Part blobs become unreferenced after completion and are reclaimed by Phase 4 GC."
- Mark §20 Phase 3 bullets with status (parts-as-blobs ✅, manifest created ✅, manifest streaming deferred with rationale, GC roots note → Phase 4).
- In §12.2/§12.4 add one-line "Implemented as:" notes matching the above.

- [ ] **Step 2: Full verification**

```bash
mix format && git diff --exit-code
mix compile --warnings-as-errors
mix test apps/ex_storage_service/test
mix test apps/ex_storage_service_s3/test
mix test apps/ex_storage_service_web/test
```

- [ ] **Step 3: Commit**

```bash
git add docs/prd/git-style-data-model.md
git commit -m "docs(prd): record phase 3 manifest implementation and serving deviation"
```
