# Global CAS (Git-Style Data Model Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move object content from bucket-local paths (`{data_root}/{bucket}/objects/…`) to a global content-addressable store (`{data_root}/cas/objects/sha256/…`) so identical content is stored once across all buckets, keys, and copies.

**Architecture:** A new plain module `ExStorageService.Storage.CAS` owns all physical blob paths and atomic commits. `Storage.Engine` becomes a thin facade: PUTs stream to a CAS tmp file and commit by rename; GETs resolve the global path first with a read-only fallback to the legacy bucket-local path (pre-migration data). CopyObject becomes metadata-only by promoting legacy blobs into the CAS. Object metadata (`obj:{bucket}:{key}`) is unchanged in this phase; new `blob:sha256:{hash}` records track blob existence. Spec: `docs/prd/git-style-data-model.md` §20 Phase 1.

**Tech Stack:** Elixir umbrella (no Ecto), Concord (Raft KV) metadata, Plug/Bandit S3 API, ExUnit + Req-based HTTP integration tests.

## Global Constraints

- No Ecto, no database, no new hex dependencies.
- Elixir >= 1.18.0, OTP 28. `mix compile --warnings-as-errors` must stay clean.
- S3 modules use `ExStorageServiceS3.*` naming; core modules `ExStorageService.*`.
- Object metadata schema `obj:{bucket}:{key}` **must not change** in this phase (PRD §20 Phase 1).
- Streaming uploads must keep computing SHA-256 + MD5 in one pass, in the request process (never inside a GenServer).
- Content files are never deleted in a request path (PRD §10.3).
- `cas` becomes a reserved name: rejected as bucket name, excluded from ContentGC's disk scan.
- Existing tests must keep passing: run `mix test apps/ex_storage_service/test`, `mix test apps/ex_storage_service_s3/test`, `mix test apps/ex_storage_service_web/test` after core tasks.
- Commit style: conventional commits with app scope, e.g. `feat(core): …`, `feat(s3): …`, no Claude trailers.
- Tests use data root `/tmp/ex_storage_service/test_data` (config), S3 integration tests hit `http://localhost:9001` via `Req` (see `apps/ex_storage_service_s3/test/ex_storage_service_s3/s3_api_test.exs` for conventions; `async: false`).

---

### Task 1: `Storage.CAS` module

**Files:**
- Create: `apps/ex_storage_service/lib/ex_storage_service/storage/cas.ex`
- Test: `apps/ex_storage_service/test/ex_storage_service/storage/cas_test.exs`

**Interfaces:**
- Consumes: `Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")` (same default as `ContentGC`).
- Produces (later tasks depend on these exact signatures):
  - `CAS.data_root() :: String.t()`
  - `CAS.reserved_root() :: String.t()` (returns `"cas"`)
  - `CAS.blob_path(content_hash :: String.t()) :: String.t()`
  - `CAS.has_blob?(content_hash) :: boolean()`
  - `CAS.tmp_upload_path() :: String.t()` (creates the tmp dir, returns a unique path)
  - `CAS.commit_blob(tmp_path :: String.t(), content_hash) :: :ok`
  - `CAS.verify_blob(content_hash) :: :ok | {:error, :missing | :corrupt | term()}`

- [ ] **Step 1: Write the failing tests**

```elixir
# apps/ex_storage_service/test/ex_storage_service/storage/cas_test.exs
defmodule ExStorageService.Storage.CASTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.CAS

  defp random_content, do: :crypto.strong_rand_bytes(64)
  defp sha256_hex(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)

  defp write_tmp(data) do
    tmp = CAS.tmp_upload_path()
    File.write!(tmp, data)
    tmp
  end

  test "blob_path/1 shards by first two hex chars under the reserved cas root" do
    hash = sha256_hex("hello")
    <<prefix::binary-size(2), rest::binary>> = hash

    assert CAS.blob_path(hash) ==
             Path.join([CAS.data_root(), "cas", "objects", "sha256", prefix, rest])
  end

  test "commit_blob/2 moves the tmp file into the CAS and has_blob?/1 sees it" do
    data = random_content()
    hash = sha256_hex(data)
    tmp = write_tmp(data)

    refute CAS.has_blob?(hash)
    assert :ok = CAS.commit_blob(tmp, hash)
    assert CAS.has_blob?(hash)
    refute File.exists?(tmp)
    assert File.read!(CAS.blob_path(hash)) == data
  end

  test "commit_blob/2 is idempotent: second commit discards the tmp file" do
    data = random_content()
    hash = sha256_hex(data)

    assert :ok = CAS.commit_blob(write_tmp(data), hash)
    tmp2 = write_tmp(data)
    assert :ok = CAS.commit_blob(tmp2, hash)
    refute File.exists?(tmp2)
    assert File.read!(CAS.blob_path(hash)) == data
  end

  test "verify_blob/1 detects intact, corrupt, and missing blobs" do
    data = random_content()
    hash = sha256_hex(data)
    assert :ok = CAS.commit_blob(write_tmp(data), hash)
    assert :ok = CAS.verify_blob(hash)

    File.write!(CAS.blob_path(hash), "tampered")
    assert {:error, :corrupt} = CAS.verify_blob(hash)

    missing_hash = sha256_hex(random_content())
    assert {:error, :missing} = CAS.verify_blob(missing_hash)
  end
end
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/cas_test.exs`
Expected: FAIL — `module ExStorageService.Storage.CAS is not available`

- [ ] **Step 3: Write the implementation**

```elixir
# apps/ex_storage_service/lib/ex_storage_service/storage/cas.ex
defmodule ExStorageService.Storage.CAS do
  @moduledoc """
  Global content-addressable blob store under the reserved `cas/` root.

  Layout: `{data_root}/cas/objects/sha256/{first_two_hex}/{rest}`.

  Blobs are immutable and shared across all buckets, keys, and versions.
  Commit is an atomic `File.rename!/2` from a tmp file on the same
  filesystem. All functions are plain path/filesystem operations executed
  in the caller's process — this module deliberately has no process.

  `cas` is a reserved name: `BucketValidator` rejects it as a bucket name
  and `ContentGC` skips it when scanning the legacy bucket-local layout.
  """

  @reserved_root "cas"

  def reserved_root, do: @reserved_root

  def data_root do
    Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")
  end

  def blob_path(content_hash) do
    <<prefix::binary-size(2), rest::binary>> = content_hash
    Path.join([data_root(), @reserved_root, "objects", "sha256", prefix, rest])
  end

  def has_blob?(content_hash), do: File.exists?(blob_path(content_hash))

  @doc """
  Returns a unique path inside `cas/tmp/uploads/`, creating the directory.
  The tmp dir shares a filesystem with `cas/objects/`, so `commit_blob/2`
  can rename atomically.
  """
  def tmp_upload_path do
    dir = Path.join([data_root(), @reserved_root, "tmp", "uploads"])
    File.mkdir_p!(dir)
    Path.join(dir, "upload-#{:erlang.unique_integer([:positive])}")
  end

  @doc """
  Atomically moves `tmp_path` into the CAS. If the blob already exists
  (dedup hit), the tmp file is discarded and the existing blob is kept.
  """
  def commit_blob(tmp_path, content_hash) do
    dest = blob_path(content_hash)

    if File.exists?(dest) do
      File.rm(tmp_path)
      :ok
    else
      File.mkdir_p!(Path.dirname(dest))
      File.rename!(tmp_path, dest)
      :ok
    end
  end

  @doc """
  Re-hashes the blob file and compares against its content hash.
  """
  def verify_blob(content_hash) do
    case File.read(blob_path(content_hash)) do
      {:ok, data} ->
        actual = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
        if actual == content_hash, do: :ok, else: {:error, :corrupt}

      {:error, :enoent} ->
        {:error, :missing}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/cas_test.exs`
Expected: 4 tests, 0 failures

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/cas.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/cas_test.exs
git commit -m "feat(core): add global content-addressable store module"
```

---

### Task 2: Blob metadata helpers in `Metadata`

**Files:**
- Modify: `apps/ex_storage_service/lib/ex_storage_service/metadata.ex` (append after the `## Object metadata operations` section, before `## Private`)
- Test: `apps/ex_storage_service/test/ex_storage_service/metadata_test.exs` (append a `describe` block)

**Interfaces:**
- Consumes: `Concord.get/1`, `Concord.put/2`.
- Produces:
  - `Metadata.put_blob_meta(content_hash :: String.t(), meta :: map()) :: :ok | {:error, term()}`
  - `Metadata.get_blob_meta(content_hash) :: {:ok, map()} | {:error, :not_found} | {:error, term()}`
  - `Metadata.ensure_blob_meta(content_hash, size :: non_neg_integer()) :: :ok | {:error, term()}` — creates the record only if absent (never resets `created_at` on dedup hits).

- [ ] **Step 1: Write the failing tests** (append inside the existing test module)

```elixir
  describe "blob metadata" do
    test "ensure_blob_meta creates a record once and get_blob_meta reads it" do
      hash = Base.encode16(:crypto.hash(:sha256, "blob-meta-#{System.unique_integer()}"), case: :lower)

      assert {:error, :not_found} = ExStorageService.Metadata.get_blob_meta(hash)
      assert :ok = ExStorageService.Metadata.ensure_blob_meta(hash, 42)

      assert {:ok, meta} = ExStorageService.Metadata.get_blob_meta(hash)
      assert meta.hash == "sha256:#{hash}"
      assert meta.size == 42
      assert meta.state == :active
      created = meta.created_at

      # Second ensure is a no-op — created_at is preserved
      assert :ok = ExStorageService.Metadata.ensure_blob_meta(hash, 42)
      assert {:ok, %{created_at: ^created}} = ExStorageService.Metadata.get_blob_meta(hash)
    end
  end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/metadata_test.exs`
Expected: FAIL — `undefined function get_blob_meta/1`

- [ ] **Step 3: Write the implementation** (insert before `## Private` in `metadata.ex`)

```elixir
  ## Blob metadata operations (global CAS)
  # Key schema: "blob:sha256:{hash}" — see docs/prd/git-style-data-model.md §7.4

  def put_blob_meta(content_hash, meta) do
    Concord.put("blob:sha256:#{content_hash}", meta)
  end

  def get_blob_meta(content_hash) do
    case Concord.get("blob:sha256:#{content_hash}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, value} -> {:ok, value}
      error -> error
    end
  end

  @doc """
  Creates the blob metadata record if it does not exist yet. Dedup hits
  (same content committed again) keep the original record.
  """
  def ensure_blob_meta(content_hash, size) do
    case get_blob_meta(content_hash) do
      {:ok, _meta} ->
        :ok

      {:error, :not_found} ->
        <<prefix::binary-size(2), rest::binary>> = content_hash
        now = DateTime.utc_now() |> DateTime.to_iso8601()

        put_blob_meta(content_hash, %{
          hash: "sha256:#{content_hash}",
          size: size,
          physical_path: Path.join(["cas", "objects", "sha256", prefix, rest]),
          state: :active,
          created_at: now,
          last_seen_at: now
        })
    end
  end
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/metadata_test.exs`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/metadata.ex \
        apps/ex_storage_service/test/ex_storage_service/metadata_test.exs
git commit -m "feat(core): add blob metadata records for global CAS"
```

---

### Task 3: Reserve `cas` as a bucket name

**Files:**
- Modify: `apps/ex_storage_service/lib/ex_storage_service/bucket_validator.ex`
- Test: Create `apps/ex_storage_service/test/ex_storage_service/bucket_validator_test.exs`

**Interfaces:**
- Produces: `BucketValidator.valid_bucket_name?("cas") == false`; `BucketValidator.validate("cas") == {:error, "Bucket name \"cas\" is reserved for internal use."}`. Both the S3 CreateBucket handler (`handlers/bucket.ex:28`) and the web UI (`bucket_live/index.ex:42,55`) already call `validate/1`, so no handler changes are needed.

- [ ] **Step 1: Write the failing tests**

```elixir
# apps/ex_storage_service/test/ex_storage_service/bucket_validator_test.exs
defmodule ExStorageService.BucketValidatorTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BucketValidator

  test "accepts normal S3 bucket names" do
    assert BucketValidator.valid_bucket_name?("my-bucket")
    assert :ok = BucketValidator.validate("my-bucket-123")
  end

  test "rejects the reserved cas name" do
    refute BucketValidator.valid_bucket_name?("cas")
    assert {:error, message} = BucketValidator.validate("cas")
    assert message =~ "reserved"
  end

  test "still accepts names merely containing cas" do
    assert BucketValidator.valid_bucket_name?("cascade")
    assert BucketValidator.valid_bucket_name?("my-cas-bucket")
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/bucket_validator_test.exs`
Expected: FAIL on `rejects the reserved cas name` (the other two pass)

- [ ] **Step 3: Implement**

In `bucket_validator.ex` add a module attribute below the moduledoc and wire it into both public functions:

```elixir
  # Directory names under data_root reserved for internal storage layouts
  # (see docs/prd/git-style-data-model.md §6).
  @reserved_names ["cas"]
```

In `valid_bucket_name?/1`, add to the `and` chain:

```elixir
      not reserved_name?(name) and
```

In `validate/1`, add a clause to the `cond` before `true ->`:

```elixir
      reserved_name?(name) ->
        {:error, "Bucket name \"#{name}\" is reserved for internal use."}
```

Add the private helper next to the others:

```elixir
  defp reserved_name?(name), do: name in @reserved_names
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/bucket_validator_test.exs`
Expected: 3 tests, 0 failures

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/bucket_validator.ex \
        apps/ex_storage_service/test/ex_storage_service/bucket_validator_test.exs
git commit -m "feat(core): reserve cas as bucket name for global CAS root"
```

---

### Task 4: Engine writes/reads the global CAS (with legacy fallback)

**Files:**
- Modify: `apps/ex_storage_service/lib/ex_storage_service/storage/engine.ex` (full rewrite below)
- Test: Create `apps/ex_storage_service/test/ex_storage_service/storage/engine_test.exs`

**Interfaces:**
- Consumes: `CAS.tmp_upload_path/0`, `CAS.commit_blob/2`, `CAS.blob_path/1`, `CAS.has_blob?/1`, `CAS.data_root/0` (Task 1); `Metadata.ensure_blob_meta/2` (Task 2).
- Produces (callers in S3/web/replication apps rely on these — signatures unchanged from today):
  - `Engine.put_object(bucket, key, data_or_stream, content_type \\ "application/octet-stream", metadata \\ %{}) :: {:ok, {content_hash, etag, size}} | {:error, term()}` — now a plain function (no GenServer call).
  - `Engine.put_object_stream(bucket, key, stream, content_type \\ ..., metadata \\ %{})` — same return; delegates to `put_object/5`.
  - `Engine.get_object(bucket, content_hash) :: {:ok, path} | {:error, :not_found}` — CAS first, legacy bucket-local fallback second.
  - `Engine.ensure_bucket_dirs(bucket) :: :ok` — plain function.
  - `Engine.data_root() :: String.t()` — plain function.
  - **New:** `Engine.promote_to_global(bucket, content_hash) :: :ok | {:error, :not_found}` — moves a legacy blob into the CAS (no-op if already global). Used by CopyObject (Task 7) and Migration (Task 9).
  - **New:** `Engine.legacy_content_path(data_root, bucket, content_hash) :: String.t()` — the old bucket-local path (used by Migration and internally).
  - **Removed:** `Engine.content_path/3`, `Engine.delete_content/2`, `Engine.commit_object/4`. Their callsites are updated in Tasks 5, 7, 8 — until those tasks land, expect compile failures if you run other apps' tests; Task 4 only requires the core app to compile, so Task 4's final step runs core tests only. (`Engine.content_path/3` has exactly two external callers: `storage/multipart.ex:151` → Task 5, `handlers/object.ex:477` → Task 7. `Engine.delete_content/2` has one: `bucket_live/files.ex:94` → Task 8. **To keep every task compiling, keep `content_path/3` and `delete_content/2` as deprecated delegates in this task and delete them in Task 8** — see implementation below.)

- [ ] **Step 1: Write the failing tests**

```elixir
# apps/ex_storage_service/test/ex_storage_service/storage/engine_test.exs
defmodule ExStorageService.Storage.EngineTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.CAS
  alias ExStorageService.Storage.Engine

  defp unique_bucket, do: "engine-test-#{:erlang.unique_integer([:positive])}"
  defp sha256_hex(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)

  test "put_object stores content in the global CAS and returns hash/etag/size" do
    bucket = unique_bucket()
    data = "engine-global-#{System.unique_integer()}"
    expected_hash = sha256_hex(data)

    assert {:ok, {^expected_hash, etag, size}} = Engine.put_object(bucket, "k1", data)
    assert etag == Base.encode16(:crypto.hash(:md5, data), case: :lower)
    assert size == byte_size(data)

    assert File.exists?(CAS.blob_path(expected_hash))
    refute File.exists?(Engine.legacy_content_path(Engine.data_root(), bucket, expected_hash))
  end

  test "identical content in two buckets stores exactly one physical blob" do
    data = "dedup-me-#{System.unique_integer()}"
    hash = sha256_hex(data)

    assert {:ok, {^hash, _, _}} = Engine.put_object(unique_bucket(), "a", data)
    assert {:ok, {^hash, _, _}} = Engine.put_object(unique_bucket(), "b", data)

    assert File.exists?(CAS.blob_path(hash))
    assert {:ok, meta} = ExStorageService.Metadata.get_blob_meta(hash)
    assert meta.size == byte_size(data)
  end

  test "put_object accepts a stream of chunks" do
    bucket = unique_bucket()
    chunks = ["chunk-one-", "chunk-two-", "#{System.unique_integer()}"]
    data = IO.iodata_to_binary(chunks)
    hash = sha256_hex(data)

    assert {:ok, {^hash, _etag, size}} = Engine.put_object_stream(bucket, "k", chunks)
    assert size == byte_size(data)
    assert File.read!(CAS.blob_path(hash)) == data
  end

  test "get_object resolves CAS content" do
    bucket = unique_bucket()
    data = "read-me-#{System.unique_integer()}"
    {:ok, {hash, _, _}} = Engine.put_object(bucket, "k", data)

    assert {:ok, path} = Engine.get_object(bucket, hash)
    assert path == CAS.blob_path(hash)
    assert File.read!(path) == data
  end

  test "get_object falls back to the legacy bucket-local layout" do
    bucket = unique_bucket()
    data = "legacy-#{System.unique_integer()}"
    hash = sha256_hex(data)

    legacy_path = Engine.legacy_content_path(Engine.data_root(), bucket, hash)
    File.mkdir_p!(Path.dirname(legacy_path))
    File.write!(legacy_path, data)

    assert {:ok, ^legacy_path} = Engine.get_object(bucket, hash)
  end

  test "promote_to_global moves a legacy blob into the CAS" do
    bucket = unique_bucket()
    data = "promote-#{System.unique_integer()}"
    hash = sha256_hex(data)

    legacy_path = Engine.legacy_content_path(Engine.data_root(), bucket, hash)
    File.mkdir_p!(Path.dirname(legacy_path))
    File.write!(legacy_path, data)

    assert :ok = Engine.promote_to_global(bucket, hash)
    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(legacy_path)
    # idempotent, and blob metadata was created
    assert :ok = Engine.promote_to_global(bucket, hash)
    assert {:ok, _} = ExStorageService.Metadata.get_blob_meta(hash)
    # missing content is reported
    assert {:error, :not_found} = Engine.promote_to_global(bucket, sha256_hex("nope-#{System.unique_integer()}"))
  end
end
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/engine_test.exs`
Expected: FAIL — `undefined function legacy_content_path/3` etc.

- [ ] **Step 3: Rewrite the Engine**

Replace the entire contents of `apps/ex_storage_service/lib/ex_storage_service/storage/engine.ex` with:

```elixir
defmodule ExStorageService.Storage.Engine do
  @moduledoc """
  Storage engine facade over the global content-addressable store
  (`ExStorageService.Storage.CAS`).

  PUTs stream data to a CAS tmp file — computing SHA-256 and MD5 in a
  single pass in the *calling* process — then commit with an atomic
  rename. GETs resolve the global CAS path first and fall back to the
  legacy bucket-local layout (`{data_root}/{bucket}/objects/...`) for
  content written before the global-CAS migration
  (see `ExStorageService.Storage.Migration`).

  The GenServer exists only to create the storage directories at boot;
  every read/write operation is a plain function.
  """

  use GenServer

  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.CAS

  ## Client API

  def start_link(opts) do
    data_root = Keyword.fetch!(opts, :data_root)
    GenServer.start_link(__MODULE__, data_root, name: __MODULE__)
  end

  @doc """
  Returns the data_root path configured for the engine.
  """
  def data_root, do: CAS.data_root()

  @doc """
  Store object data in the global CAS, computing SHA-256 and MD5 in a
  single pass. `data_or_stream` can be a binary or an `Enumerable` of
  binary chunks; streams are enumerated in the calling process, so this
  is safe for `Plug.Conn.read_body/2`-backed streams.

  Returns `{:ok, {content_hash, etag, size}}` on success.
  """
  def put_object(
        _bucket,
        _key,
        data_or_stream,
        _content_type \\ "application/octet-stream",
        _metadata \\ %{}
      ) do
    tmp_path = CAS.tmp_upload_path()

    case stream_to_file(data_or_stream, tmp_path) do
      {:ok, {content_hash, md5, size}} ->
        :ok = CAS.commit_blob(tmp_path, content_hash)
        Metadata.ensure_blob_meta(content_hash, size)
        etag = Base.encode16(md5, case: :lower)
        {:ok, {content_hash, etag, size}}

      {:error, reason} ->
        File.rm(tmp_path)
        {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc """
  Stream-aware PUT. Kept as a separate name for existing callers; the
  write always happens in the calling process now, so this is equivalent
  to `put_object/5`.
  """
  def put_object_stream(
        bucket,
        key,
        stream,
        content_type \\ "application/octet-stream",
        metadata \\ %{}
      ) do
    put_object(bucket, key, stream, content_type, metadata)
  end

  @doc """
  Returns the file path for the given content hash, suitable for sendfile.

  Resolves the global CAS first, then falls back to the legacy
  bucket-local path for content that predates the CAS migration.
  """
  def get_object(bucket, content_hash) do
    legacy = legacy_content_path(CAS.data_root(), bucket, content_hash)

    cond do
      CAS.has_blob?(content_hash) -> {:ok, CAS.blob_path(content_hash)}
      File.exists?(legacy) -> {:ok, legacy}
      true -> {:error, :not_found}
    end
  end

  @doc """
  Ensures the given content hash is present in the global CAS, promoting
  it from the legacy bucket-local layout when necessary (used by
  metadata-only CopyObject and by the migration task).

  Reads of the legacy source keep working after promotion because
  `get_object/2` checks the CAS first.
  """
  def promote_to_global(bucket, content_hash) do
    if CAS.has_blob?(content_hash) do
      :ok
    else
      legacy = legacy_content_path(CAS.data_root(), bucket, content_hash)

      case File.stat(legacy) do
        {:ok, %File.Stat{size: size}} ->
          dest = CAS.blob_path(content_hash)
          File.mkdir_p!(Path.dirname(dest))
          File.rename!(legacy, dest)
          Metadata.ensure_blob_meta(content_hash, size)
          :ok

        {:error, _} ->
          {:error, :not_found}
      end
    end
  end

  @doc """
  Construct the legacy bucket-local filesystem path for a content hash.
  Only pre-migration content lives here; new writes go to the CAS.
  """
  def legacy_content_path(data_root, bucket, content_hash) do
    <<prefix::binary-size(2), rest::binary>> = content_hash
    Path.join([data_root, bucket, "objects", prefix, rest])
  end

  @deprecated "Use legacy_content_path/3 (new content lives in the global CAS)"
  def content_path(data_root, bucket, content_hash) do
    legacy_content_path(data_root, bucket, content_hash)
  end

  @deprecated "Content files are removed by GC only (PRD §10.3); this is now a no-op"
  def delete_content(_bucket, _content_hash) do
    :ok
  end

  @doc """
  Ensure the bucket directory structure exists (legacy layout; still used
  for multipart part staging).
  """
  def ensure_bucket_dirs(bucket) do
    File.mkdir_p!(Path.join([CAS.data_root(), bucket, "objects"]))
    :ok
  end

  ## Server Callbacks

  @impl true
  def init(data_root) do
    data_root = Path.expand(data_root)
    File.mkdir_p!(data_root)
    File.mkdir_p!(Path.join([data_root, CAS.reserved_root(), "objects", "sha256"]))
    File.mkdir_p!(Path.join([data_root, CAS.reserved_root(), "tmp", "uploads"]))
    Logger.info("Storage engine started with data root: #{data_root}")
    {:ok, %{data_root: data_root}}
  end

  ## Private

  defp stream_to_file(data, tmp_path) when is_binary(data) do
    sha256 = :crypto.hash(:sha256, data)
    md5 = :crypto.hash(:md5, data)
    size = byte_size(data)

    case File.write(tmp_path, data) do
      :ok ->
        content_hash = Base.encode16(sha256, case: :lower)
        {:ok, {content_hash, md5, size}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp stream_to_file(stream, tmp_path) do
    try do
      file = File.open!(tmp_path, [:write, :raw, :binary])
      sha256_ctx = :crypto.hash_init(:sha256)
      md5_ctx = :crypto.hash_init(:md5)

      {sha256_ctx, md5_ctx, size} =
        Enum.reduce(stream, {sha256_ctx, md5_ctx, 0}, fn chunk, {sha_ctx, md_ctx, acc_size} ->
          :ok = IO.binwrite(file, chunk)

          sha_ctx = :crypto.hash_update(sha_ctx, chunk)
          md_ctx = :crypto.hash_update(md_ctx, chunk)
          {sha_ctx, md_ctx, acc_size + byte_size(chunk)}
        end)

      File.close(file)

      sha256 = :crypto.hash_final(sha256_ctx)
      md5 = :crypto.hash_final(md5_ctx)
      content_hash = Base.encode16(sha256, case: :lower)

      {:ok, {content_hash, md5, size}}
    rescue
      e -> {:error, Exception.message(e)}
    end
  end
end
```

Notes for the implementer:
- `put_object/5` heads keep `bucket`/`key`/`content_type`/`metadata` parameters (underscored) because all existing callers pass them; do not change caller code in this task.
- The `@deprecated` `content_path/3` and `delete_content/2` delegates keep `multipart.ex`, `handlers/object.ex`, and `bucket_live/files.ex` compiling until Tasks 5, 7, 8 update them. Task 8 deletes both delegates.

- [ ] **Step 4: Run core tests**

Run: `mix test apps/ex_storage_service/test`
Expected: all pass (including the new engine tests; `multipart_test.exs` still passes because `content_path/3` still exists as a delegate)

- [ ] **Step 5: Run S3 and web tests to confirm nothing broke**

Run: `mix test apps/ex_storage_service_s3/test && mix test apps/ex_storage_service_web/test`
Expected: all pass — S3 handlers call `put_object`/`put_object_stream`/`get_object`, whose contracts are unchanged; GETs of pre-existing test content go through the legacy fallback.

- [ ] **Step 6: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/engine.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/engine_test.exs
git commit -m "feat(core): store object content in global CAS with legacy read fallback"
```

---

### Task 5: Multipart completion commits into the global CAS

**Files:**
- Modify: `apps/ex_storage_service/lib/ex_storage_service/storage/multipart.ex:150-153`
- Test: existing `apps/ex_storage_service/test/ex_storage_service/storage/multipart_test.exs` plus one new assertion test

**Interfaces:**
- Consumes: `CAS.commit_blob/2` (Task 1), `Metadata.ensure_blob_meta/2` (Task 2).
- Produces: no API change; completed multipart content now lands at `CAS.blob_path(hash)`.

- [ ] **Step 1: Write the failing test** (append to `multipart_test.exs`, following its existing setup conventions for creating an upload and uploading parts — reuse the module's existing helpers)

```elixir
  test "completed multipart content lands in the global CAS" do
    bucket = "mpu-cas-#{:erlang.unique_integer([:positive])}"
    ExStorageService.Metadata.create_bucket(bucket)

    {:ok, upload_id} = ExStorageService.Storage.Multipart.init_upload(bucket, "big-object")

    part = String.duplicate("a", 5 * 1024 * 1024)
    {:ok, _etag1} = ExStorageService.Storage.Multipart.upload_part(bucket, upload_id, 1, part)
    {:ok, etag2} = ExStorageService.Storage.Multipart.upload_part(bucket, upload_id, 2, "tail")

    parts = [{1, ""}, {2, etag2}]
    assert {:ok, {content_hash, _etag, _size}} =
             ExStorageService.Storage.Multipart.complete_upload(bucket, upload_id, parts)

    assert File.exists?(ExStorageService.Storage.CAS.blob_path(content_hash))
    assert {:ok, _} = ExStorageService.Metadata.get_blob_meta(content_hash)
  end
```

(Adjust `init_upload`/`upload_part`/`complete_upload` call shapes to match the existing tests in that file if they differ — the existing tests are the source of truth for the part-tuple format.)

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/multipart_test.exs`
Expected: the new test FAILS on `File.exists?(CAS.blob_path(...))` (content went to the legacy path); existing tests pass.

- [ ] **Step 3: Implement**

In `multipart.ex`, replace:

```elixir
            # Move to content-addressed storage
            dest = ExStorageService.Storage.Engine.content_path(data_root, bucket, content_hash)
            File.mkdir_p!(Path.dirname(dest))
            File.rename!(tmp_path, dest)
```

with:

```elixir
            # Commit to the global content-addressed store
            :ok = ExStorageService.Storage.CAS.commit_blob(tmp_path, content_hash)
            ExStorageService.Metadata.ensure_blob_meta(content_hash, total_size)
```

If `data_root` becomes unused in `complete_upload` after this change, the tmp-dir setup earlier in the function still uses it — verify with `mix compile --warnings-as-errors` and underscore only if the compiler says so.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/multipart_test.exs`
Expected: all pass

- [ ] **Step 5: Run S3 multipart integration tests**

Run: `mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/multipart_test.exs apps/ex_storage_service_s3/test/ex_storage_service_s3/multipart_edge_test.exs`
Expected: all pass (GET after complete resolves via CAS path in `Engine.get_object/2`)

- [ ] **Step 6: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/multipart.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/multipart_test.exs
git commit -m "feat(core): commit completed multipart uploads to global CAS"
```

---

### Task 6: ContentGC ignores the reserved `cas/` tree

**Files:**
- Modify: `apps/ex_storage_service/lib/ex_storage_service/storage/content_gc.ex:157-168` (`get_disk_content_hashes/1`)
- Test: Create `apps/ex_storage_service/test/ex_storage_service/storage/content_gc_test.exs`

**Interfaces:**
- Consumes: `CAS.reserved_root/0`, `CAS.blob_path/1` (Task 1).
- Produces: no API change. ContentGC keeps owning the **legacy** tree only (PRD §14.3); global-CAS sweep arrives in Phase 4.

**Why this matters:** `get_disk_content_hashes/1` treats every entry under `data_root` as a bucket. `cas/objects/` exists, so without exclusion the GC would descend into `cas/objects/sha256/{ab}/…`, misparse directories as content files, and attempt `File.rm` on them.

- [ ] **Step 1: Write the failing test**

```elixir
# apps/ex_storage_service/test/ex_storage_service/storage/content_gc_test.exs
defmodule ExStorageService.Storage.ContentGCTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.CAS
  alias ExStorageService.Storage.ContentGC

  test "GC never touches blobs under the reserved cas/ root" do
    data = "gc-must-not-touch-#{System.unique_integer()}"
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    path = CAS.blob_path(hash)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, data)

    # Backdate far past the orphan grace window (600s); the blob has no
    # obj:/obj_ver: metadata, so under the legacy rules it would look
    # like a deletable orphan.
    old = System.os_time(:second) - 24 * 3600
    File.touch!(path, old)

    assert {:ok, _deleted} = ContentGC.run_now()

    assert File.exists?(path), "ContentGC must not delete global CAS blobs"
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/content_gc_test.exs`
Expected: FAIL — either the file is deleted or `File.rm` errors are logged; the assertion on `File.exists?` fails. (If it happens to pass because `File.rm` fails on directories at the sharding depth, temporarily verify the warning logs — the fix below is still required; keep the test as the regression guard.)

- [ ] **Step 3: Implement**

In `content_gc.ex`, change `get_disk_content_hashes/1`:

```elixir
  defp get_disk_content_hashes(data_root) do
    case File.ls(data_root) do
      {:ok, entries} ->
        entries
        # The reserved global-CAS root is not a bucket; its blobs are
        # managed by the Phase 4 CAS GC, never by this legacy sweep.
        |> Enum.reject(&(&1 == ExStorageService.Storage.CAS.reserved_root()))
        |> Enum.flat_map(fn bucket_dir ->
          objects_dir = Path.join([data_root, bucket_dir, "objects"])
          list_content_files(bucket_dir, objects_dir)
        end)

      {:error, _} ->
        []
    end
  end
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/content_gc_test.exs`
Expected: 1 test, 0 failures

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/content_gc.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/content_gc_test.exs
git commit -m "fix(core): exclude reserved cas/ root from legacy content GC scan"
```

---

### Task 7: CopyObject becomes metadata-only for local buckets

**Files:**
- Modify: `apps/ex_storage_service_s3/lib/ex_storage_service_s3/handlers/object.ex:401-485`
- Test: Create `apps/ex_storage_service_s3/test/ex_storage_service_s3/global_cas_test.exs`

**Interfaces:**
- Consumes: `Engine.promote_to_global/2` (Task 4), `CAS.blob_path/1` (Task 1).
- Produces: cross-bucket CopyObject no longer duplicates content on disk. The cloud-cache copy clause (`copy_destination_content/6` with `{:ok, cloud_config}`) is untouched.

- [ ] **Step 1: Write the failing integration tests**

```elixir
# apps/ex_storage_service_s3/test/ex_storage_service_s3/global_cas_test.exs
defmodule ExStorageServiceS3.GlobalCasTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.CAS
  alias ExStorageService.Storage.Engine

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "gcas-#{:erlang.unique_integer([:positive])}"
  defp sha256_hex(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)

  defp create_bucket(bucket) do
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")
    bucket
  end

  test "PUT of identical content to two buckets stores one physical blob" do
    b1 = create_bucket(unique_bucket())
    b2 = create_bucket(unique_bucket())
    data = "same-bytes-#{System.unique_integer()}"
    hash = sha256_hex(data)

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{b1}/one.txt", body: data)
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{b2}/two.txt", body: data)

    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(Engine.legacy_content_path(Engine.data_root(), b1, hash))
    refute File.exists?(Engine.legacy_content_path(Engine.data_root(), b2, hash))

    {:ok, %{status: 200, body: body}} = Req.get("#{@base_url}/#{b2}/two.txt")
    assert body == data
  end

  test "cross-bucket CopyObject is metadata-only and destination is readable" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())
    data = "copy-me-#{System.unique_integer()}"
    hash = sha256_hex(data)

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/orig.txt", body: data)

    {:ok, %{status: 200}} =
      Req.put("#{@base_url}/#{dst}/copied.txt",
        headers: [{"x-amz-copy-source", "/#{src}/orig.txt"}],
        body: ""
      )

    # exactly one physical file: the CAS blob; no legacy dest copy
    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(Engine.legacy_content_path(Engine.data_root(), dst, hash))

    {:ok, %{status: 200, body: body}} = Req.get("#{@base_url}/#{dst}/copied.txt")
    assert body == data

    # source unaffected
    {:ok, %{status: 200, body: src_body}} = Req.get("#{@base_url}/#{src}/orig.txt")
    assert src_body == data
  end

  test "CopyObject promotes pre-migration legacy content into the CAS" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())
    data = "legacy-copy-#{System.unique_integer()}"
    hash = sha256_hex(data)

    # simulate a pre-migration object: legacy file + obj: metadata, no CAS blob
    legacy = Engine.legacy_content_path(Engine.data_root(), src, hash)
    File.mkdir_p!(Path.dirname(legacy))
    File.write!(legacy, data)

    now = DateTime.utc_now() |> DateTime.to_iso8601()

    ExStorageService.Metadata.put_object_meta(src, "old.txt", %{
      content_hash: hash,
      size: byte_size(data),
      etag: Base.encode16(:crypto.hash(:md5, data), case: :lower),
      content_type: "text/plain",
      metadata: %{},
      created_at: now,
      updated_at: now
    })

    {:ok, %{status: 200}} =
      Req.put("#{@base_url}/#{dst}/new.txt",
        headers: [{"x-amz-copy-source", "/#{src}/old.txt"}],
        body: ""
      )

    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(legacy)

    # both source and destination readable after promotion
    {:ok, %{status: 200, body: b1}} = Req.get("#{@base_url}/#{src}/old.txt")
    {:ok, %{status: 200, body: b2}} = Req.get("#{@base_url}/#{dst}/new.txt")
    assert b1 == data and b2 == data
  end

  test "bucket named cas is rejected" do
    {:ok, resp} = Req.put("#{@base_url}/cas", body: "")
    assert resp.status == 400
  end
end
```

- [ ] **Step 2: Run tests to verify current behavior fails them**

Run: `mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/global_cas_test.exs`
Expected: the two CopyObject tests FAIL (legacy dest copy exists / promotion doesn't happen). The PUT-dedup test and `cas` rejection test may already pass after Tasks 3–4 — that's fine.

- [ ] **Step 3: Implement**

In `handlers/object.ex`, replace the local-copy helpers (currently `copy_destination_content/6` `:disabled` clause at lines 401–410, `copy_local_destination_content/3` both clauses at 433–446, and `copy_local_content/3` at 471–485) with:

```elixir
  defp copy_destination_content(
         source_bucket,
         _source_key,
         _dest_bucket,
         _dest_key,
         source_meta,
         :disabled
       ) do
    # Content is globally addressed: ensure the blob is in the CAS
    # (promoting pre-migration legacy content), then the copy is
    # metadata-only — no physical file duplication.
    case Engine.promote_to_global(source_bucket, source_meta.content_hash) do
      :ok -> :ok
      {:error, :not_found} -> {:error, :source_missing}
    end
  end
```

Delete `copy_local_destination_content/3` (both clauses) and `copy_local_content/3` entirely. The cloud-cache clause of `copy_destination_content/6` (lines 412–431) stays as-is.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/global_cas_test.exs`
Expected: 4 tests, 0 failures

- [ ] **Step 5: Run the full S3 suite**

Run: `mix test apps/ex_storage_service_s3/test`
Expected: all pass (existing CopyObject tests in `s3_api_test.exs` / `extended_features_test.exs` now exercise the metadata-only path)

- [ ] **Step 6: Commit**

```bash
git add apps/ex_storage_service_s3/lib/ex_storage_service_s3/handlers/object.ex \
        apps/ex_storage_service_s3/test/ex_storage_service_s3/global_cas_test.exs
git commit -m "feat(s3): make CopyObject metadata-only via global CAS"
```

---

### Task 8: Web UI stops deleting content files; remove deprecated Engine delegates

**Files:**
- Modify: `apps/ex_storage_service_web/lib/ex_storage_service_web/live/bucket_live/files.ex` (line 94 and the `alias ExStorageService.Storage.Engine` at line 5)
- Modify: `apps/ex_storage_service/lib/ex_storage_service/storage/engine.ex` (delete the two `@deprecated` delegates)

**Interfaces:**
- Consumes: nothing new.
- Produces: physical deletion is GC-only everywhere; `Engine.content_path/3` and `Engine.delete_content/2` no longer exist (all callers were updated in Tasks 5 and 7).

- [ ] **Step 1: Remove the web callsite**

In `files.ex`, in `handle_event("confirm_delete_object", ...)`, delete the line:

```elixir
        Engine.delete_content(bucket, meta.content_hash)
```

and delete the now-unused alias at the top of the module:

```elixir
  alias ExStorageService.Storage.Engine
```

(Verify with `grep -n "Engine\." apps/ex_storage_service_web/lib/ex_storage_service_web/live/bucket_live/files.ex` that no other usage remains — `bucket_live/index.ex` keeps its own `ensure_bucket_dirs` usage and is untouched.)

- [ ] **Step 2: Delete the deprecated Engine delegates**

In `engine.ex`, remove both functions added in Task 4:

```elixir
  @deprecated "Use legacy_content_path/3 (new content lives in the global CAS)"
  def content_path(data_root, bucket, content_hash) do
    legacy_content_path(data_root, bucket, content_hash)
  end

  @deprecated "Content files are removed by GC only (PRD §10.3); this is now a no-op"
  def delete_content(_bucket, _content_hash) do
    :ok
  end
```

- [ ] **Step 3: Verify no remaining callers, compile strictly**

Run: `grep -rn "content_path\|delete_content" apps --include='*.ex' --include='*.exs' | grep -v legacy_content_path | grep -v "storage/engine.ex"`
Expected: no matches outside comments/tests that reference legacy naming.

Run: `mix compile --warnings-as-errors`
Expected: clean

- [ ] **Step 4: Run web + core tests**

Run: `mix test apps/ex_storage_service_web/test && mix test apps/ex_storage_service/test`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service_web/lib/ex_storage_service_web/live/bucket_live/files.ex \
        apps/ex_storage_service/lib/ex_storage_service/storage/engine.ex
git commit -m "feat(web): defer object content deletion to GC (global CAS)"
```

---

### Task 9: Migration task for legacy content

**Files:**
- Create: `apps/ex_storage_service/lib/ex_storage_service/storage/migration.ex`
- Create: `apps/ex_storage_service/lib/mix/tasks/ess.migrate_cas.ex`
- Test: `apps/ex_storage_service/test/ex_storage_service/storage/migration_test.exs`

**Interfaces:**
- Consumes: `Concord.get_all/0`; `Engine.promote_to_global/2`, `Engine.legacy_content_path/3` (Task 4); `CAS.has_blob?/1`, `CAS.blob_path/1` (Task 1); `Metadata.ensure_blob_meta/2` (Task 2).
- Produces: `Migration.migrate_to_global_cas() :: {:ok, %{migrated: n, already_global: n, missing: [{bucket, hash}]}} | {:error, term()}`; mix task `mix ess.migrate_cas`. Physical files only — object metadata is not rewritten (PRD §20 Phase 1). Legacy duplicates on a dedup hit are left in place for admin-confirmed cleanup (PRD §19.3 step 7–8).

- [ ] **Step 1: Write the failing tests**

```elixir
# apps/ex_storage_service/test/ex_storage_service/storage/migration_test.exs
defmodule ExStorageService.Storage.MigrationTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, Engine, Migration}

  defp unique_bucket, do: "mig-#{:erlang.unique_integer([:positive])}"
  defp sha256_hex(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)

  defp seed_legacy_object(bucket, key, data) do
    hash = sha256_hex(data)
    legacy = Engine.legacy_content_path(Engine.data_root(), bucket, hash)
    File.mkdir_p!(Path.dirname(legacy))
    File.write!(legacy, data)

    now = DateTime.utc_now() |> DateTime.to_iso8601()

    Metadata.put_object_meta(bucket, key, %{
      content_hash: hash,
      size: byte_size(data),
      etag: Base.encode16(:crypto.hash(:md5, data), case: :lower),
      content_type: "application/octet-stream",
      metadata: %{},
      created_at: now,
      updated_at: now
    })

    hash
  end

  test "moves legacy files into the CAS and reports counts" do
    bucket = unique_bucket()
    Metadata.create_bucket(bucket)
    h1 = seed_legacy_object(bucket, "a.bin", "mig-data-1-#{System.unique_integer()}")
    h2 = seed_legacy_object(bucket, "b.bin", "mig-data-2-#{System.unique_integer()}")

    assert {:ok, report} = Migration.migrate_to_global_cas()

    assert report.migrated >= 2
    assert CAS.has_blob?(h1) and CAS.has_blob?(h2)
    refute File.exists?(Engine.legacy_content_path(Engine.data_root(), bucket, h1))
    assert {:ok, _} = Metadata.get_blob_meta(h1)
    # objects remain readable through the engine
    assert {:ok, _path} = Engine.get_object(bucket, h1)
  end

  test "is idempotent and counts already-global blobs" do
    bucket = unique_bucket()
    Metadata.create_bucket(bucket)
    h = seed_legacy_object(bucket, "c.bin", "mig-data-3-#{System.unique_integer()}")

    assert {:ok, _} = Migration.migrate_to_global_cas()
    assert {:ok, report2} = Migration.migrate_to_global_cas()

    assert report2.already_global >= 1
    assert CAS.has_blob?(h)
  end

  test "reports metadata pointing at missing files" do
    bucket = unique_bucket()
    Metadata.create_bucket(bucket)
    ghost_hash = sha256_hex("ghost-#{System.unique_integer()}")

    now = DateTime.utc_now() |> DateTime.to_iso8601()

    Metadata.put_object_meta(bucket, "ghost.bin", %{
      content_hash: ghost_hash,
      size: 5,
      etag: "0",
      content_type: "application/octet-stream",
      metadata: %{},
      created_at: now,
      updated_at: now
    })

    assert {:ok, report} = Migration.migrate_to_global_cas()
    assert {bucket, ghost_hash} in report.missing
  end
end
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/migration_test.exs`
Expected: FAIL — `module ExStorageService.Storage.Migration is not available`

- [ ] **Step 3: Implement the migration module**

```elixir
# apps/ex_storage_service/lib/ex_storage_service/storage/migration.ex
defmodule ExStorageService.Storage.Migration do
  @moduledoc """
  One-shot migration of legacy bucket-local content files
  (`{data_root}/{bucket}/objects/...`) into the global CAS
  (`{data_root}/cas/objects/sha256/...`).

  Physical files only: object metadata (`obj:*`, `obj_ver:*`) is not
  rewritten in Phase 1 (see docs/prd/git-style-data-model.md §19–20).
  Run in maintenance mode (no concurrent writes). Idempotent: re-running
  counts already-migrated blobs under `:already_global`.
  """

  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, Engine}

  @doc """
  Migrates every content hash referenced by `obj:*` / `obj_ver:*`
  metadata. Returns `{:ok, report}` with `:migrated`, `:already_global`,
  and `:missing` (list of `{bucket, hash}` whose content exists in
  neither layout — repair-worker input).
  """
  def migrate_to_global_cas do
    case Concord.get_all() do
      {:ok, all} ->
        report =
          all
          |> Enum.flat_map(&referenced_hash/1)
          |> Enum.uniq()
          |> Enum.reduce(%{migrated: 0, already_global: 0, missing: []}, &migrate_one/2)

        Logger.info("CAS migration: #{inspect(Map.delete(report, :missing))}, missing: #{length(report.missing)}")

        {:ok, report}

      error ->
        error
    end
  end

  # Key formats: "obj:{bucket}:{key}" and "obj_ver:{bucket}:{key}:{vid}".
  # In both, the segment after the first colon is the bucket.
  defp referenced_hash({key, value}) do
    with true <- String.starts_with?(key, "obj:") or String.starts_with?(key, "obj_ver:"),
         [_ns, bucket, _rest] <- String.split(key, ":", parts: 3),
         hash when is_binary(hash) <- Map.get(value, :content_hash) do
      [{bucket, hash}]
    else
      _ -> []
    end
  end

  defp migrate_one({bucket, hash}, acc) do
    cond do
      CAS.has_blob?(hash) ->
        ensure_meta_from_disk(hash)
        %{acc | already_global: acc.already_global + 1}

      true ->
        case Engine.promote_to_global(bucket, hash) do
          :ok ->
            %{acc | migrated: acc.migrated + 1}

          {:error, :not_found} ->
            Logger.warning("CAS migration: content missing for #{bucket} hash #{hash}")
            %{acc | missing: [{bucket, hash} | acc.missing]}
        end
    end
  end

  defp ensure_meta_from_disk(hash) do
    case File.stat(CAS.blob_path(hash)) do
      {:ok, %File.Stat{size: size}} -> Metadata.ensure_blob_meta(hash, size)
      {:error, _} -> :ok
    end
  end
end
```

- [ ] **Step 4: Implement the mix task**

```elixir
# apps/ex_storage_service/lib/mix/tasks/ess.migrate_cas.ex
defmodule Mix.Tasks.Ess.MigrateCas do
  @shortdoc "Migrate legacy bucket-local content files into the global CAS"

  @moduledoc """
  Moves all content files referenced by object metadata from the legacy
  bucket-local layout into the global CAS. Run with the service stopped
  or in maintenance mode:

      mix ess.migrate_cas

  Idempotent. Prints a report; metadata entries whose content is missing
  in both layouts are listed for manual repair. Legacy duplicate files
  (dedup hits) are left in place — delete the legacy layout only after
  verifying the report (PRD §19.3).
  """

  use Mix.Task

  @requirements ["app.start"]

  @impl Mix.Task
  def run(_args) do
    case ExStorageService.Storage.Migration.migrate_to_global_cas() do
      {:ok, report} ->
        Mix.shell().info("Migrated: #{report.migrated}")
        Mix.shell().info("Already global: #{report.already_global}")
        Mix.shell().info("Missing content: #{length(report.missing)}")

        Enum.each(report.missing, fn {bucket, hash} ->
          Mix.shell().error("  missing: bucket=#{bucket} hash=#{hash}")
        end)

      {:error, reason} ->
        Mix.raise("CAS migration failed: #{inspect(reason)}")
    end
  end
end
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/migration_test.exs`
Expected: 3 tests, 0 failures

- [ ] **Step 6: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/migration.ex \
        apps/ex_storage_service/lib/mix/tasks/ess.migrate_cas.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/migration_test.exs
git commit -m "feat(core): add legacy-to-global CAS migration task"
```

---

### Task 10: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Format check**

Run: `mix format && git diff --exit-code`
Expected: no formatting changes (if there are, commit them as `style: format`)

- [ ] **Step 2: Strict compile**

Run: `mix compile --warnings-as-errors`
Expected: clean

- [ ] **Step 3: Full test suites, per app**

Run:
```bash
mix test apps/ex_storage_service/test
mix test apps/ex_storage_service_s3/test
mix test apps/ex_storage_service_web/test
```
Expected: all pass. (CLI app is untouched; skip per PRD scope.)

- [ ] **Step 4: Acceptance spot-check against PRD §21.1 / §21.3 / §21.5**

- Dedup: covered by `global_cas_test.exs` (two buckets → one blob; copy → no physical copy) and `engine_test.exs`.
- Crash safety: a failed `put_object` leaves only a `cas/tmp/uploads/` file, never a visible blob or metadata — verify `stream_to_file` error path removes the tmp file (engine code) and note any gap.
- GC safety: `content_gc_test.exs` proves the legacy sweep cannot touch `cas/`.

- [ ] **Step 5: Report**

Summarize: tests run, dedup verified, any deviations from this plan.
