# Pack Storage (Git-Style Data Model Phase 6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cold blobs get consolidated into immutable, content-addressed pack files (`cas/packs/pack-{hash}.pack` + `.idx` sidecar) to cut file/inode count, while every read path — S3 GET/Range, versioned GET, CopyObject source reads, replication — keeps working transparently. CAS identity is preserved: blobs are still addressed by their SHA-256; only the physical location changes.

**Architecture:** Packs are **uncompressed concatenations** of blob contents. That single decision preserves all serving semantics: a packed blob is served with `send_file(conn, status, pack_path, offset, length)` — still zero-copy, still exact `Content-Length`, and Range requests are just `pack_offset + range_offset`. The pack file is itself content-addressed (named by the SHA-256 of its own bytes). The index lives twice: a `pack:{pack_hash}` Concord record (fast lookup) and a JSON `.idx` sidecar (repair). Each packed blob's `blob:sha256:{hash}` record gets `state: :packed` plus `pack: %{hash:, offset:}`; lookups are O(1) via blob metadata, no pack scanning. A `Storage.Packer` GenServer applies the cold policy (age-based; per-bucket S3 `Transition` rules are a recorded follow-up). Only **reachable, active, loose** blobs are packed — unreachable blobs are CasGC's business, and packs are never mutated (repack/pack-GC of dead entries is a recorded follow-up).

**Crash safety (write order):** tmp pack written + hashed → renamed to final pack path → `.idx` sidecar → Concord pack record → per-blob metadata update (`:packed`) → loose file deletion last. A crash at any point leaves blobs readable (loose copies win until deleted; a blob that is both loose and packed is harmless).

## Global Constraints

- Same as prior plans (no new deps, `--warnings-as-errors`, format, conventional commits, Req-based S3 integration tests on `localhost:9001`).
- `Engine.get_object/2` keeps its `{:ok, path} | {:error, :not_found}` contract for **loose + legacy** blobs (tests and multipart use it); packed-aware callers move to the new `Engine.get_object_location/2` / `Engine.read_object/2`.
- Multipart part blobs are never packed in practice (they are fresh and deleted after completion), and `Multipart.concatenate_parts/1` may keep reading loose paths directly.
- `CasGC` must not interact with packed blobs: its candidate scan reads loose files only (already true — packed blobs have no loose file), and the Packer only packs *reachable* blobs so the two never compete. Add a regression test.
- Packer defaults: enabled, sweep every 6h, `pack_cold_after` 30 days, `pack_min_blobs` 100, `pack_max_blobs` 1000, `pack_max_bytes` 1 GiB. All overridable per `pack_now/1` call for tests.

---

### Task 1: `Storage.Pack` — format, writer, reader

**Files:**
- Create: `apps/ex_storage_service/lib/ex_storage_service/storage/pack.ex`
- Test: Create: `apps/ex_storage_service/test/ex_storage_service/storage/pack_test.exs`

**Interfaces:**
- Consumes: `CAS.data_root/0`, `CAS.reserved_root/0`, `CAS.blob_path/1`; `Metadata.get_blob_meta/1`, `put_blob_meta/2`; `Concord`.
- Produces:
  - `Pack.pack_blobs([hash]) :: {:ok, %{pack_hash: String.t(), packed: non_neg_integer()}} | {:error, term()}` — packs the given **loose** blobs (skips any that are missing or already packed); no-op `{:ok, %{pack_hash: nil, packed: 0}}` for an effectively empty list.
  - `Pack.locate(hash) :: {:ok, {path, offset, size}} | {:error, :not_found}` — location of a **packed** blob via its blob metadata.
  - `Pack.read(hash) :: {:ok, binary} | {:error, term()}` — pread of a packed blob.
  - `Pack.pack_path(pack_hash) :: String.t()` — `{data_root}/cas/packs/pack-{hash}.pack`.

- [ ] **Step 1: Write the failing tests**

```elixir
# apps/ex_storage_service/test/ex_storage_service/storage/pack_test.exs
defmodule ExStorageService.Storage.PackTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, Pack}

  defp seed_loose_blob(data) do
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    path = CAS.blob_path(hash)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, data)
    Metadata.ensure_blob_meta(hash, byte_size(data))
    hash
  end

  test "pack_blobs consolidates loose blobs and preserves CAS identity" do
    d1 = "pack-me-1-#{System.unique_integer()}"
    d2 = "pack-me-2-#{System.unique_integer()}"
    h1 = seed_loose_blob(d1)
    h2 = seed_loose_blob(d2)

    assert {:ok, %{pack_hash: pack_hash, packed: 2}} = Pack.pack_blobs([h1, h2])

    # pack file is content-addressed by its own bytes
    pack_path = Pack.pack_path(pack_hash)
    assert File.exists?(pack_path)
    assert Base.encode16(:crypto.hash(:sha256, File.read!(pack_path)), case: :lower) == pack_hash
    assert File.exists?(pack_path <> ".idx")

    # loose files are gone; blob metadata points into the pack
    refute File.exists?(CAS.blob_path(h1))
    assert {:ok, %{state: :packed, pack: %{hash: ^pack_hash}}} = Metadata.get_blob_meta(h1)

    # reads return the original bytes
    assert {:ok, ^d1} = Pack.read(h1)
    assert {:ok, ^d2} = Pack.read(h2)

    assert {:ok, {^pack_path, offset, size}} = Pack.locate(h2)
    assert size == byte_size(d2)
    assert offset == byte_size(d1)
  end

  test "pack_blobs skips missing and already-packed blobs" do
    d = "pack-skip-#{System.unique_integer()}"
    h = seed_loose_blob(d)
    missing = Base.encode16(:crypto.hash(:sha256, "ghost-#{System.unique_integer()}"), case: :lower)

    assert {:ok, %{packed: 1}} = Pack.pack_blobs([h, missing])
    # re-packing the same blob is a no-op
    assert {:ok, %{packed: 0, pack_hash: nil}} = Pack.pack_blobs([h])
    assert {:ok, ^d} = Pack.read(h)
  end

  test "locate on a loose or unknown blob returns not_found" do
    h = seed_loose_blob("still-loose-#{System.unique_integer()}")
    assert {:error, :not_found} = Pack.locate(h)
  end
end
```

- [ ] **Step 2: Run to verify failure** — `mix test apps/ex_storage_service/test/ex_storage_service/storage/pack_test.exs` → module not available.

- [ ] **Step 3: Implement**

```elixir
# apps/ex_storage_service/lib/ex_storage_service/storage/pack.ex
defmodule ExStorageService.Storage.Pack do
  @moduledoc """
  Immutable, content-addressed pack files for cold blobs.

  A pack is an **uncompressed concatenation** of blob contents at
  `{data_root}/cas/packs/pack-{sha256-of-pack-bytes}.pack`, so a packed
  blob is served with `send_file(path, offset, size)` — zero-copy, exact
  Content-Length, and Range = pack_offset + range_offset. CAS identity is
  preserved: blobs stay addressed by their SHA-256; `blob:sha256:{hash}`
  metadata carries `state: :packed` and `pack: %{hash:, offset:}`.

  The index lives twice: a `pack:{pack_hash}` Concord record and a JSON
  `.idx` sidecar for repair. Packs are never mutated; reclaiming dead
  entries (repack) is a future follow-up.

  Crash-safe write order: tmp pack → rename → sidecar → pack record →
  per-blob metadata → loose deletion last. A blob that is briefly both
  loose and packed is harmless (loose wins on reads).
  """

  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.CAS

  def pack_path(pack_hash) do
    Path.join([CAS.data_root(), CAS.reserved_root(), "packs", "pack-#{pack_hash}.pack"])
  end

  @doc """
  Pack the given loose blobs. Skips hashes whose loose file is missing or
  whose metadata is already `:packed`. Returns the new pack's hash and the
  number of blobs packed.
  """
  def pack_blobs(hashes) do
    entries =
      hashes
      |> Enum.uniq()
      |> Enum.filter(&packable?/1)

    if entries == [] do
      {:ok, %{pack_hash: nil, packed: 0}}
    else
      write_pack(entries)
    end
  end

  @doc "Location of a packed blob: `{:ok, {pack_path, offset, size}}`."
  def locate(hash) do
    with {:ok, %{state: :packed, pack: pack_info}} <- Metadata.get_blob_meta(hash),
         pack_hash when is_binary(pack_hash) <- get_field(pack_info, :hash),
         offset when is_integer(offset) <- get_field(pack_info, :offset),
         {:ok, %{size: size}} <- Metadata.get_blob_meta(hash) do
      {:ok, {pack_path(pack_hash), offset, size}}
    else
      _ -> {:error, :not_found}
    end
  end

  @doc "Read a packed blob's bytes."
  def read(hash) do
    with {:ok, {path, offset, size}} <- locate(hash),
         {:ok, fd} <- File.open(path, [:read, :raw, :binary]) do
      try do
        case :file.pread(fd, offset, size) do
          {:ok, data} -> {:ok, data}
          :eof -> {:error, :corrupt_pack}
          {:error, reason} -> {:error, reason}
        end
      after
        File.close(fd)
      end
    end
  end

  ## Private

  defp packable?(hash) do
    File.exists?(CAS.blob_path(hash)) and
      case Metadata.get_blob_meta(hash) do
        {:ok, %{state: :packed}} -> false
        _ -> true
      end
  end

  defp write_pack(hashes) do
    tmp_dir = Path.join([CAS.data_root(), CAS.reserved_root(), "tmp"])
    File.mkdir_p!(tmp_dir)
    tmp_path = Path.join(tmp_dir, "pack-#{:erlang.unique_integer([:positive])}.tmp")

    out = File.open!(tmp_path, [:write, :raw, :binary])

    try do
      {index, _offset, sha_ctx} =
        Enum.reduce(hashes, {[], 0, :crypto.hash_init(:sha256)}, fn hash, {idx, offset, ctx} ->
          data = File.read!(CAS.blob_path(hash))
          :ok = IO.binwrite(out, data)
          size = byte_size(data)
          entry = %{blob_hash: hash, offset: offset, size: size}
          {[entry | idx], offset + size, :crypto.hash_update(ctx, data)}
        end)

      File.close(out)

      index = Enum.reverse(index)
      pack_hash = sha_ctx |> :crypto.hash_final() |> Base.encode16(case: :lower)
      dest = pack_path(pack_hash)
      File.mkdir_p!(Path.dirname(dest))
      File.rename!(tmp_path, dest)

      write_sidecar(dest, pack_hash, index)

      total_size = Enum.reduce(index, 0, &(&1.size + &2))

      Concord.put("pack:#{pack_hash}", %{
        hash: pack_hash,
        entries: index,
        size: total_size,
        blob_count: length(index),
        created_at: DateTime.utc_now() |> DateTime.to_iso8601()
      })

      Enum.each(index, fn %{blob_hash: hash, offset: offset} ->
        mark_packed(hash, pack_hash, offset)
        File.rm(CAS.blob_path(hash))
      end)

      Logger.info("Pack: packed #{length(index)} blobs into pack-#{pack_hash} (#{total_size} bytes)")

      {:ok, %{pack_hash: pack_hash, packed: length(index)}}
    rescue
      e ->
        File.close(out)
        File.rm(tmp_path)
        {:error, Exception.message(e)}
    end
  end

  defp write_sidecar(dest, pack_hash, index) do
    sidecar =
      JSON.encode!(%{
        format: "ess-pack-v1",
        hash: pack_hash,
        entries: Enum.map(index, fn e -> [e.blob_hash, e.offset, e.size] end)
      })

    File.write!(dest <> ".idx", sidecar)
  end

  defp mark_packed(hash, pack_hash, offset) do
    case Metadata.get_blob_meta(hash) do
      {:ok, meta} ->
        meta
        |> Map.put(:state, :packed)
        |> Map.put(:pack, %{hash: pack_hash, offset: offset})
        |> Map.put(:physical_path, Path.join(["cas", "packs", "pack-#{pack_hash}.pack"]))
        |> then(&Metadata.put_blob_meta(hash, &1))

      {:error, :not_found} ->
        Metadata.put_blob_meta(hash, %{
          hash: "sha256:#{hash}",
          size: nil,
          physical_path: Path.join(["cas", "packs", "pack-#{pack_hash}.pack"]),
          state: :packed,
          pack: %{hash: pack_hash, offset: offset},
          created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
          last_seen_at: DateTime.utc_now() |> DateTime.to_iso8601()
        })
    end
  end

  defp get_field(map, key) when is_map(map), do: map[key] || map[to_string(key)]
  defp get_field(_, _), do: nil
end
```

**Implementer note on `mark_packed/3` for never-seen blobs:** `size: nil` would break `locate/1`; instead, when metadata is missing, `File.stat` the loose file *before* deletion and store its size. Restructure: capture size in the pack index entry (it is already there) and pass `entry.size` into `mark_packed(hash, pack_hash, offset, size)` — use that in both branches. Adjust the code accordingly (the test's `locate/1` assertion on size will catch it).

- [ ] **Step 4: Run tests** — pack_test 3 tests green, then `mix test apps/ex_storage_service/test`.

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/pack.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/pack_test.exs
git commit -m "feat(core): add content-addressed pack files for cold blobs"
```

---

### Task 2: `Storage.Packer` policy GenServer + Engine packed-aware reads

**Files:**
- Create: `apps/ex_storage_service/lib/ex_storage_service/storage/packer.ex`
- Modify: `apps/ex_storage_service/lib/ex_storage_service/storage/engine.ex` (add `get_object_location/2`, `read_object/2`; make `promote_to_global/2` treat packed as present)
- Modify: `apps/ex_storage_service/lib/ex_storage_service/storage/cas_gc.ex` (make `reachable_hashes/1` public as `@doc false def reachable_hashes(all)` so Packer reuses it)
- Modify: `apps/ex_storage_service/lib/ex_storage_service/application.ex` (add `ExStorageService.Storage.Packer` after `CasGC`)
- Test: Create: `apps/ex_storage_service/test/ex_storage_service/storage/packer_test.exs`

**Interfaces:**
- `Packer.pack_now(opts \\ []) :: {:ok, %{pack_hash:, packed:}}` — opts: `:cold_after` (seconds), `:min_blobs`, `:max_blobs`, `:max_bytes`. Selects loose, **reachable**, cold blobs and calls `Pack.pack_blobs/1`.
- `Engine.get_object_location(bucket, hash) :: {:ok, {:file, path}} | {:ok, {:pack, path, offset, size}} | {:error, :not_found}` — loose CAS → pack → legacy, in that order.
- `Engine.read_object(bucket, hash) :: {:ok, binary} | {:error, term()}`.
- `Engine.promote_to_global/2` returns `:ok` when the blob is packed.

- [ ] **Step 1: Write the failing tests**

```elixir
# apps/ex_storage_service/test/ex_storage_service/storage/packer_test.exs
defmodule ExStorageService.Storage.PackerTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, CasGC, Engine, Packer}

  defp put_and_reference(data) do
    bucket = "packer-#{:erlang.unique_integer([:positive])}"
    {:ok, {hash, etag, size}} = Engine.put_object(bucket, "k", data)

    Metadata.put_object_meta(bucket, "k", %{
      content_hash: hash,
      size: size,
      etag: etag,
      created_at: DateTime.utc_now() |> DateTime.to_iso8601()
    })

    # backdate the loose file so it is "cold"
    File.touch!(CAS.blob_path(hash), System.os_time(:second) - 90 * 86_400)
    {bucket, hash, data}
  end

  test "packs cold reachable blobs; reads keep working through the Engine" do
    {bucket, hash, data} = put_and_reference("cold-data-#{System.unique_integer()}")

    assert {:ok, %{packed: packed}} = Packer.pack_now(cold_after: 0, min_blobs: 1)
    assert packed >= 1
    refute File.exists?(CAS.blob_path(hash))

    assert {:ok, {:pack, pack_path, offset, size}} = Engine.get_object_location(bucket, hash)
    assert File.exists?(pack_path)
    assert size == byte_size(data)
    assert is_integer(offset)

    assert {:ok, ^data} = Engine.read_object(bucket, hash)
    assert :ok = Engine.promote_to_global(bucket, hash)
  end

  test "does not pack fresh, unreachable, or already-packed blobs" do
    # fresh + reachable
    {_bucket, fresh_hash, _} = put_and_reference("fresh-#{System.unique_integer()}")
    File.touch!(CAS.blob_path(fresh_hash), System.os_time(:second))

    # cold but unreachable (no obj/obj_ver references)
    orphan_data = "orphan-#{System.unique_integer()}"
    {:ok, {orphan_hash, _, _}} = Engine.put_object("packer-orphan", "k", orphan_data)
    File.touch!(CAS.blob_path(orphan_hash), System.os_time(:second) - 90 * 86_400)

    {:ok, _} = Packer.pack_now(cold_after: 3600, min_blobs: 1)

    assert File.exists?(CAS.blob_path(fresh_hash)), "fresh blob must stay loose"
    assert File.exists?(CAS.blob_path(orphan_hash)), "unreachable blob is GC's business, not the packer's"
  end

  test "respects the min_blobs threshold" do
    {_bucket, hash, _} = put_and_reference("threshold-#{System.unique_integer()}")

    {:ok, %{packed: 0}} = Packer.pack_now(cold_after: 0, min_blobs: 1_000_000)
    assert File.exists?(CAS.blob_path(hash))
  end

  test "CasGC ignores packed blobs" do
    {_bucket, hash, _} = put_and_reference("gc-packed-#{System.unique_integer()}")
    {:ok, %{packed: p}} = Packer.pack_now(cold_after: 0, min_blobs: 1)
    assert p >= 1

    {:ok, _} = CasGC.run_now(orphan_mtime_grace: 0, candidate_grace: 0, quarantine_grace: 0)
    assert {:ok, %{state: :packed}} = Metadata.get_blob_meta(hash)
    assert {:error, :not_found} = get_candidate(hash)
  end

  defp get_candidate(hash) do
    case Concord.get("gc:candidate:#{hash}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, v} -> {:ok, v}
      other -> other
    end
  end
end
```

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement the Packer**

```elixir
# apps/ex_storage_service/lib/ex_storage_service/storage/packer.ex
defmodule ExStorageService.Storage.Packer do
  @moduledoc """
  Cold-storage policy for the global CAS (PRD §20 Phase 6).

  Periodically consolidates **loose, reachable** blobs whose files have
  not been modified for `:pack_cold_after` seconds into pack files via
  `Storage.Pack`. Unreachable blobs are left for `CasGC`. The policy is a
  global age threshold; per-bucket S3 lifecycle `Transition` rules are a
  recorded follow-up.

  Configuration (app env, overridable per `pack_now/1` call):
  - `:packer_interval` — sweep interval ms (default 6 h)
  - `:pack_cold_after` — seconds since last modification (default 30 days)
  - `:pack_min_blobs` — skip packing below this count (default 100)
  - `:pack_max_blobs` / `:pack_max_bytes` — per-pack limits (default 1000 / 1 GiB)
  """

  use GenServer

  require Logger

  alias ExStorageService.Storage.{CAS, CasGC, Pack}

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Run one packing pass now. See moduledoc for options."
  def pack_now(opts \\ []), do: GenServer.call(__MODULE__, {:pack_now, opts}, :infinity)

  @impl true
  def init(opts) do
    interval =
      Keyword.get(
        opts,
        :interval,
        Application.get_env(:ex_storage_service, :packer_interval, :timer.hours(6))
      )

    schedule(interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_info(:pack, state) do
    do_pack([])
    schedule(state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call({:pack_now, opts}, _from, state), do: {:reply, do_pack(opts), state}

  defp schedule(interval), do: Process.send_after(self(), :pack, interval)

  defp do_pack(opts) do
    cold_after = conf(opts, :cold_after, :pack_cold_after, 30 * 86_400)
    min_blobs = conf(opts, :min_blobs, :pack_min_blobs, 100)
    max_blobs = conf(opts, :max_blobs, :pack_max_blobs, 1000)
    max_bytes = conf(opts, :max_bytes, :pack_max_bytes, 1024 * 1024 * 1024)

    case Concord.get_all() do
      {:ok, all} ->
        reachable = CasGC.reachable_hashes(all)
        now = System.os_time(:second)

        candidates =
          loose_blobs()
          |> Enum.filter(fn {hash, path} ->
            MapSet.member?(reachable, hash) and cold?(path, now, cold_after)
          end)
          |> take_batch(max_blobs, max_bytes)

        if length(candidates) < min_blobs do
          {:ok, %{pack_hash: nil, packed: 0}}
        else
          Pack.pack_blobs(Enum.map(candidates, fn {hash, _path} -> hash end))
        end

      {:error, reason} ->
        Logger.warning("Packer: metadata scan failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp conf(opts, key, env_key, default) do
    Keyword.get(opts, key, Application.get_env(:ex_storage_service, env_key, default))
  end

  defp loose_blobs do
    objects_dir = Path.join([CAS.data_root(), CAS.reserved_root(), "objects", "sha256"])

    case File.ls(objects_dir) do
      {:ok, prefixes} ->
        Enum.flat_map(prefixes, fn prefix ->
          dir = Path.join(objects_dir, prefix)

          case File.ls(dir) do
            {:ok, files} -> Enum.map(files, &{prefix <> &1, Path.join(dir, &1)})
            {:error, _} -> []
          end
        end)

      {:error, _} ->
        []
    end
  end

  defp cold?(path, now, cold_after) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> now - mtime >= cold_after
      {:error, _} -> false
    end
  end

  defp take_batch(candidates, max_blobs, max_bytes) do
    candidates
    |> Enum.reduce_while({[], 0}, fn {hash, path} = entry, {acc, bytes} ->
      size =
        case File.stat(path) do
          {:ok, %File.Stat{size: s}} -> s
          _ -> 0
        end

      cond do
        length(acc) >= max_blobs -> {:halt, {acc, bytes}}
        bytes + size > max_bytes and acc != [] -> {:halt, {acc, bytes}}
        true -> {:cont, {[entry | acc], bytes + size}}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
    |> Enum.map(fn {hash, path} -> {hash, path} end)
  end
end
```

(Clean up the redundant final `Enum.map`; it is shown for clarity only — remove it so `--warnings-as-errors`/credo-style review stays clean.)

- [ ] **Step 4: Engine changes**

In `engine.ex`:

```elixir
  @doc """
  Resolve a blob to a servable location: a whole file (loose CAS or legacy
  layout) or a slice of a pack file. Serving code uses the offset/length
  forms of `send_file` for pack slices, preserving zero-copy and Range.
  """
  def get_object_location(bucket, content_hash) do
    legacy = legacy_content_path(CAS.data_root(), bucket, content_hash)

    cond do
      CAS.has_blob?(content_hash) ->
        {:ok, {:file, CAS.blob_path(content_hash)}}

      match?({:ok, _}, Pack.locate(content_hash)) ->
        {:ok, {path, offset, size}} = Pack.locate(content_hash)
        {:ok, {:pack, path, offset, size}}

      File.exists?(legacy) ->
        {:ok, {:file, legacy}}

      true ->
        {:error, :not_found}
    end
  end

  @doc "Read a blob's bytes regardless of physical location."
  def read_object(bucket, content_hash) do
    case get_object_location(bucket, content_hash) do
      {:ok, {:file, path}} -> File.read(path)
      {:ok, {:pack, _path, _offset, _size}} -> Pack.read(content_hash)
      {:error, reason} -> {:error, reason}
    end
  end
```

Add `alias ExStorageService.Storage.Pack` at the top. In `promote_to_global/2`, change the presence check from `if CAS.has_blob?(content_hash) do` to:

```elixir
    if CAS.has_blob?(content_hash) or match?({:ok, _}, Pack.locate(content_hash)) do
```

In `cas_gc.ex`, change `defp reachable_hashes(all)` to:

```elixir
  @doc false
  def reachable_hashes(all) do
```

In `application.ex`, add `ExStorageService.Storage.Packer,` after the `CasGC` child.

- [ ] **Step 5: Run tests** — packer_test 4 tests + full core suite green.

- [ ] **Step 6: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/packer.ex \
        apps/ex_storage_service/lib/ex_storage_service/storage/engine.ex \
        apps/ex_storage_service/lib/ex_storage_service/storage/cas_gc.ex \
        apps/ex_storage_service/lib/ex_storage_service/application.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/packer_test.exs
git commit -m "feat(core): age-based cold packing policy with packed-aware engine reads"
```

---

### Task 3: Packed-aware serving and reads in the S3 app

**Files:**
- Modify: `apps/ex_storage_service_s3/lib/ex_storage_service_s3/handlers/object/local_backend.ex` (`get_object/4` serves from locations)
- Modify: `apps/ex_storage_service_s3/lib/ex_storage_service_s3/handlers/object.ex` (`get_object_version/4` serves from locations; `read_uncached_source_object_data/3` uses `Engine.read_object/2`)
- Modify: `apps/ex_storage_service/lib/ex_storage_service/replication/worker.ex` (read via `Engine.read_object/2`)
- Test: Create `apps/ex_storage_service_s3/test/ex_storage_service_s3/packed_objects_test.exs`

**Interfaces:** consumes Task 2's `Engine.get_object_location/2` / `read_object/2`.

- [ ] **Step 1: Write the failing integration tests**

```elixir
# apps/ex_storage_service_s3/test/ex_storage_service_s3/packed_objects_test.exs
defmodule ExStorageServiceS3.PackedObjectsTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.{CAS, Packer}

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "packed-#{:erlang.unique_integer([:positive])}"

  defp create_bucket(bucket) do
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")
    bucket
  end

  # PUT an object, then force its blob into a pack.
  defp put_packed_object(bucket, key, data) do
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/#{key}", body: data)
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    File.touch!(CAS.blob_path(hash), System.os_time(:second) - 90 * 86_400)
    {:ok, %{packed: packed}} = Packer.pack_now(cold_after: 0, min_blobs: 1)
    assert packed >= 1
    refute File.exists?(CAS.blob_path(hash))
    hash
  end

  test "GET serves a packed object with correct body, etag, and content-length" do
    bucket = create_bucket(unique_bucket())
    data = "packed-serving-#{System.unique_integer()}-#{String.duplicate("x", 1000)}"
    put_packed_object(bucket, "cold.bin", data)

    {:ok, resp} = Req.get("#{@base_url}/#{bucket}/cold.bin")
    assert resp.status == 200
    assert resp.body == data
    assert Req.Response.get_header(resp, "content-length") == [to_string(byte_size(data))]
  end

  test "Range GET on a packed object returns the right slice" do
    bucket = create_bucket(unique_bucket())
    data = "0123456789abcdefghij-#{System.unique_integer()}"
    put_packed_object(bucket, "ranged.bin", data)

    {:ok, resp} = Req.get("#{@base_url}/#{bucket}/ranged.bin", headers: [{"range", "bytes=5-9"}])
    assert resp.status == 206
    assert resp.body == binary_part(data, 5, 5)
    assert Req.Response.get_header(resp, "content-range") == ["bytes 5-9/#{byte_size(data)}"]
  end

  test "two packed objects in one pack are served independently" do
    bucket = create_bucket(unique_bucket())
    d1 = "first-in-pack-#{System.unique_integer()}"
    d2 = "second-in-pack-#{System.unique_integer()}"
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/one.bin", body: d1)
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/two.bin", body: d2)

    for d <- [d1, d2] do
      hash = Base.encode16(:crypto.hash(:sha256, d), case: :lower)
      File.touch!(CAS.blob_path(hash), System.os_time(:second) - 90 * 86_400)
    end

    {:ok, %{packed: packed}} = Packer.pack_now(cold_after: 0, min_blobs: 2)
    assert packed >= 2

    {:ok, %{status: 200, body: b1}} = Req.get("#{@base_url}/#{bucket}/one.bin")
    {:ok, %{status: 200, body: b2}} = Req.get("#{@base_url}/#{bucket}/two.bin")
    assert b1 == d1 and b2 == d2
  end

  test "CopyObject from a packed source is metadata-only and readable" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())
    data = "copy-packed-#{System.unique_integer()}"
    put_packed_object(src, "orig.bin", data)

    {:ok, %{status: 200}} =
      Req.put("#{@base_url}/#{dst}/copy.bin",
        headers: [{"x-amz-copy-source", "/#{src}/orig.bin"}],
        body: ""
      )

    {:ok, %{status: 200, body: body}} = Req.get("#{@base_url}/#{dst}/copy.bin")
    assert body == data
  end

  test "versioned GET of a packed old version works" do
    bucket = create_bucket(unique_bucket())

    versioning_xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>
    """

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}?versioning", body: versioning_xml)

    data_v1 = "packed-v1-#{System.unique_integer()}"
    {:ok, r1} = Req.put("#{@base_url}/#{bucket}/doc.txt", body: data_v1)
    [v1] = Req.Response.get_header(r1, "x-amz-version-id")
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/doc.txt", body: "v2-current")

    hash_v1 = Base.encode16(:crypto.hash(:sha256, data_v1), case: :lower)
    File.touch!(CAS.blob_path(hash_v1), System.os_time(:second) - 90 * 86_400)
    {:ok, %{packed: p}} = Packer.pack_now(cold_after: 0, min_blobs: 1)
    assert p >= 1

    {:ok, resp} = Req.get("#{@base_url}/#{bucket}/doc.txt?versionId=#{v1}")
    assert resp.status == 200
    assert resp.body == data_v1
  end
end
```

- [ ] **Step 2: Run to verify failures** (GETs of packed objects currently 500 "Content file missing").

- [ ] **Step 3: Implement serving**

**(a)** `local_backend.ex` `get_object/4`: replace `case Engine.get_object(bucket, content_hash) do {:ok, file_path} -> ...` with `case Engine.get_object_location(bucket, content_hash) do {:ok, location} -> ...` and change only the three send sites (keep 304/Range/header logic identical):

- Range success:

```elixir
                      {:ok, offset, length} ->
                        content_range = "bytes #{offset}-#{offset + length - 1}/#{size}"
                        {send_path, base_offset} = location_file(location)

                        conn
                        |> ...same headers...
                        |> send_file(206, send_path, base_offset + offset, length)
```

- Full response:

```elixir
                  [] ->
                    conn
                    |> ...same headers...
                    |> send_object(location)
```

Add two private helpers at the bottom:

```elixir
  defp location_file({:file, path}), do: {path, 0}
  defp location_file({:pack, path, offset, _size}), do: {path, offset}

  defp send_object(conn, {:file, path}), do: send_file(conn, 200, path)

  defp send_object(conn, {:pack, path, offset, size}),
    do: send_file(conn, 200, path, offset, size)
```

(Adapt the pipe at the full-response site: `|> then(&send_object(&1, location))` or restructure to call `send_object/2` last.)

**(b)** `handlers/object.ex` `get_object_version/4`: same pattern — `Engine.get_object_location/2`, then `send_file(200, path)` for `{:file, path}` or `send_file(200, path, offset, size)` for `{:pack, path, offset, size}` (no Range handling exists there). Add local helpers or inline a case.

**(c)** `handlers/object.ex` `read_uncached_source_object_data/3`: replace the `Engine.get_object` + `File.read` pair with:

```elixir
    case Engine.read_object(source_bucket, content_hash) do
      {:ok, data} ->
        {:ok, data}

      {:error, _} ->
        case cloud_cache_config(source_bucket) do
          {:ok, src_config} -> CloudClient.get_object(src_config, source_key)
          :disabled -> {:error, :no_source}
        end
    end
```

**(d)** `replication/worker.ex`: replace the `Engine.get_object(bucket, content_hash)` → `push_object(... file_path ...)` flow with `Engine.read_object/2`: on `{:ok, body}` do the HEAD-skip check then PUT `body` directly (drop the `File.read!` in `push_object`, pass the binary); on `{:error, :not_found}` keep `handle_missing_content/3` unchanged.

- [ ] **Step 4: Run the new tests + all three suites**

```bash
mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/packed_objects_test.exs
mix test apps/ex_storage_service/test && mix test apps/ex_storage_service_s3/test && mix test apps/ex_storage_service_web/test
```

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service_s3/lib apps/ex_storage_service/lib/ex_storage_service/replication/worker.ex \
        apps/ex_storage_service_s3/test/ex_storage_service_s3/packed_objects_test.exs
git commit -m "feat(s3): serve packed blobs via zero-copy pack slices"
```

---

### Task 4: PRD sync + full verification

- [ ] **Step 1: PRD sync** (`docs/prd/git-style-data-model.md`): revision note 11d — uncompressed content-addressed packs preserve send_file/Range/Content-Length; index in Concord + `.idx` sidecar; age-based cold policy via `Storage.Packer` (per-bucket S3 `Transition` rules and repack/pack-GC of dead entries are follow-ups). Mark §20 Phase 6 bullets: pack writer ✅, pack index ✅, reading packed blobs ✅ (incl. Range/copy/replication/versioned reads), lifecycle transition → implemented as global age-based policy (S3 Transition rules follow-up), CAS identity preserved ✅.

- [ ] **Step 2: Full verification** — format, strict compile, three suites.

- [ ] **Step 3: Commit**

```bash
git add docs/prd/git-style-data-model.md
git commit -m "docs(prd): record phase 6 pack storage implementation status"
```
