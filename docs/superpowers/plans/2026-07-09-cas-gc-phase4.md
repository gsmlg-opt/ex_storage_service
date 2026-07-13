# Global CAS GC (Git-Style Data Model Phase 4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reclaim unreferenced global-CAS blobs (deleted objects, purged versions, completed-multipart part blobs) through a conservative candidate → quarantine → delete pipeline with dry-run, restore-on-rereference, and audit logging. The legacy `ContentGC` keeps owning the legacy bucket-local tree; the new GC owns `cas/objects` only.

**Architecture:** New `ExStorageService.Storage.CasGC` GenServer (periodic, like `ContentGC`). Each sweep: (1) build the reachable-hash set from GC roots — `obj:*` and `obj_ver:*` `content_hash` fields plus active `mpu_part:*` `hash` fields; (2) **restore** any quarantined blob whose hash became reachable again; (3) scan `cas/objects/sha256/*/*` — unreachable files older than an mtime grace become `gc:candidate:{hash}` records; (4) candidates past `eligible_after` and still unreachable move to `cas/gc/quarantine/sha256-{hash}` (blob meta → `:quarantined`); (5) quarantined entries past their second `eligible_after` and still unreachable are deleted (file + blob meta + candidate record). Every stage re-checks reachability. Manifest files/records are never swept (small, audit value). Reason codes: `:unreferenced`.

**Tech Stack:** Same as prior phases. Candidate records: `"gc:candidate:{hash}"` per PRD §7.8 with a `stage` field.

## Global Constraints

- Same as prior plans (no new deps, `--warnings-as-errors`, format, conventional commits).
- **GC must never delete a reachable blob** (PRD §21.5) — reachability is re-checked at candidate creation, quarantine transition, and final deletion.
- GC never runs in a request process; it is a supervised GenServer with `run_now/1` and `dry_run/0` for tests/operators.
- Grace periods are options on `run_now/1` (and app-env defaults) so tests can walk the full lifecycle deterministically: `orphan_mtime_grace` (default 600s), `candidate_grace` (default 3600s), `quarantine_grace` (default 86_400s).
- `ContentGC` and its tests are untouched.

---

### Task 1: `Storage.CasGC` module + supervision

**Files:**
- Create: `apps/ex_storage_service/lib/ex_storage_service/storage/cas_gc.ex`
- Modify: `apps/ex_storage_service/lib/ex_storage_service/application.ex` (add `ExStorageService.Storage.CasGC` child directly after `ExStorageService.Storage.ContentGC`)
- Test: Create `apps/ex_storage_service/test/ex_storage_service/storage/cas_gc_test.exs`

**Interfaces:**
- Consumes: `CAS.data_root/0`, `CAS.reserved_root/0`, `CAS.blob_path/1`; `Metadata.get_blob_meta/1`, `put_blob_meta/2`; `Concord.get_all/0`, `get/1`, `put/2`, `delete/1`.
- Produces:
  - `CasGC.run_now(opts \\ []) :: {:ok, report}` — one full sweep; opts: `:orphan_mtime_grace`, `:candidate_grace`, `:quarantine_grace` (seconds), `:dry_run` (boolean). `report :: %{candidates_created:, quarantined:, deleted:, restored:, reachable: non_neg_integer()}`.
  - `CasGC.dry_run() :: {:ok, report}` — alias for `run_now(dry_run: true)`.
  - `CasGC.stats() :: %{candidates: n, quarantined: n}` — operator visibility.

- [ ] **Step 1: Write the failing tests**

```elixir
# apps/ex_storage_service/test/ex_storage_service/storage/cas_gc_test.exs
defmodule ExStorageService.Storage.CasGCTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, CasGC}

  # Every stage immediate: orphan grace 0, candidate grace 0, quarantine grace 0.
  @instant [orphan_mtime_grace: 0, candidate_grace: 0, quarantine_grace: 0]

  defp seed_blob(data) do
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    path = CAS.blob_path(hash)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, data)
    # backdate so mtime-grace tests with nonzero grace also work
    File.touch!(path, System.os_time(:second) - 7200)
    Metadata.ensure_blob_meta(hash, byte_size(data))
    hash
  end

  defp quarantine_path(hash) do
    Path.join([CAS.data_root(), CAS.reserved_root(), "gc", "quarantine", "sha256-#{hash}"])
  end

  defp reference(hash) do
    bucket = "gcref-#{:erlang.unique_integer([:positive])}"
    key = "k"

    Metadata.put_object_meta(bucket, key, %{
      content_hash: hash,
      size: 1,
      etag: "e",
      created_at: DateTime.utc_now() |> DateTime.to_iso8601()
    })

    {bucket, key}
  end

  test "full lifecycle: candidate -> quarantine -> delete for unreferenced blobs" do
    hash = seed_blob("gc-lifecycle-#{System.unique_integer()}")

    # sweep 1: candidate created, file untouched
    {:ok, r1} = CasGC.run_now(@instant)
    assert r1.candidates_created >= 1
    assert File.exists?(CAS.blob_path(hash))
    assert {:ok, %{stage: :candidate}} = get_candidate(hash)

    # sweep 2: quarantined — file moved, blob meta updated
    {:ok, r2} = CasGC.run_now(@instant)
    assert r2.quarantined >= 1
    refute File.exists?(CAS.blob_path(hash))
    assert File.exists?(quarantine_path(hash))
    assert {:ok, %{state: :quarantined}} = Metadata.get_blob_meta(hash)

    # sweep 3: deleted — file, blob meta, candidate all gone
    {:ok, r3} = CasGC.run_now(@instant)
    assert r3.deleted >= 1
    refute File.exists?(quarantine_path(hash))
    assert {:error, :not_found} = Metadata.get_blob_meta(hash)
    assert {:error, :not_found} = get_candidate(hash)
  end

  test "referenced blobs are never selected" do
    hash = seed_blob("gc-referenced-#{System.unique_integer()}")
    reference(hash)

    {:ok, _} = CasGC.run_now(@instant)
    {:ok, _} = CasGC.run_now(@instant)
    {:ok, _} = CasGC.run_now(@instant)

    assert File.exists?(CAS.blob_path(hash))
    assert {:error, :not_found} = get_candidate(hash)
  end

  test "active multipart part blobs are rooted" do
    hash = seed_blob("gc-part-#{System.unique_integer()}")

    Concord.put("mpu_part:gcbucket:upload1:1", %{
      part_number: 1,
      hash: hash,
      size: 1,
      etag: "e",
      uploaded_at: DateTime.utc_now() |> DateTime.to_iso8601()
    })

    {:ok, _} = CasGC.run_now(@instant)
    assert File.exists?(CAS.blob_path(hash))
    assert {:error, :not_found} = get_candidate(hash)

    Concord.delete("mpu_part:gcbucket:upload1:1")
  end

  test "quarantined blob is restored when its hash becomes reachable again" do
    hash = seed_blob("gc-restore-#{System.unique_integer()}")

    {:ok, _} = CasGC.run_now(@instant)
    {:ok, _} = CasGC.run_now(@instant)
    assert File.exists?(quarantine_path(hash))

    # a new object now references the quarantined content
    reference(hash)

    {:ok, r} = CasGC.run_now(@instant)
    assert r.restored >= 1
    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(quarantine_path(hash))
    assert {:ok, %{state: :active}} = Metadata.get_blob_meta(hash)
    assert {:error, :not_found} = get_candidate(hash)
  end

  test "dry_run reports without modifying anything" do
    hash = seed_blob("gc-dry-#{System.unique_integer()}")

    {:ok, report} = CasGC.run_now(Keyword.put(@instant, :dry_run, true))
    assert report.candidates_created >= 1

    assert File.exists?(CAS.blob_path(hash))
    assert {:error, :not_found} = get_candidate(hash)
  end

  test "fresh unreferenced blobs are protected by the mtime grace" do
    data = "gc-fresh-#{System.unique_integer()}"
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    path = CAS.blob_path(hash)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, data)

    {:ok, _} = CasGC.run_now(orphan_mtime_grace: 600, candidate_grace: 0, quarantine_grace: 0)
    assert File.exists?(path)
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

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/cas_gc_test.exs`
Expected: FAIL — module not available

- [ ] **Step 3: Implement**

```elixir
# apps/ex_storage_service/lib/ex_storage_service/storage/cas_gc.ex
defmodule ExStorageService.Storage.CasGC do
  @moduledoc """
  Garbage collector for the global content-addressable store.

  Conservative candidate → quarantine → delete pipeline (PRD §14):

    1. Build the reachable-hash set from GC roots: `obj:*` / `obj_ver:*`
       object metadata (`content_hash`) and active multipart part records
       (`mpu_part:*` `hash`).
    2. Restore any quarantined blob whose hash became reachable again.
    3. Unreachable blob files older than the mtime grace become
       `gc:candidate:{hash}` records.
    4. Candidates past `eligible_after` and still unreachable move to
       `cas/gc/quarantine/sha256-{hash}` (blob metadata → `:quarantined`).
    5. Quarantined entries past their second `eligible_after` and still
       unreachable are deleted: file, blob metadata, candidate record.

  Reachability is re-checked at every stage, so a blob that regains a
  reference at any point is never deleted. The legacy `ContentGC` owns
  the legacy bucket-local tree; this module only touches `cas/objects`
  and `cas/gc/quarantine`.

  Configuration (app env, overridable per `run_now/1` call):
  - `:cas_gc_interval` — sweep interval ms (default 30 min)
  - `:cas_gc_orphan_mtime_grace` — seconds a file must be unmodified before
    it can become a candidate (default 600)
  - `:cas_gc_candidate_grace` — seconds before a candidate may be
    quarantined (default 3600)
  - `:cas_gc_quarantine_grace` — seconds before a quarantined blob may be
    deleted (default 86_400)
  """

  use GenServer

  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.CAS

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Run one sweep now. Options: :orphan_mtime_grace, :candidate_grace, :quarantine_grace, :dry_run."
  def run_now(opts \\ []) do
    GenServer.call(__MODULE__, {:run_now, opts}, :infinity)
  end

  @doc "Report what a sweep would do without modifying files or metadata."
  def dry_run, do: run_now(dry_run: true)

  @doc "Counts of current candidate and quarantined records."
  def stats do
    case Concord.get_all() do
      {:ok, all} ->
        candidates =
          all
          |> Enum.filter(fn {k, _} -> String.starts_with?(k, "gc:candidate:") end)
          |> Enum.map(fn {_, v} -> v end)

        %{
          candidates: Enum.count(candidates, &(&1.stage == :candidate)),
          quarantined: Enum.count(candidates, &(&1.stage == :quarantined))
        }

      _ ->
        %{candidates: 0, quarantined: 0}
    end
  end

  @impl true
  def init(opts) do
    interval =
      Keyword.get(
        opts,
        :interval,
        Application.get_env(:ex_storage_service, :cas_gc_interval, :timer.minutes(30))
      )

    schedule(interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_info(:sweep, state) do
    do_sweep([])
    schedule(state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call({:run_now, opts}, _from, state) do
    {:reply, do_sweep(opts), state}
  end

  defp schedule(interval), do: Process.send_after(self(), :sweep, interval)

  ## Sweep

  defp do_sweep(opts) do
    now = System.os_time(:second)
    dry_run? = Keyword.get(opts, :dry_run, false)

    orphan_grace = grace(opts, :orphan_mtime_grace, :cas_gc_orphan_mtime_grace, 600)
    candidate_grace = grace(opts, :candidate_grace, :cas_gc_candidate_grace, 3600)
    quarantine_grace = grace(opts, :quarantine_grace, :cas_gc_quarantine_grace, 86_400)

    case Concord.get_all() do
      {:ok, all} ->
        reachable = reachable_hashes(all)
        candidates = existing_candidates(all)

        report = %{
          reachable: MapSet.size(reachable),
          candidates_created: 0,
          quarantined: 0,
          deleted: 0,
          restored: 0
        }

        report = restore_rereferenced(candidates, reachable, dry_run?, report)
        report = advance_candidates(candidates, reachable, now, candidate_grace, quarantine_grace, dry_run?, report)
        report = create_candidates(candidates, reachable, now, orphan_grace, candidate_grace, dry_run?, report)

        if report.candidates_created + report.quarantined + report.deleted + report.restored > 0 do
          Logger.info("CasGC sweep#{if dry_run?, do: " (dry-run)", else: ""}: #{inspect(report)}")
        end

        {:ok, report}

      {:error, reason} ->
        Logger.warning("CasGC: metadata scan failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp grace(opts, key, env_key, default) do
    Keyword.get(opts, key, Application.get_env(:ex_storage_service, env_key, default))
  end

  ## Roots

  defp reachable_hashes(all) do
    all
    |> Enum.flat_map(fn
      {"obj:" <> _, %{content_hash: hash}} when is_binary(hash) -> [hash]
      {"obj_ver:" <> _, %{content_hash: hash}} when is_binary(hash) -> [hash]
      {"mpu_part:" <> _, %{hash: hash}} when is_binary(hash) -> [hash]
      _ -> []
    end)
    |> MapSet.new()
  end

  defp existing_candidates(all) do
    all
    |> Enum.flat_map(fn
      {"gc:candidate:" <> hash, record} -> [{hash, record}]
      _ -> []
    end)
    |> Map.new()
  end

  ## Stage: restore quarantined blobs that regained references

  defp restore_rereferenced(candidates, reachable, dry_run?, report) do
    candidates
    |> Enum.filter(fn {hash, record} ->
      record.stage == :quarantined and MapSet.member?(reachable, hash)
    end)
    |> Enum.reduce(report, fn {hash, _record}, acc ->
      unless dry_run? do
        qpath = quarantine_path(hash)
        dest = CAS.blob_path(hash)

        if File.exists?(qpath) do
          File.mkdir_p!(Path.dirname(dest))
          File.rename!(qpath, dest)
        end

        set_blob_state(hash, :active)
        Concord.delete(candidate_key(hash))
        Logger.info("CasGC: restored re-referenced blob #{hash}")
      end

      %{acc | restored: acc.restored + 1}
    end)
  end

  ## Stage: advance existing candidates (quarantine / delete / drop)

  defp advance_candidates(candidates, reachable, now, candidate_grace, quarantine_grace, dry_run?, report) do
    Enum.reduce(candidates, report, fn {hash, record}, acc ->
      cond do
        # regained a reference before quarantine — drop the candidate
        record.stage == :candidate and MapSet.member?(reachable, hash) ->
          unless dry_run?, do: Concord.delete(candidate_key(hash))
          acc

        record.stage == :candidate and now >= record.eligible_after ->
          quarantine(hash, now, quarantine_grace, dry_run?)
          %{acc | quarantined: acc.quarantined + 1}

        record.stage == :quarantined and now >= record.eligible_after and
            not MapSet.member?(reachable, hash) ->
          delete_quarantined(hash, dry_run?)
          %{acc | deleted: acc.deleted + 1}

        true ->
          acc
      end
    end)
  end

  defp quarantine(hash, now, quarantine_grace, dry_run?) do
    unless dry_run? do
      src = CAS.blob_path(hash)
      qpath = quarantine_path(hash)

      if File.exists?(src) do
        File.mkdir_p!(Path.dirname(qpath))
        File.rename!(src, qpath)
      end

      set_blob_state(hash, :quarantined)

      Concord.put(candidate_key(hash), %{
        hash: "sha256:#{hash}",
        reason: :unreferenced,
        stage: :quarantined,
        first_seen_at: now,
        eligible_after: now + quarantine_grace
      })

      Logger.info("CasGC: quarantined unreferenced blob #{hash}")
    end
  end

  defp delete_quarantined(hash, dry_run?) do
    unless dry_run? do
      File.rm(quarantine_path(hash))
      Concord.delete("blob:sha256:#{hash}")
      Concord.delete(candidate_key(hash))
      Logger.info("CasGC: deleted unreferenced blob #{hash}")
    end
  end

  ## Stage: create candidates for unreachable disk blobs

  defp create_candidates(candidates, reachable, now, orphan_grace, candidate_grace, dry_run?, report) do
    disk_blobs()
    |> Enum.reduce(report, fn {hash, path}, acc ->
      cond do
        MapSet.member?(reachable, hash) -> acc
        Map.has_key?(candidates, hash) -> acc
        not older_than?(path, now, orphan_grace) -> acc
        true ->
          unless dry_run? do
            Concord.put(candidate_key(hash), %{
              hash: "sha256:#{hash}",
              reason: :unreferenced,
              stage: :candidate,
              first_seen_at: now,
              eligible_after: now + candidate_grace
            })

            Logger.info("CasGC: marked candidate #{hash}")
          end

          %{acc | candidates_created: acc.candidates_created + 1}
      end
    end)
  end

  defp disk_blobs do
    objects_dir = Path.join([CAS.data_root(), CAS.reserved_root(), "objects", "sha256"])

    case File.ls(objects_dir) do
      {:ok, prefixes} ->
        Enum.flat_map(prefixes, fn prefix ->
          prefix_dir = Path.join(objects_dir, prefix)

          case File.ls(prefix_dir) do
            {:ok, files} ->
              Enum.map(files, fn file -> {prefix <> file, Path.join(prefix_dir, file)} end)

            {:error, _} ->
              []
          end
        end)

      {:error, _} ->
        []
    end
  end

  defp older_than?(path, now, grace) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> now - mtime >= grace
      {:error, _} -> true
    end
  end

  defp set_blob_state(hash, state) do
    case Metadata.get_blob_meta(hash) do
      {:ok, meta} -> Metadata.put_blob_meta(hash, Map.put(meta, :state, state))
      {:error, :not_found} -> :ok
    end
  end

  defp candidate_key(hash), do: "gc:candidate:#{hash}"

  defp quarantine_path(hash) do
    Path.join([CAS.data_root(), CAS.reserved_root(), "gc", "quarantine", "sha256-#{hash}"])
  end
end
```

In `application.ex`, add after the `ExStorageService.Storage.ContentGC` child:

```elixir
      ExStorageService.Storage.CasGC,
```

- [ ] **Step 4: Run tests**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/cas_gc_test.exs`
Expected: 6 tests, 0 failures

Then the full suites (GC runs on a 30-min timer, so it never fires during test runs, but other tests create unreferenced blobs — CasGC only mutates on `run_now`):
`mix test apps/ex_storage_service/test && mix test apps/ex_storage_service_s3/test && mix test apps/ex_storage_service_web/test`

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/cas_gc.ex \
        apps/ex_storage_service/lib/ex_storage_service/application.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/cas_gc_test.exs
git commit -m "feat(core): add global CAS garbage collector with quarantine pipeline"
```

---

### Task 2: PRD sync + full verification

**Files:**
- Modify: `docs/prd/git-style-data-model.md`

- [ ] **Step 1: PRD sync** — mark §20 Phase 4 bullets done (mark-and-sweep with candidate queue ✅, quarantine ✅, dry-run ✅, restore-on-rereference ✅, `stats/0` for operator visibility; admin *UI* deferred as follow-up; legacy ContentGC retires when the legacy layout is deleted — unchanged). Add revision note 11b summarizing the same.

- [ ] **Step 2: Full verification** — format, strict compile, three suites.

- [ ] **Step 3: Commit**

```bash
git add docs/prd/git-style-data-model.md
git commit -m "docs(prd): record phase 4 CAS GC implementation status"
```
