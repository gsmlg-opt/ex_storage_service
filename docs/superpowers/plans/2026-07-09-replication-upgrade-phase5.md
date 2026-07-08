# Replication Upgrade (Git-Style Data Model Phase 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replication becomes version/content-oriented: jobs pin the exact version (version_id + content hash + etag + size) captured at enqueue time instead of replicating "whatever the key holds at run time"; the worker skips transfer when the destination already has identical content (HEAD etag/size match); superseded-and-collected versions are skipped as stale instead of erroring; PUT bodies stream instead of `File.read!`.

**Architecture (deviations to record in the PRD):** Replication targets are generic S3-compatible endpoints reached over the S3 API (bearer-token v1 auth), so "destination can skip blob transfer if it already has the content hash" is implemented as **key-level HEAD etag+size comparison** — there is no cross-node blob-hash endpoint. "Transfer manifests before refs" is N/A: manifests are internal bookkeeping; multipart objects replicate as their materialized whole blob. Delete-marker replication is already body-less (plain DELETE, 404-idempotent) — unchanged.

## Global Constraints

- Same as prior plans. Existing `job_queue_test.exs` must keep passing; old-format queued jobs (payload without `:object`) must still process (worker falls back to reading current metadata).
- `Worker.replicate_delete/3` is unchanged.
- The `Sync` module (bucket-scan reconciliation) is unchanged — its etag comparison already provides scan-level skip.

---

### Task 1: Version-pinned, skip-aware `Worker.replicate_put/4`

**Files:**
- Modify: `apps/ex_storage_service/lib/ex_storage_service/replication/worker.ex`
- Test: Create `apps/ex_storage_service_s3/test/ex_storage_service_s3/replication_worker_test.exs` (integration tests live in the S3 app because they use the live S3 API on `localhost:9001` as the replica endpoint; core-app tests do not boot the S3 server)

**Interfaces:**
- Produces: `Worker.replicate_put(bucket, key, %Replica{}, object_info \\ nil) :: :ok | {:error, term()}` where `object_info` is `%{version_id:, content_hash:, etag:, size:, content_type:}` (nil ⇒ read current metadata, legacy behavior). Consumed by Task 2's dispatcher.

- [ ] **Step 1: Write the failing tests**

```elixir
# apps/ex_storage_service_s3/test/ex_storage_service_s3/replication_worker_test.exs
defmodule ExStorageServiceS3.ReplicationWorkerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias ExStorageService.Replication.Config.Replica
  alias ExStorageService.Replication.Worker

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "repl-#{:erlang.unique_integer([:positive])}"

  defp create_bucket(bucket) do
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")
    bucket
  end

  defp replica_for(dest_bucket) do
    %Replica{endpoint: @base_url, access_key: nil, secret_key_enc: nil, bucket: dest_bucket}
  end

  defp object_info(bucket, key) do
    {:ok, meta} = ExStorageService.Metadata.get_object_meta(bucket, key)

    %{
      version_id: Map.get(meta, :version_id),
      content_hash: meta.content_hash,
      etag: meta.etag,
      size: meta.size,
      content_type: Map.get(meta, :content_type, "application/octet-stream")
    }
  end

  test "replicates the pinned version to the destination bucket" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())
    data = "replicate-me-#{System.unique_integer()}"

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/a.txt", body: data)

    assert :ok = Worker.replicate_put(src, "a.txt", replica_for(dst), object_info(src, "a.txt"))

    {:ok, %{status: 200, body: body}} = Req.get("#{@base_url}/#{dst}/a.txt")
    assert body == data
  end

  test "skips transfer when the destination already has identical content" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())
    data = "skip-me-#{System.unique_integer()}"

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/b.txt", body: data)
    info = object_info(src, "b.txt")

    assert :ok = Worker.replicate_put(src, "b.txt", replica_for(dst), info)

    log =
      capture_log(fn ->
        assert :ok = Worker.replicate_put(src, "b.txt", replica_for(dst), info)
      end)

    assert log =~ "already present"
  end

  test "skips as stale when the pinned version was superseded and its content is gone" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/c.txt", body: "current-content")

    stale_info = %{
      version_id: "ancient",
      content_hash: Base.encode16(:crypto.hash(:sha256, "collected-#{System.unique_integer()}"), case: :lower),
      etag: "deadbeef",
      size: 9,
      content_type: "text/plain"
    }

    log =
      capture_log(fn ->
        assert :ok = Worker.replicate_put(src, "c.txt", replica_for(dst), stale_info)
      end)

    assert log =~ "stale"
    # nothing was written to the destination
    {:ok, %{status: 404}} = Req.get("#{@base_url}/#{dst}/c.txt")
  end

  test "errors when pinned content is missing and the key still points at it" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/d.txt", body: "will-lose-content")
    info = object_info(src, "d.txt")

    # simulate content loss (e.g. manual deletion) while the ref still points at it
    File.rm!(ExStorageService.Storage.CAS.blob_path(info.content_hash))

    assert {:error, _} = Worker.replicate_put(src, "d.txt", replica_for(dst), info)
  end

  test "replicate_delete removes the destination object and is idempotent" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/e.txt", body: "bye")
    :ok = Worker.replicate_put(src, "e.txt", replica_for(dst), object_info(src, "e.txt"))

    assert :ok = Worker.replicate_delete(src, "e.txt", replica_for(dst))
    {:ok, %{status: 404}} = Req.get("#{@base_url}/#{dst}/e.txt")
    # 404 on repeat is success
    assert :ok = Worker.replicate_delete(src, "e.txt", replica_for(dst))
  end
end
```

- [ ] **Step 2: Run tests to verify the new behaviors fail**

Run: `mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/replication_worker_test.exs`
Expected: FAIL — `replicate_put/4` undefined; skip/stale logs absent.

- [ ] **Step 3: Implement**

Replace `replicate_put/3` in `worker.ex` with (keep `replicate_delete/3`, `build_url/3`, `auth_headers/1` as-is; update the moduledoc to describe version-pinned payloads and skip semantics):

```elixir
  @doc """
  Replicate a PUT to the replica.

  `object_info` pins the version captured when the job was enqueued
  (`%{version_id:, content_hash:, etag:, size:, content_type:}`). When
  `nil` (legacy jobs, Sync-enqueued jobs), the current object metadata is
  read instead.

  Skip semantics (idempotency):
  - destination already holds identical content (HEAD etag+size match) → `:ok`
  - pinned content is gone AND the key has moved on to a newer version → `:ok` (stale)
  """
  @spec replicate_put(String.t(), String.t(), Replica.t(), map() | nil) ::
          :ok | {:error, term()}
  def replicate_put(bucket, key, replica, object_info \\ nil)

  def replicate_put(bucket, key, %Replica{} = replica, nil) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        replicate_put(bucket, key, replica, %{
          version_id: get_field(meta, :version_id),
          content_hash: get_field(meta, :content_hash),
          etag: get_field(meta, :etag),
          size: get_field(meta, :size),
          content_type: get_field(meta, :content_type) || "application/octet-stream"
        })

      {:error, reason} ->
        Logger.error("Cannot replicate PUT #{bucket}/#{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def replicate_put(bucket, key, %Replica{} = replica, object_info) do
    content_hash = get_field(object_info, :content_hash)
    etag = get_field(object_info, :etag)
    size = get_field(object_info, :size)
    content_type = get_field(object_info, :content_type) || "application/octet-stream"

    remote_bucket = replica.bucket || bucket
    url = build_url(replica.endpoint, remote_bucket, key)
    headers = auth_headers(replica)

    case Engine.get_object(bucket, content_hash) do
      {:ok, file_path} ->
        if destination_has_content?(url, headers, etag, size) do
          Logger.debug("Replication PUT #{bucket}/#{key}: already present at #{replica.endpoint}")
          :ok
        else
          push_object(bucket, key, url, headers, file_path, content_type, replica)
        end

      {:error, :not_found} ->
        handle_missing_content(bucket, key, content_hash)
    end
  end

  ## Private

  defp push_object(bucket, key, url, headers, file_path, content_type, replica) do
    body = File.stream!(file_path, 262_144)

    case Req.put(url, body: body, headers: [{"content-type", content_type} | headers]) do
      {:ok, %{status: status}} when status in 200..299 ->
        Logger.debug("Replicated PUT #{bucket}/#{key} to #{replica.endpoint}")
        :ok

      {:ok, %{status: status, body: resp_body}} ->
        Logger.error(
          "Replication PUT failed for #{bucket}/#{key} to #{replica.endpoint}: HTTP #{status} - #{inspect(resp_body)}"
        )

        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.error(
          "Replication PUT failed for #{bucket}/#{key} to #{replica.endpoint}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  # The destination is generic S3, so content-hash skip is implemented as a
  # key-level HEAD etag+size comparison.
  defp destination_has_content?(url, headers, etag, size) when is_binary(etag) do
    case Req.head(url, headers: headers) do
      {:ok, %{status: 200} = resp} ->
        remote_etag =
          resp
          |> Req.Response.get_header("etag")
          |> List.first()
          |> case do
            nil -> nil
            quoted -> String.trim(quoted, "\"")
          end

        remote_size =
          resp
          |> Req.Response.get_header("content-length")
          |> List.first()
          |> case do
            nil -> nil
            len -> String.to_integer(len)
          end

        remote_etag == etag and remote_size == size

      _ ->
        false
    end
  end

  defp destination_has_content?(_url, _headers, _etag, _size), do: false

  # Pinned content is gone from local storage. If the key has moved on to a
  # newer version, the job is stale (a newer job will replicate the newer
  # version) — succeed without transfer. If the key still points at the
  # pinned content, that is genuine content loss — error for retry/repair.
  defp handle_missing_content(bucket, key, content_hash) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        if get_field(meta, :content_hash) == content_hash do
          Logger.error(
            "Replication PUT #{bucket}/#{key}: content #{inspect(content_hash)} missing from storage"
          )

          {:error, :content_missing}
        else
          Logger.info("Replication PUT #{bucket}/#{key}: pinned version is stale, skipping")
          :ok
        end

      {:error, :not_found} ->
        Logger.info("Replication PUT #{bucket}/#{key}: object gone, pinned version is stale, skipping")
        :ok
    end
  end

  defp get_field(map, key) when is_map(map), do: map[key] || map[to_string(key)]
```

Delete the old `replicate_put/3` body entirely (including its `File.read!` and meta re-read).

- [ ] **Step 4: Run tests**

Run: `mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/replication_worker_test.exs && mix test apps/ex_storage_service/test`
Expected: 5 new tests pass; core suite (incl. `job_queue_test.exs`) passes.

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/replication/worker.ex \
        apps/ex_storage_service_s3/test/ex_storage_service_s3/replication_worker_test.exs
git commit -m "feat(core): version-pinned replication with skip-if-present and streaming PUT"
```

---

### Task 2: Enqueue-time version pinning in Hooks + dispatcher threading

**Files:**
- Modify: `apps/ex_storage_service/lib/ex_storage_service/replication/hooks.ex` (`after_put/2` payload gains `:object`)
- Modify: `apps/ex_storage_service/lib/ex_storage_service/replication/job_queue.ex` (`default_processor/1` threads `payload[:object]` through)
- Test: extend `apps/ex_storage_service/test/ex_storage_service/replication/job_queue_test.exs` with one processor-dispatch test if a seam exists; otherwise assert payload shape via a new Hooks-level test in the same file's style (see Step 1)

**Interfaces:**
- `Hooks.after_put/2` and `after_delete/2` signatures unchanged (all S3/web callsites stay untouched).

- [ ] **Step 1: Write the failing test** (append to `job_queue_test.exs`, matching its setup conventions)

```elixir
    test "after_put enqueues jobs with a pinned object snapshot", context do
      _ = context
      bucket = "hooks-#{:erlang.unique_integer([:positive])}"
      key = "pinned.txt"

      now = DateTime.utc_now() |> DateTime.to_iso8601()

      ExStorageService.Metadata.put_object_meta(bucket, key, %{
        content_hash: "abc123",
        etag: "etag123",
        size: 7,
        content_type: "text/plain",
        version_id: "v-1",
        created_at: now,
        updated_at: now
      })

      :ok =
        ExStorageService.Replication.Config.add_bucket_replica(bucket, %{
          endpoint: "http://localhost:59999",
          access_key: nil,
          secret_key: nil,
          bucket: nil
        })

      :ok = ExStorageService.Replication.Hooks.after_put(bucket, key)

      {:ok, jobs} = ExStorageService.Replication.JobQueue.list_jobs(queue: :replication)

      assert Enum.any?(jobs, fn job ->
               job.payload[:bucket] == bucket and job.payload[:key] == key and
                 job.payload[:object][:content_hash] == "abc123" and
                 job.payload[:object][:version_id] == "v-1"
             end)
    end
```

**Note to implementer:** check `Replication.Config`'s actual API for adding a replica (`add_bucket_replica/2` shown here is a guess — read `config.ex` and use the real function/shape; if secrets are required, pass empty strings). Check `JobQueue` for a job-listing function (`list_jobs/1` is a guess — `job_queue_test.exs` already lists jobs somewhere; reuse whatever it uses, e.g. reading `Concord` keys directly). Adapt the assertions to those APIs; the assertion that matters is the `payload[:object]` snapshot content.

- [ ] **Step 2: Run to verify it fails** (payload has no `:object`)

- [ ] **Step 3: Implement**

In `hooks.ex` `after_put/2`, before the `Enum.each`, capture the snapshot once:

```elixir
        object_snapshot =
          case ExStorageService.Metadata.get_object_meta(bucket, key) do
            {:ok, meta} ->
              %{
                version_id: Map.get(meta, :version_id),
                content_hash: Map.get(meta, :content_hash),
                etag: Map.get(meta, :etag),
                size: Map.get(meta, :size),
                content_type: Map.get(meta, :content_type, "application/octet-stream")
              }

            {:error, _} ->
              nil
          end
```

and add `object: object_snapshot` to the enqueued payload map.

In `job_queue.ex` `default_processor/1`, change the `:put` clause to:

```elixir
      %{action: :put, bucket: bucket, key: key, replica: replica} = payload ->
        Worker.replicate_put(bucket, key, to_replica_struct(replica), payload[:object])
```

- [ ] **Step 4: Run core + S3 suites**

Run: `mix test apps/ex_storage_service/test && mix test apps/ex_storage_service_s3/test`

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/replication/hooks.ex \
        apps/ex_storage_service/lib/ex_storage_service/replication/job_queue.ex \
        apps/ex_storage_service/test/ex_storage_service/replication/job_queue_test.exs
git commit -m "feat(core): pin replicated version at enqueue time"
```

---

### Task 3: PRD sync + full verification

- [ ] **Step 1: PRD sync** (`docs/prd/git-style-data-model.md`) — revision note 11c: version-pinned replication payloads; key-level HEAD etag+size skip (generic-S3 targets, no cross-node blob endpoint); stale-version skip; streaming PUT bodies; delete/delete-marker replication already body-less; "transfer manifests before refs" N/A (whole-blob replication). Mark §20 Phase 5 bullets with statuses, marking manifest-transfer N/A and blob-hash skip as implemented-at-key-level.

- [ ] **Step 2: Full verification** — format, strict compile, three suites.

- [ ] **Step 3: Commit**

```bash
git add docs/prd/git-style-data-model.md
git commit -m "docs(prd): record phase 5 replication upgrade status"
```
