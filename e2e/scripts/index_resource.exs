Code.require_file("index_acceptance_bounds.exs", __DIR__)

# Actual native Concord exact-read instrumentation, after transactional publication.
defmodule ESSIndexAcceptance.Backend do
  alias ExStorageService.Metadata.Backend.Concord, as: Native

  def get(key, opts) do
    if Keyword.has_key?(opts, :revision), do: raise("historical read in ready path")
    result = Native.get(key, opts)
    ESSIndexAcceptance.Bounds.record_read(key, result)
  end
end

defmodule ESSIndexAcceptance do
  alias ExStorageService.Metadata
  alias ESSIndexAcceptance.Bounds
  alias ExStorageService.Metadata.{BucketLifecycle, ListIndex, ObjectCommit}
  alias ExStorageService.Storage.VersionListing

  def run do
    evidence = System.fetch_env!("ESS_INDEX_EVIDENCE")

    sizes =
      case System.get_env("ESS_INDEX_OPTIONAL_MILLION") do
        "true" -> [10_000, 100_000, 1_000_000]
        _ -> [10_000, 100_000]
      end

    {:ok, {hash, etag, size}} =
      ExStorageService.Storage.Engine.put_object("fixture", "shared", "shared-fixture")

    metadata = %{content_hash: hash, etag: etag, size: size, created_at: "2026-10-18T00:00:00Z"}

    rows =
      Enum.map(sizes, fn count ->
        bucket = "index-#{count}"
        unrelated = "unrelated-#{count}"
        :ok = Metadata.create_bucket(bucket)
        :ok = Metadata.create_bucket(unrelated)

        # TODO(upstream): gsmlg-dev/concord#99 bound full-state per-write work.
        # Keep the real ObjectCommit fixture and original workload/deadline here.
        {build_us, _} =
          :timer.tc(fn ->
            for number <- 1..count do
              key = "group/" <> String.pad_leading(Integer.to_string(number), 8, "0")
              {:ok, _} = ObjectCommit.put(bucket, key, metadata, bucket_versioning: true)
            end

            for key <- ["outside", "space plus+%/雪", "deep/a/b/c"] do
              {:ok, _} = ObjectCommit.put(bucket, key, metadata, bucket_versioning: true)
            end

            :ok = ExStorageService.Storage.Versioning.set_versioning(bucket, :enabled)

            for _ <- 1..3,
                do:
                  {:ok, _} =
                    ObjectCommit.put(bucket, "versions", metadata, bucket_versioning: true)

            {:ok, _} = ObjectCommit.delete_marker(bucket, "deleted", bucket_versioning: true)
          end)

        initial_inventory = offline_inventory(bucket)
        baseline = quiescent_measurements(bucket)

        for number <- 1..count do
          {:ok, _} =
            ObjectCommit.put(unrelated, Integer.to_string(number), metadata,
              bucket_versioning: true
            )
        end

        final_inventory = offline_inventory(bucket)
        expanded = quiescent_measurements(bucket)

        unless Enum.map(baseline, & &1.reads) == Enum.map(expanded, & &1.reads),
          do: raise("unrelated metadata changed exact listing work")

        %{
          keys: count,
          build_microseconds: build_us,
          before_unrelated: baseline,
          after_unrelated: expanded,
          before_inventory: initial_inventory,
          after_inventory: final_inventory,
          concord_resident_memory: :erlang.memory(:total)
        }
      end)

    File.write!(
      evidence,
      JSON.encode!(%{
        status: "passed",
        bounds: Bounds.declaration(),
        measurements: rows,
        scope: "standalone A28; A29 cluster/resume is a separate mandatory lane"
      })
    )
  end

  # Full inventory is deliberate fixture validation and size accounting. It ends
  # before the request closure is created; no snapshot is captured by a sample.
  defp offline_inventory(bucket) do
    {:ok, snapshot} = Concord.get_all(consistency: :linearizable)
    bucket_record = Map.fetch!(snapshot, "bucket:#{bucket}")
    generation = BucketLifecycle.generation(bucket_record)
    root = Map.fetch!(snapshot, ListIndex.root_key(bucket, generation))
    :ok = ListIndex.ready(%{value: root}, generation)

    trees =
      for {kind, field} <- [
            current: :current_root,
            versions: :versions_root,
            uploads: :uploads_root
          ],
          do: {ListIndex.namespace(bucket, root, kind), Map.fetch!(root, field)}

    fixture = Bounds.fixture!(snapshot, trees)
    encoded = ExStorageService.Metadata.Keys.encode_component(bucket)

    nodes =
      Enum.filter(snapshot, fn {key, _} ->
        String.starts_with?(key, "ess:v2:list_index:" <> encoded <> ":")
      end)

    %{
      fixture: fixture,
      index_records: length(nodes),
      index_serialized_bytes: :erlang.external_size(nodes),
      source_serialized_bytes: :erlang.external_size(snapshot)
    }
  end

  # This disposable benchmark owns its runtime and measures quiescent listing.
  # Acknowledgement waits for an active synchronous poll callback to finish.
  # Do not suspend the replica/engine or flush/rewrite poller messages/timers.
  defp quiescent_measurements(bucket) do
    poller = Process.whereis(Concord.Telemetry.Poller)
    true = is_pid(poller) and Process.alive?(poller)
    :ok = :sys.suspend(poller, 5_000)

    try do
      measurements(bucket)
    after
      # Resume failure must fail this lane, never be swallowed as cleanup.
      :ok = :sys.resume(poller, 5_000)
    end
  end

  defp measurements(bucket) do
    for maximum <- [0, 1, 10, 100, 1000],
        kind <- [:first, :middle, :last, :absent, :delimiter, :versions] do
      query =
        case kind do
          :first -> [max_keys: maximum]
          :middle -> [max_keys: maximum, continuation_token: "group/00005000"]
          :last -> [max_keys: maximum, continuation_token: "zzzz"]
          :absent -> [max_keys: maximum, prefix: "missing/"]
          :delimiter -> [max_keys: maximum, delimiter: "/"]
          :versions -> [max_keys: maximum, prefix: "versions"]
        end

      request = fn ->
        opts = Keyword.put(query, :backend, ESSIndexAcceptance.Backend)

        if kind == :versions,
          do: VersionListing.list(bucket, opts),
          else: Metadata.list_objects(bucket, opts)
      end

      {:ok, _} = request.()

      samples = for _ <- 1..30, do: Bounds.measure(request, maximum)

      times = Enum.sort(Enum.map(samples, & &1.duration))

      %{
        maximum: maximum,
        kind: kind,
        p50_us: Enum.at(times, 14),
        p95_us: Enum.at(times, 28),
        requests_per_second: 30_000_000 / max(Enum.sum(times), 1),
        limits: Bounds.limits(maximum),
        reads: Enum.max(Enum.map(samples, & &1.reads)),
        samples: samples
      }
    end
  end
end

# This disposable fixture starts core explicitly, so all background blob workers
# are disabled before supervision starts. Normal production configuration is untouched.
workers =
  Map.new(
    [
      :multipart_gc,
      :content_gc,
      :cas_gc,
      :packer,
      :lifecycle,
      :cross_cluster_replication,
      :repair,
      :scrub
    ],
    &{&1, false}
  )

configured = Application.get_env(:ex_storage_service, :instance_config, [])

Application.put_env(
  :ex_storage_service,
  :instance_config,
  Keyword.put(configured, :workers, workers)
)

{:ok, config} = ExStorageService.InstanceConfig.from_application_env()
true = Enum.all?(config.workers, fn {_worker, enabled} -> enabled == false end)
{:ok, _} = Application.ensure_all_started(:ex_storage_service)
ESSIndexAcceptance.run()
