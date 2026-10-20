defmodule ExStorageService.Metrics do
  @moduledoc """
  Prometheus-style metrics collection using ETS counters.

  Attaches to telemetry events and exposes metrics in Prometheus text format
  via the `format_metrics/0` function.
  """

  @counters_table :ex_storage_service_metrics_counters
  @histograms_table :ex_storage_service_metrics_histograms
  @gauges_table :ex_storage_service_metrics_gauges
  @stages_table :ex_storage_service_metrics_stages
  @stage_limit 1024
  @request_operations [:get_object, :head_object, :put_object, :delete_object]
  @handler_ids [
    "ex-storage-metrics-request",
    "ex-storage-metrics-cluster",
    "ex-storage-metrics-listing",
    "ex-storage-metrics-operations"
  ]

  @doc """
  Initialize metrics tables and attach telemetry handlers.
  Call this during application startup.
  """
  def setup do
    ensure_tables()
    attach_handlers()
  end

  defp ensure_tables do
    if :ets.info(@stages_table) == :undefined do
      :ets.new(@stages_table, [:named_table, :public, :set, write_concurrency: true])
      :ets.insert(@stages_table, [{:slots, 0}, {:complete, true}])
    end

    case :ets.info(@counters_table) do
      :undefined ->
        :ets.new(@counters_table, [
          :named_table,
          :public,
          :set,
          read_concurrency: true,
          write_concurrency: true
        ])

      _ ->
        :ok
    end

    case :ets.info(@histograms_table) do
      :undefined ->
        :ets.new(@histograms_table, [
          :named_table,
          :public,
          :ordered_set,
          read_concurrency: true,
          write_concurrency: true
        ])

      _ ->
        :ok
    end

    case :ets.info(@gauges_table) do
      :undefined ->
        :ets.new(@gauges_table, [
          :named_table,
          :public,
          :set,
          read_concurrency: true,
          write_concurrency: true
        ])

      _ ->
        :ok
    end
  end

  defp attach_handlers do
    Enum.each(@handler_ids, &:telemetry.detach/1)

    :ok =
      :telemetry.attach_many(
        "ex-storage-metrics-request",
        [
          [:ex_storage_service, :s3, :request, :stop],
          [:ex_storage_service, :s3, :request, :exception]
        ],
        &__MODULE__.handle_request/4,
        nil
      )

    :ok =
      :telemetry.attach_many(
        "ex-storage-metrics-cluster",
        [
          [:ex_storage_service, :cluster, :quorum, :stop],
          [:ex_storage_service, :cluster, :blob_transport, :stop],
          [:ex_storage_service, :cluster, :blob_transport, :checksum_failure],
          [:ex_storage_service, :cluster, :scrub, :stop],
          [:ex_storage_service, :cluster, :repair, :backlog],
          [:ex_storage_service, :replication, :backlog],
          [:ex_storage_service, :cluster, :lease, :contention],
          [:ex_storage_service, :storage, :gc, :stop]
        ],
        &__MODULE__.handle_cluster/4,
        nil
      )

    :ok =
      :telemetry.attach_many(
        "ex-storage-metrics-listing",
        [
          [:ex_storage_service, :listing, :page, :stop],
          [:ex_storage_service, :listing, :index, :status]
        ],
        &__MODULE__.handle_listing/4,
        nil
      )

    :telemetry.attach_many(
      "ex-storage-metrics-operations",
      [
        [:ex_storage_service, :transfer, :start],
        [:ex_storage_service, :transfer, :stop],
        [:ex_storage_service, :transfer, :delivery],
        [:ex_storage_service, :storage, :stage, :start],
        [:ex_storage_service, :storage, :stage, :write],
        [:ex_storage_service, :storage, :stage, :observe],
        [:ex_storage_service, :s3, :error],
        [:ex_storage_service, :gateway, :upstream, :stop],
        [:ex_storage_service, :gateway, :cache, :stop],
        [:ex_storage_service, :job, :transition],
        [:ex_storage_service, :capacity, :observe],
        [:ex_storage_service, :operation_status, :read],
        [:ex_storage_service, :operation, :uncertain]
      ],
      &__MODULE__.handle_operations/4,
      nil
    )
  end

  @doc false
  def handle_listing([:ex_storage_service, :listing, :page, :stop], measurements, metadata, _) do
    labels = %{
      kind: bounded_label(metadata[:kind], [:current, :versions, :uploads]),
      result: bounded_label(metadata[:result], [:ok, :error])
    }

    duration = System.convert_time_unit(measurements.duration, :native, :millisecond)
    increment_counter("listing_pages_total", labels)
    record_histogram("listing_duration_milliseconds", labels, duration)
  end

  def handle_listing([:ex_storage_service, :listing, :index, :status], _, metadata, _) do
    # Counts observations, not buckets. No per-bucket labels or stale global
    # readiness gauge can accidentally claim that every bucket is indexed.
    state = bounded_label(metadata[:state], [:ready, :migration_required, :building, :validating])
    increment_counter("listing_index_checks_total", %{state: state})
  end

  defp bounded_label(value, allowed),
    do: if(value in allowed, do: Atom.to_string(value), else: "other")

  @doc false
  def handle_request([:ex_storage_service, :s3, :request, :stop], measurements, metadata, config),
    do: handle_request_stop(nil, measurements, metadata, config)

  def handle_request(
        [:ex_storage_service, :s3, :request, :exception],
        measurements,
        metadata,
        config
      ),
      do: handle_request_exception(nil, measurements, metadata, config)

  @doc false
  def handle_request_stop(_event, measurements, metadata, _config) do
    operation = bounded_label(metadata[:operation], @request_operations)
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    result = if metadata[:result] == :error, do: "error", else: "ok"
    increment_counter("s3_requests_total", %{operation: operation, status: result})
    record_histogram("s3_request_duration_milliseconds", %{operation: operation}, duration_ms)

    if size = Map.get(metadata, :size) do
      record_histogram("s3_object_size_bytes", %{operation: operation}, size)
    end
  end

  @doc false
  def handle_request_exception(_event, measurements, metadata, _config) do
    operation = bounded_label(metadata[:operation], @request_operations)
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    increment_counter("s3_requests_total", %{operation: operation, status: "error"})
    record_histogram("s3_request_duration_milliseconds", %{operation: operation}, duration_ms)
  end

  @doc false
  def handle_operations([:ex_storage_service, :transfer, :start], _, metadata, _) do
    kind = bounded_label(metadata[:kind], [:stage, :upstream, :s3_read, :s3_write, :s3_other])
    adjust_gauge("transfer_active", %{kind: kind}, 1)
  end

  def handle_operations([:ex_storage_service, :transfer, :stop], measurements, metadata, _) do
    kind = bounded_label(metadata[:kind], [:stage, :upstream, :s3_read, :s3_write, :s3_other])
    result = bounded_label(metadata[:result], [:ok, :error, :exception, :owner_exit])
    adjust_gauge("transfer_active", %{kind: kind}, -1)
    increment_counter("transfers_total", %{kind: kind, result: result})

    record_histogram(
      "transfer_duration_milliseconds",
      %{kind: kind, result: result},
      System.convert_time_unit(measurements.duration, :native, :millisecond)
    )

    if kind == "stage" and result == "owner_exit", do: incomplete_stages()
  end

  def handle_operations([:ex_storage_service, :transfer, :delivery], measurements, metadata, _) do
    source = bounded_label(metadata[:source], [:file, :stream])
    increment_counter("s3_response_bytes_total", %{source: source}, max(measurements[:bytes], 0))

    if metadata[:result] != :ok do
      increment_counter("s3_response_delivery_errors_total", %{
        source: source,
        result: bounded_label(metadata[:result], [:closed, :error])
      })
    end
  end

  def handle_operations([:ex_storage_service, :storage, :stage, :start], _, metadata, _) do
    if is_binary(metadata[:path]) do
      slots = :ets.update_counter(@stages_table, :slots, {2, 1}, {:slots, 0})

      if slots <= @stage_limit do
        unless :ets.insert_new(@stages_table, {{:stage, metadata.path}, 0}),
          do: :ets.update_counter(@stages_table, :slots, {2, -1})
      else
        :ets.update_counter(@stages_table, :slots, {2, -1})
        incomplete_stages()
      end
    end
  end

  def handle_operations(
        [:ex_storage_service, :storage, :stage, :write],
        measurements,
        metadata,
        _
      ) do
    bytes = max(Map.get(measurements, :bytes, 0), 0)
    increment_counter("storage_staging_bytes_written_total", %{}, bytes)
    key = {:stage, metadata[:path]}

    try do
      :ets.update_counter(@stages_table, key, {2, bytes})
    rescue
      ArgumentError -> :ok
    end
  end

  def handle_operations(
        [:ex_storage_service, :storage, :stage, :observe],
        measurements,
        metadata,
        _
      ) do
    key = {:stage, metadata[:path]}

    cond do
      measurements[:complete] == false ->
        incomplete_stages()

      measurements[:present] == false ->
        if :ets.take(@stages_table, key) != [],
          do: :ets.update_counter(@stages_table, :slots, {2, -1})

      measurements[:present] == true ->
        # Never create an unbounded record from a standalone observation.
        :ets.update_element(@stages_table, key, {2, max(measurements[:bytes], 0)})

      true ->
        :ok
    end
  end

  def handle_operations([:ex_storage_service, :s3, :error], _, metadata, _) do
    category =
      bounded_label(metadata[:category], [:integrity, :precondition, :unsupported, :other])

    increment_counter("s3_errors_total", %{category: category})
  end

  def handle_operations(
        [:ex_storage_service, :gateway, :upstream, :stop],
        measurements,
        metadata,
        _
      ) do
    labels = %{
      operation: bounded_label(metadata[:operation], [:put, :get, :head, :delete, :list, :probe]),
      result:
        bounded_label(metadata[:result], [
          :ok,
          :not_found,
          :denied,
          :precondition,
          :unavailable,
          :http_error,
          :integrity,
          :transport_error
        ])
    }

    increment_counter("gateway_upstream_requests_total", labels)

    record_histogram(
      "gateway_upstream_duration_milliseconds",
      labels,
      System.convert_time_unit(measurements.duration, :native, :millisecond)
    )
  end

  def handle_operations([:ex_storage_service, :gateway, :cache, :stop], _, metadata, _) do
    increment_counter("gateway_cache_operations_total", %{
      operation: bounded_label(metadata[:operation], [:read, :publish]),
      result: bounded_label(metadata[:result], [:hit, :miss, :ok, :busy, :error])
    })
  end

  def handle_operations([:ex_storage_service, :job, :transition], _, metadata, _) do
    increment_counter("durable_job_transitions_total", %{
      kind:
        bounded_label(metadata[:kind], [
          :cross_cluster_put,
          :cross_cluster_delete,
          :repair_blob,
          :scrub,
          :cleanup
        ]),
      execution: bounded_label(metadata[:execution], [:ok, :error]),
      transition: bounded_label(metadata[:transition], [:ok, :error, :stale])
    })
  end

  def handle_operations([:ex_storage_service, :operation_status, :read], _, metadata, _) do
    increment_counter("operation_status_reads_total", %{
      source: bounded_label(metadata[:source], [:hosted, :gateway_write, :gateway_delete]),
      result: bounded_label(metadata[:result], [:ok, :error]),
      state:
        bounded_label(metadata[:state], [
          :pending,
          :unknown,
          :committed,
          :aborted,
          :prepared,
          :local_committed,
          :absent
        ])
    })
  end

  def handle_operations([:ex_storage_service, :operation, :uncertain], _, metadata, _) do
    increment_counter("uncertain_operation_outcomes_total", %{
      source: bounded_label(metadata[:source], [:hosted, :gateway_write, :gateway_delete]),
      outcome: bounded_label(metadata[:outcome], [:unknown, :reconciliation_required])
    })
  end

  def handle_operations([:ex_storage_service, :capacity, :observe], measurements, metadata, _) do
    labels = %{root: bounded_label(metadata[:root], [:blob, :tmp, :metadata])}
    complete = metadata[:complete] == true
    set_gauge("storage_capacity_observation_complete", labels, if(complete, do: 1, else: 0))

    if complete do
      set_gauge("storage_filesystem_total_bytes", labels, measurements.total_bytes)
      set_gauge("storage_filesystem_available_bytes", labels, measurements.available_bytes)
    end
  end

  defp adjust_gauge(name, labels, amount),
    do: :ets.update_counter(@gauges_table, {name, labels}, {2, amount}, {{name, labels}, 0})

  defp incomplete_stages, do: :ets.insert(@stages_table, {:complete, false})

  @doc """
  Returns a bounded observation of staging paths created since metrics setup.

  At most 1024 paths are retained. This is not a filesystem inventory: owner
  death or overflow marks coverage incomplete, retains known bytes, and never
  deletes a file. Startup leftovers and out-of-band file operations are outside
  this observation. No disk traversal is performed by metrics scrapes.
  """
  def staging_snapshot do
    if :ets.info(@stages_table) == :undefined do
      %{tracked_bytes: 0, tracked_files: 0, complete_since_setup: false, limit: @stage_limit}
    else
      {bytes, files} =
        :ets.foldl(
          fn
            {{:stage, _path}, size}, {bytes, files} -> {bytes + size, files + 1}
            _, acc -> acc
          end,
          {0, 0},
          @stages_table
        )

      complete = :ets.lookup(@stages_table, :complete) == [{:complete, true}]

      %{
        tracked_bytes: bytes,
        tracked_files: files,
        complete_since_setup: complete,
        limit: @stage_limit
      }
    end
  end

  @doc false
  def handle_cluster(
        [:ex_storage_service, :cluster, :quorum, :stop],
        measurements,
        metadata,
        _config
      ) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    record_histogram(
      "cluster_quorum_duration_milliseconds",
      %{result: bounded_label(metadata[:result], [:ok, :error, :degraded])},
      duration_ms
    )
  end

  def handle_cluster(
        [:ex_storage_service, :cluster, :blob_transport, :stop],
        measurements,
        metadata,
        _config
      ) do
    bytes = max(Map.get(measurements, :bytes, 0), 0)
    direction = bounded_label(metadata[:direction], [:client, :server])
    operation = bounded_label(metadata[:operation], [:put_blob, :open_blob])

    if operation == "put_blob" do
      increment_counter("cluster_replica_bytes_total", %{direction: direction}, bytes)
    end

    if operation == "open_blob" and direction == "client" do
      increment_counter("cluster_remote_read_bytes_total", %{}, bytes)
    end
  end

  def handle_cluster(
        [:ex_storage_service, :cluster, :blob_transport, :checksum_failure],
        measurements,
        _metadata,
        _config
      ) do
    increment_counter(
      "cluster_checksum_failures_total",
      %{},
      max(Map.get(measurements, :count, 1), 0)
    )
  end

  def handle_cluster(
        [:ex_storage_service, :cluster, :scrub, :stop],
        measurements,
        metadata,
        _config
      ) do
    if Map.get(metadata, :status) == :error and
         checksum_failure?(Map.get(metadata, :reason)) do
      increment_counter(
        "cluster_checksum_failures_total",
        %{},
        max(Map.get(measurements, :count, 1), 0)
      )
    end
  end

  def handle_cluster(
        [:ex_storage_service, :cluster, :repair, :backlog],
        measurements,
        _metadata,
        _config
      ) do
    Enum.each([:pending, :running, :failed, :under_replicated], fn state ->
      set_gauge(
        "cluster_repair_backlog",
        %{state: Atom.to_string(state)},
        max(Map.get(measurements, state, 0), 0)
      )
    end)
  end

  def handle_cluster(
        [:ex_storage_service, :replication, :backlog],
        measurements,
        metadata,
        _config
      ) do
    complete = Map.get(metadata, :complete) == true
    set_gauge("replication_backlog_scan_complete", %{}, if(complete, do: 1, else: 0))

    if complete do
      Enum.each([:pending, :running, :failed], fn state ->
        set_gauge(
          "replication_backlog",
          %{state: Atom.to_string(state)},
          max(Map.get(measurements, state, 0), 0)
        )
      end)

      set_gauge(
        "replication_oldest_job_age_seconds",
        %{},
        max(Map.get(measurements, :oldest_age_ms, 0), 0) / 1000
      )
    end
  end

  def handle_cluster(
        [:ex_storage_service, :cluster, :lease, :contention],
        measurements,
        metadata,
        _config
      ) do
    increment_counter(
      "cluster_lease_contention_total",
      %{
        kind:
          bounded_label(metadata[:kind], [
            :repair_blob,
            :scrub,
            :cleanup,
            :cross_cluster_put,
            :cross_cluster_delete
          ]),
        reason:
          bounded_label(metadata[:reason], [
            :owned,
            :stale,
            :stale_lease,
            :busy,
            :not_claimable,
            :leased_by_other_owner
          ])
      },
      max(Map.get(measurements, :count, 1), 0)
    )
  end

  def handle_cluster(
        [:ex_storage_service, :storage, :gc, :stop],
        measurements,
        _metadata,
        _config
      ) do
    Enum.each([:candidates, :quarantined, :orphans], fn state ->
      if Map.has_key?(measurements, state) do
        set_gauge(
          "storage_orphan_blobs",
          %{state: Atom.to_string(state)},
          max(Map.fetch!(measurements, state), 0)
        )
      end
    end)
  end

  defp increment_counter(name, labels, amount \\ 1) do
    key = {name, labels}

    try do
      :ets.update_counter(@counters_table, key, {2, amount})
    rescue
      ArgumentError ->
        :ets.insert_new(@counters_table, {key, 0})
        :ets.update_counter(@counters_table, key, {2, amount})
    catch
      :error, :badarg ->
        :ets.insert_new(@counters_table, {key, 0})

        try do
          :ets.update_counter(@counters_table, key, {2, amount})
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
    end
  end

  defp set_gauge(name, labels, value),
    do: :ets.insert(@gauges_table, {{name, labels}, value})

  @histogram_buckets [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
  @size_buckets [100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000]

  defp record_histogram(name, labels, value) do
    buckets = if String.contains?(name, "size"), do: @size_buckets, else: @histogram_buckets

    # Increment sum and count
    sum_key = {name, labels, :sum}
    count_key = {name, labels, :count}

    try do
      :ets.update_counter(@histograms_table, sum_key, {2, trunc(value)})
    rescue
      _ ->
        :ets.insert_new(@histograms_table, {sum_key, 0})

        try do
          :ets.update_counter(@histograms_table, sum_key, {2, trunc(value)})
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
    catch
      _, _ ->
        :ets.insert_new(@histograms_table, {sum_key, 0})

        try do
          :ets.update_counter(@histograms_table, sum_key, {2, trunc(value)})
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
    end

    try do
      :ets.update_counter(@histograms_table, count_key, {2, 1})
    rescue
      _ ->
        :ets.insert_new(@histograms_table, {count_key, 0})

        try do
          :ets.update_counter(@histograms_table, count_key, {2, 1})
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
    catch
      _, _ ->
        :ets.insert_new(@histograms_table, {count_key, 0})

        try do
          :ets.update_counter(@histograms_table, count_key, {2, 1})
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
    end

    # Increment bucket counters
    Enum.each(buckets, fn bucket ->
      if value <= bucket do
        bucket_key = {name, labels, {:le, bucket}}

        try do
          :ets.update_counter(@histograms_table, bucket_key, {2, 1})
        rescue
          _ ->
            :ets.insert_new(@histograms_table, {bucket_key, 0})

            try do
              :ets.update_counter(@histograms_table, bucket_key, {2, 1})
            rescue
              _ -> :ok
            catch
              _, _ -> :ok
            end
        catch
          _, _ ->
            :ets.insert_new(@histograms_table, {bucket_key, 0})

            try do
              :ets.update_counter(@histograms_table, bucket_key, {2, 1})
            rescue
              _ -> :ok
            catch
              _, _ -> :ok
            end
        end
      end
    end)

    # +Inf bucket
    inf_key = {name, labels, {:le, :inf}}

    try do
      :ets.update_counter(@histograms_table, inf_key, {2, 1})
    rescue
      _ ->
        :ets.insert_new(@histograms_table, {inf_key, 0})

        try do
          :ets.update_counter(@histograms_table, inf_key, {2, 1})
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
    catch
      _, _ ->
        :ets.insert_new(@histograms_table, {inf_key, 0})

        try do
          :ets.update_counter(@histograms_table, inf_key, {2, 1})
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
    end
  end

  @doc """
  Format all collected metrics in Prometheus text exposition format.
  """
  def format_metrics do
    if :ets.info(@gauges_table) != :undefined do
      stages = staging_snapshot()
      set_gauge("storage_staging_tracked_bytes", %{}, stages.tracked_bytes)
      set_gauge("storage_staging_tracked_files", %{}, stages.tracked_files)

      set_gauge(
        "storage_staging_tracking_complete_since_setup",
        %{},
        if(stages.complete_since_setup, do: 1, else: 0)
      )
    end

    counters = format_counters()
    histograms = format_histograms()
    gauges = format_gauges()

    [
      "# ExStorageService Metrics\n",
      counters,
      histograms,
      gauges
    ]
    |> IO.iodata_to_binary()
  end

  defp format_gauges do
    case :ets.info(@gauges_table) do
      :undefined ->
        ""

      _ ->
        @gauges_table
        |> :ets.tab2list()
        |> Enum.group_by(fn {{name, _labels}, _value} -> name end)
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.map(fn {name, items} ->
          [
            "# HELP #{name} Current storage cluster value\n",
            "# TYPE #{name} gauge\n",
            items
            |> Enum.sort_by(fn {{_name, labels}, _value} -> labels end)
            |> Enum.map(fn {{_name, labels}, value} ->
              "#{name}#{format_labels(labels)} #{value}\n"
            end)
          ]
        end)
    end
  end

  @doc false
  def reset do
    ensure_tables()

    Enum.each(
      [@counters_table, @histograms_table, @gauges_table, @stages_table],
      &:ets.delete_all_objects/1
    )

    :ets.insert(@stages_table, [{:slots, 0}, {:complete, true}])
    :ok
  end

  defp checksum_failure?(:checksum_mismatch), do: true
  defp checksum_failure?({:size_mismatch, _expected, _actual}), do: true
  defp checksum_failure?(_reason), do: false

  defp format_counters do
    case :ets.info(@counters_table) do
      :undefined ->
        ""

      _ ->
        entries = :ets.tab2list(@counters_table)

        if entries == [] do
          ""
        else
          # Group by metric name
          groups =
            entries
            |> Enum.group_by(fn {{name, _labels}, _val} -> name end)

          Enum.map(groups, fn {name, items} ->
            [
              "# HELP #{name} Total count of S3 requests\n",
              "# TYPE #{name} counter\n",
              Enum.map(items, fn {{_name, labels}, val} ->
                label_str = format_labels(labels)
                "#{name}#{label_str} #{val}\n"
              end)
            ]
          end)
        end
    end
  end

  defp format_histograms do
    case :ets.info(@histograms_table) do
      :undefined ->
        ""

      _ ->
        entries = :ets.tab2list(@histograms_table)

        if entries == [] do
          ""
        else
          # Group by metric name
          groups =
            entries
            |> Enum.group_by(fn {{name, _labels, _kind}, _val} -> name end)

          Enum.map(groups, fn {name, items} ->
            # Sub-group by labels
            by_labels =
              items
              |> Enum.group_by(fn {{_name, labels, _kind}, _val} -> labels end)

            [
              "# HELP #{name} Histogram of S3 operation measurements\n",
              "# TYPE #{name} histogram\n",
              Enum.map(by_labels, fn {labels, label_items} ->
                label_str = format_labels(labels)

                bucket_lines =
                  label_items
                  |> Enum.filter(fn {{_, _, kind}, _} -> match?({:le, _}, kind) end)
                  |> Enum.sort_by(fn
                    {{_, _, {:le, :inf}}, _} -> :infinity
                    {{_, _, {:le, v}}, _} -> v
                  end)
                  |> Enum.map(fn {{_, _, {:le, le}}, val} ->
                    le_str = if le == :inf, do: "+Inf", else: to_string(le)
                    base_labels = format_labels_raw(labels)

                    le_label =
                      if base_labels == "",
                        do: "le=\"#{le_str}\"",
                        else: "#{base_labels},le=\"#{le_str}\""

                    "#{name}_bucket{#{le_label}} #{val}\n"
                  end)

                sum_val =
                  Enum.find_value(label_items, 0, fn
                    {{_, _, :sum}, val} -> val
                    _ -> nil
                  end)

                count_val =
                  Enum.find_value(label_items, 0, fn
                    {{_, _, :count}, val} -> val
                    _ -> nil
                  end)

                [
                  bucket_lines,
                  "#{name}_sum#{label_str} #{sum_val}\n",
                  "#{name}_count#{label_str} #{count_val}\n"
                ]
              end)
            ]
          end)
        end
    end
  end

  defp format_labels(labels) when map_size(labels) == 0, do: ""

  defp format_labels(labels) do
    "{#{format_labels_raw(labels)}}"
  end

  defp format_labels_raw(labels) when map_size(labels) == 0, do: ""

  defp format_labels_raw(labels) do
    labels
    |> Enum.sort_by(fn {k, _} -> k end)
    |> Enum.map(fn {k, v} -> "#{k}=\"#{v}\"" end)
    |> Enum.join(",")
  end
end
