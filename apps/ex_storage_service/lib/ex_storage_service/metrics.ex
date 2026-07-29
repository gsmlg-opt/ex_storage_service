defmodule ExStorageService.Metrics do
  @moduledoc """
  Prometheus-style metrics collection using ETS counters.

  Attaches to telemetry events and exposes metrics in Prometheus text format
  via the `format_metrics/0` function.
  """

  require Logger

  @counters_table :ex_storage_service_metrics_counters
  @histograms_table :ex_storage_service_metrics_histograms
  @gauges_table :ex_storage_service_metrics_gauges
  @handler_ids [
    "ex-storage-metrics-request",
    "ex-storage-metrics-cluster"
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

    :telemetry.attach_many(
      "ex-storage-metrics-cluster",
      [
        [:ex_storage_service, :cluster, :quorum, :stop],
        [:ex_storage_service, :cluster, :blob_transport, :stop],
        [:ex_storage_service, :cluster, :blob_transport, :checksum_failure],
        [:ex_storage_service, :cluster, :scrub, :stop],
        [:ex_storage_service, :cluster, :repair, :backlog],
        [:ex_storage_service, :cluster, :lease, :contention],
        [:ex_storage_service, :storage, :gc, :stop]
      ],
      &__MODULE__.handle_cluster/4,
      nil
    )
  end

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
    operation = Map.get(metadata, :operation, "unknown")
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    increment_counter("s3_requests_total", %{operation: operation, status: "ok"})
    record_histogram("s3_request_duration_milliseconds", %{operation: operation}, duration_ms)

    if size = Map.get(metadata, :size) do
      record_histogram("s3_object_size_bytes", %{operation: operation}, size)
    end
  end

  @doc false
  def handle_request_exception(_event, measurements, metadata, _config) do
    operation = Map.get(metadata, :operation, "unknown")
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    increment_counter("s3_requests_total", %{operation: operation, status: "error"})
    record_histogram("s3_request_duration_milliseconds", %{operation: operation}, duration_ms)
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
      %{result: stable_value(Map.get(metadata, :result, :unknown))},
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
    direction = stable_value(Map.get(metadata, :direction, :unknown))
    operation = stable_value(Map.get(metadata, :operation, :unknown))

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
        [:ex_storage_service, :cluster, :lease, :contention],
        measurements,
        metadata,
        _config
      ) do
    increment_counter(
      "cluster_lease_contention_total",
      %{
        kind: stable_value(Map.get(metadata, :kind, :unknown)),
        reason: stable_value(Map.get(metadata, :reason, :unknown))
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
    Enum.each([@counters_table, @histograms_table, @gauges_table], &:ets.delete_all_objects/1)
    :ok
  end

  defp stable_value(value) when is_atom(value), do: Atom.to_string(value)
  defp stable_value(value) when is_integer(value), do: Integer.to_string(value)
  defp stable_value(value) when is_binary(value), do: value
  defp stable_value(_value), do: "other"

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
