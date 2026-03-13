defmodule ExStorageService.Metrics do
  @moduledoc """
  Prometheus-style metrics collection using ETS counters.

  Attaches to telemetry events and exposes metrics in Prometheus text format
  via the `format_metrics/0` function.
  """

  require Logger

  @counters_table :ex_storage_service_metrics_counters
  @histograms_table :ex_storage_service_metrics_histograms

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
  end

  defp attach_handlers do
    :telemetry.attach(
      "ex-storage-metrics-request-stop",
      [:ex_storage_service, :s3, :request, :stop],
      &__MODULE__.handle_request_stop/4,
      nil
    )

    :telemetry.attach(
      "ex-storage-metrics-request-exception",
      [:ex_storage_service, :s3, :request, :exception],
      &__MODULE__.handle_request_exception/4,
      nil
    )
  end

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

  defp increment_counter(name, labels) do
    key = {name, labels}

    try do
      :ets.update_counter(@counters_table, key, {2, 1})
    rescue
      ArgumentError ->
        :ets.insert_new(@counters_table, {key, 0})
        :ets.update_counter(@counters_table, key, {2, 1})
    catch
      :error, :badarg ->
        :ets.insert_new(@counters_table, {key, 0})

        try do
          :ets.update_counter(@counters_table, key, {2, 1})
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        end
    end
  end

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

    [
      "# ExStorageService Metrics\n",
      counters,
      histograms
    ]
    |> IO.iodata_to_binary()
  end

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
