defmodule ExStorageService.Telemetry do
  @moduledoc """
  Telemetry event definitions and helpers for S3 and cluster operations.

  Events emitted:
    - [:ex_storage_service, :s3, :request, :start]
    - [:ex_storage_service, :s3, :request, :stop]
    - [:ex_storage_service, :s3, :request, :exception]
  """

  @doc """
  Execute a telemetry-instrumented S3 operation.

  Emits start/stop/exception events around the given function.
  """
  def span(operation, metadata, fun) do
    start_time = System.monotonic_time()
    metadata = Map.put(metadata, :operation, operation)

    :telemetry.execute(
      [:ex_storage_service, :s3, :request, :start],
      %{system_time: System.system_time()},
      metadata
    )

    try do
      result = transfer_span(request_kind(operation), fun)
      duration = System.monotonic_time() - start_time

      :telemetry.execute(
        [:ex_storage_service, :s3, :request, :stop],
        %{duration: duration},
        Map.put(metadata, :result, request_result(result))
      )

      result
    rescue
      e ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:ex_storage_service, :s3, :request, :exception],
          %{duration: duration},
          Map.merge(metadata, %{kind: :error, reason: e, stacktrace: __STACKTRACE__})
        )

        reraise e, __STACKTRACE__
    catch
      kind, reason ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:ex_storage_service, :s3, :request, :exception],
          %{duration: duration},
          Map.merge(metadata, %{kind: kind, reason: reason, stacktrace: __STACKTRACE__})
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  # The monitor owns the matched lifecycle events, including untrappable owner
  # exits. It neither owns nor deletes files. Each transfer has one monitor and
  # at most one completion message; chunk progress never queues here.
  @doc false
  def transfer_span(kind, fun) do
    owner = self()
    started = System.monotonic_time()
    token = make_ref()

    observer =
      spawn(fn ->
        monitor = Process.monitor(owner)
        transfer_event(:start, kind, started, :running)

        receive do
          {^token, result} ->
            Process.demonitor(monitor, [:flush])
            transfer_event(:stop, kind, started, result)
            send(owner, {token, :observed})

          {:DOWN, ^monitor, :process, ^owner, _reason} ->
            transfer_event(:stop, kind, started, :owner_exit)
        end
      end)

    try do
      result = fun.()
      finish_transfer(observer, token, request_result(result))
      result
    catch
      kind, reason ->
        finish_transfer(observer, token, :exception)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  @doc false
  def stage_span(path, fs, fun) do
    previous = Process.put({__MODULE__, :stage_path}, path)
    :telemetry.execute([:ex_storage_service, :storage, :stage, :start], %{}, %{path: path})

    try do
      transfer_span(:stage, fun)
    after
      observe_stage(path, fs)

      if previous,
        do: Process.put({__MODULE__, :stage_path}, previous),
        else: Process.delete({__MODULE__, :stage_path})
    end
  end

  @doc false
  def staged_bytes(bytes) do
    :telemetry.execute(
      [:ex_storage_service, :storage, :stage, :write],
      %{bytes: bytes},
      %{path: Process.get({__MODULE__, :stage_path})}
    )
  end

  @doc false
  def observe_stage(path, fs) do
    observation =
      case fs.stat(path) do
        {:ok, %{type: :regular, size: size}} -> %{bytes: size, present: true}
        {:error, :enoent} -> %{bytes: 0, present: false}
        _ -> %{complete: false}
      end

    :telemetry.execute([:ex_storage_service, :storage, :stage, :observe], observation, %{
      path: path
    })
  rescue
    # Injectable filesystem adapters may not implement stat. Instrumentation
    # must never turn a storage result into a new exception.
    _ ->
      :telemetry.execute([:ex_storage_service, :storage, :stage, :observe], %{complete: false}, %{
        path: path
      })
  end

  @doc false
  def upstream(operation, fun) do
    started = System.monotonic_time()
    result = transfer_span(:upstream, fun)

    :telemetry.execute(
      [:ex_storage_service, :gateway, :upstream, :stop],
      %{duration: System.monotonic_time() - started},
      %{operation: operation, result: upstream_result(result)}
    )

    result
  end

  @doc false
  def response_delivery(source, bytes, result) do
    :telemetry.execute([:ex_storage_service, :transfer, :delivery], %{bytes: bytes}, %{
      source: source,
      result: result
    })
  end

  @doc false
  def cache(operation, result) do
    :telemetry.execute([:ex_storage_service, :gateway, :cache, :stop], %{count: 1}, %{
      operation: operation,
      result: result
    })
  end

  @doc false
  def s3_error(code) do
    category =
      cond do
        code in ["BadDigest", "InvalidDigest", "XAmzContentSHA256Mismatch", "InvalidChecksum"] ->
          :integrity

        code in ["PreconditionFailed", "ConditionalRequestConflict"] ->
          :precondition

        code == "NotImplemented" ->
          :unsupported

        true ->
          :other
      end

    :telemetry.execute([:ex_storage_service, :s3, :error], %{count: 1}, %{category: category})
  end

  @doc false
  def job_transition(kind, execution, transition) do
    result =
      case transition do
        {:ok, _} -> :ok
        :ok -> :ok
        {:error, {:stale_lease, _}} -> :stale
        {:error, :stale_lease} -> :stale
        _ -> :error
      end

    :telemetry.execute([:ex_storage_service, :job, :transition], %{count: 1}, %{
      kind: kind,
      execution: execution,
      transition: result
    })
  end

  defp finish_transfer(observer, token, result) do
    send(observer, {token, result})

    receive do
      {^token, :observed} -> :ok
    after
      # Observability cannot hold a completed transfer indefinitely.
      1_000 -> :ok
    end
  end

  defp transfer_event(event, kind, started, result) do
    :telemetry.execute(
      [:ex_storage_service, :transfer, event],
      %{duration: System.monotonic_time() - started},
      %{kind: kind, result: result}
    )
  end

  defp request_kind(operation) when operation in [:get_object, :head_object], do: :s3_read
  defp request_kind(operation) when operation in [:put_object, :delete_object], do: :s3_write
  defp request_kind(_), do: :s3_other

  defp request_result(%{private: %{ess_transfer_result: :error}}), do: :error
  defp request_result(%{status: status}) when is_integer(status) and status >= 400, do: :error
  defp request_result({:error, _}), do: :error
  defp request_result({:error, _, _}), do: :error
  defp request_result(_), do: :ok

  defp upstream_result({:error, :not_found}), do: :not_found
  defp upstream_result({:error, {:http_error, status}}) when status in [401, 403], do: :denied
  defp upstream_result({:error, {:http_error, 404}}), do: :not_found
  defp upstream_result({:error, {:http_error, 412}}), do: :precondition

  defp upstream_result({:error, {:http_error, status}}) when status in [429, 500, 502, 503, 504],
    do: :unavailable

  defp upstream_result({:error, {:http_error, _}}), do: :http_error
  defp upstream_result({:error, :forbidden}), do: :denied
  defp upstream_result({:error, :bucket_not_found}), do: :not_found
  defp upstream_result({:error, :upstream_length_mismatch}), do: :integrity
  defp upstream_result({:error, _}), do: :transport_error
  defp upstream_result(_), do: :ok

  @doc false
  def uncertain_operation(source, outcome) do
    :telemetry.execute([:ex_storage_service, :operation, :uncertain], %{count: 1}, %{
      source: source,
      outcome: outcome
    })
  end

  @doc false
  def quorum_stop(duration, measurements, metadata) do
    :telemetry.execute(
      [:ex_storage_service, :cluster, :quorum, :stop],
      Map.put(measurements, :duration, duration),
      metadata
    )
  end

  @doc false
  def repair_backlog(measurements) do
    :telemetry.execute(
      [:ex_storage_service, :cluster, :repair, :backlog],
      measurements,
      %{}
    )
  end

  @doc false
  def lease_contention(kind, reason) do
    :telemetry.execute(
      [:ex_storage_service, :cluster, :lease, :contention],
      %{count: 1},
      %{kind: kind, reason: reason}
    )
  end

  @doc false
  def orphan_counts(measurements) do
    :telemetry.execute(
      [:ex_storage_service, :storage, :gc, :stop],
      measurements,
      %{}
    )
  end
end
