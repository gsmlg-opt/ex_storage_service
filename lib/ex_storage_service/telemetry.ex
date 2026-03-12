defmodule ExStorageService.Telemetry do
  @moduledoc """
  Telemetry event definitions and helpers for S3 operations.

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
      result = fun.()
      duration = System.monotonic_time() - start_time

      :telemetry.execute(
        [:ex_storage_service, :s3, :request, :stop],
        %{duration: duration},
        metadata
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
end
