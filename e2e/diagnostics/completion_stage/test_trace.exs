# Standalone fixture: run in an isolated Elixir VM, never with application code loaded.
Code.require_file("trace_module.ex", __DIR__)
ExUnit.start(autorun: false)
:debug = Logger.level()

{:logger_translator, {_filter, %{otp: true, sasl: true}}} =
  List.keyfind(:logger.get_primary_config().filters, :logger_translator, 0)

defmodule ExStorageService.Storage.Multipart do
  def prepare_complete_upload(_, _, _, _opts \\ []),
    do: {:error, :metadata_quorum_unavailable}
end

defmodule ExStorageService.ObjectService do
  def commit_existing_blob(_, _, _, _, _), do: {:error, :compare_retry_exhausted}
end

defmodule ExStorageService.Metadata.OperationIntents do
  def open(_, _, _, _, _, _), do: {:error, :operation_intent_compare_failed}
end

defmodule ExStorageService.Cluster.WriteCoordinator do
  def ensure_blob(_, _, _), do: {:error, :blob_write_quorum_unavailable}
end

defmodule ExStorageService.Metadata.ObjectCommit do
  def put(_, _, _, _), do: {:error, :precondition_failed}
end

defmodule ExStorageService.Metadata.Backend.Concord do
  def transaction(_, _),
    do: {:ok, %{succeeded: false, value: "private-output-marker", other: :private_atom_marker}}
end

defmodule ExStorageServiceS3.Handlers.Shared do
  def storage_error_response(conn, _reason, _resource, _request_id), do: conn
end

defmodule ESSIndependentTraceFixture do
  def ping, do: :ok

  def loop do
    receive do
      :ping ->
        ping()
        loop()

      :stop ->
        :ok
    end
  end
end

root =
  Path.join(
    System.tmp_dir!(),
    "ess-completion-stage-#{System.pid()}-#{System.system_time(:nanosecond)}"
  )

:ok = File.mkdir(root)
:ok = File.chmod(root, 0o700)
path = Path.join(root, "trace.jsonl")
independent = :trace.session_create(:completion_fixture_independent, self(), [])
{independent_worker, worker_monitor} = spawn_monitor(ESSIndependentTraceFixture, :loop, [])

try do
  1 = :trace.function(independent, {ESSIndependentTraceFixture, :ping, 0}, [], [:local])
  1 = :trace.process(independent, independent_worker, true, [:call, :arity])
  send(independent_worker, :ping)

  receive do
    {:trace, ^independent_worker, :call, {ESSIndependentTraceFixture, :ping, 0}} -> :ok
  after
    1_000 -> raise "independent fixture trace did not start"
  end

  {:ok, pid} = ESSClusterStageDiagnosticTrace.start(path)
  monitor = Process.monitor(pid)
  marker = "private-output-marker"

  redacted_status =
    ESSClusterStageDiagnosticTrace.format_status(%{
      state: %{secret: marker},
      message: {:trace_ts, marker},
      reason: marker,
      log: [marker]
    })

  false = inspect(redacted_status) =~ marker
  [] = redacted_status.log
  ExStorageService.Storage.Multipart.prepare_complete_upload(marker, marker, marker)
  ExStorageService.ObjectService.commit_existing_blob(marker, marker, marker, marker, marker)
  ExStorageService.Metadata.OperationIntents.open(marker, marker, marker, marker, marker, marker)
  ExStorageService.Cluster.WriteCoordinator.ensure_blob(marker, marker, marker)
  ExStorageService.Metadata.ObjectCommit.put(marker, marker, marker, marker)
  ExStorageService.Metadata.Backend.Concord.transaction(marker, marker)

  ExStorageServiceS3.Handlers.Shared.storage_error_response(
    %{conn: marker},
    {:error, {:timeout, {:private_atom_marker, marker}}},
    marker,
    marker
  )

  {:ok, receipt} = ESSClusterStageDiagnosticTrace.stop()

  receive do
    {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
  after
    1_000 -> raise "owned diagnostic tracer did not stop"
  end

  true =
    receipt.stopped and receipt.session_destroyed and receipt.delivery_drained and
      receipt.output_closed

  7 = receipt.output_events
  0 = receipt.unmatched_returns
  text = File.read!(path)
  false = String.contains?(text, marker)
  false = String.contains?(text, "private_atom_marker")
  rows = text |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)
  7 = length(rows)

  [
    "commit_existing",
    "ensure_blob",
    "intent_open",
    "object_commit",
    "prepare",
    "storage_error",
    "transaction"
  ] = rows |> Enum.map(& &1["stage"]) |> Enum.sort()

  true =
    Enum.all?(
      rows,
      &(Map.keys(&1) |> Enum.sort() ==
          ["duration_us", "monotonic_us", "process", "result", "stage"])
    )

  true = Enum.all?(rows, &(is_integer(&1["process"]) and &1["process"] > 0))
  [transaction] = Enum.filter(rows, &(&1["stage"] == "transaction"))
  %{"class" => "ok", "compare_succeeded" => false} = transaction["result"]

  expected_errors = %{
    "prepare" => "metadata_quorum_unavailable",
    "commit_existing" => "compare_retry_exhausted",
    "intent_open" => "operation_intent_compare_failed",
    "ensure_blob" => "blob_write_quorum_unavailable",
    "object_commit" => "precondition_failed"
  }

  Enum.each(expected_errors, fn {stage, reason} ->
    [row] = Enum.filter(rows, &(&1["stage"] == stage))
    %{"class" => "error", "reason" => ^reason} = row["result"]
  end)

  [error] = Enum.filter(rows, &(&1["stage"] == "storage_error"))

  %{"class" => "error", "reason" => %{"tuple" => ["error", %{"tuple" => ["timeout", "other"]}]}} =
    error["result"]

  true =
    Enum.all?(
      Enum.reject(rows, &(&1["stage"] == "storage_error")),
      &(is_integer(&1["duration_us"]) and &1["duration_us"] >= 0)
    )

  true = JSON.decode!(File.read!(path <> ".stopped.json"))["output_closed"]
  0o600 = Bitwise.band(File.stat!(path).mode, 0o777)
  0o600 = Bitwise.band(File.stat!(path <> ".stopped.json").mode, 0o777)
  :ok = ESSClusterStageDiagnosticTrace.stop()

  Enum.each([:control, :protected], fn mode ->
    captured =
      ExUnit.CaptureLog.capture_log([level: :debug], fn ->
        :logger.warning("diagnostic-parent-before")
        {:ok, crashed} = ESSClusterStageDiagnosticTrace.start(Path.join(root, "#{mode}.jsonl"))
        crash_monitor = Process.monitor(crashed)

        if mode == :control do
          :sys.replace_state(crashed, fn state ->
            Logger.delete_process_level(self())
            state
          end)
        end

        :ok = File.close(:sys.get_state(crashed).fd)
        :ok = :sys.suspend(crashed)

        send(
          crashed,
          {:trace_ts, self(), :call,
           {ExStorageServiceS3.Handlers.Shared, :storage_error_response, 4}, marker,
           System.monotonic_time()}
        )

        send(crashed, {:queued_private_return, marker})
        :ok = :sys.resume(crashed)

        receive do
          {:DOWN, ^crash_monitor, :process, ^crashed, reason} when reason != :normal -> :ok
        after
          1_000 -> raise "diagnostic crash fixture did not stop"
        end

        :logger.warning("diagnostic-parent-after")
        Logger.flush()
      end)

    expected_private_output = mode == :control
    ^expected_private_output = String.contains?(captured, marker)
    true = String.contains?(captured, "diagnostic-parent-before")
    true = String.contains?(captured, "diagnostic-parent-after")
  end)

  send(independent_worker, :ping)

  receive do
    {:trace, ^independent_worker, :call, {ESSIndependentTraceFixture, :ping, 0}} -> :ok
  after
    1_000 -> raise "diagnostic removed independent tracing"
  end

  IO.puts(
    JSON.encode!(%{
      fixture_events: 7,
      stopped: true,
      sensitive_output: false,
      crash_payload_suppressed: true,
      compare_false_preserved: true,
      independent_session_preserved: true
    })
  )
after
  ESSClusterStageDiagnosticTrace.stop()
  :trace.session_destroy(independent)
  send(independent_worker, :stop)

  receive do
    {:DOWN, ^worker_monitor, :process, ^independent_worker, :normal} -> :ok
  after
    1_000 -> raise "independent fixture worker did not stop"
  end

  nil = Process.whereis(ESSClusterStageDiagnosticTrace)
  File.rm_rf!(root)
end
