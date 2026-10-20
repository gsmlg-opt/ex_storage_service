# Application-free contract for the real bounded error-classification tracer.
defmodule ExStorageServiceS3.Handlers.Shared do
  def storage_error_response(_conn, reason, _resource, _id), do: reason
end

defmodule ESSResourceTraceSentinel do
  def call, do: :ok
end

Code.require_file("resource_error_trace.exs", __DIR__)
[directory] = System.argv()
path = Path.join(directory, "errors.jsonl")

sentinel = :trace.session_create(:resource_trace_sentinel, self(), [])
1 = :trace.function(sentinel, {ESSResourceTraceSentinel, :call, 0}, true, [:local])

sentinel_worker =
  spawn(fn ->
    loop = fn loop ->
      receive do
        :invoke ->
          :ok = ESSResourceTraceSentinel.call()
          loop.(loop)

        :stop ->
          :ok
      end
    end

    loop.(loop)
  end)

1 = :trace.process(sentinel, sentinel_worker, true, [:call, :arity])

check_sentinel = fn ->
  send(sentinel_worker, :invoke)

  receive do
    {:trace, _, :call, {ESSResourceTraceSentinel, :call, 0}} -> :ok
  after
    1_000 -> raise "independent trace session was disturbed"
  end
end

check_sentinel.()
:ok = ESSResourceErrorTrace.start(path)
check_sentinel.()

for reason <- [
      :timeout,
      {:metadata, :conflict},
      {:metadata, {:wrapped, :denied}},
      "sensitive-error-fixture",
      {:unexpected, %{credential: "sensitive-credential-fixture"}}
    ] do
  ExStorageServiceS3.Handlers.Shared.storage_error_response(
    :private,
    reason,
    "private-key",
    "private-id"
  )
end

await = fn await, predicate, attempts ->
  cond do
    predicate.() ->
      :ok

    attempts == 0 ->
      raise "bounded trace observation missing"

    true ->
      Process.sleep(10)
      await.(await, predicate, attempts - 1)
  end
end

read = fn -> File.read!(path) |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1) end
await.(await, fn -> Enum.count(read.(), &Map.has_key?(&1, "classification")) == 5 end, 100)
rows = Enum.filter(read.(), &Map.has_key?(&1, "classification"))

true =
  Enum.map(rows, & &1["classification"]) == [
    ["timeout"],
    ["metadata", "conflict"],
    ["metadata", "wrapped", "denied"],
    ["opaque_error"],
    ["opaque_error"]
  ]

false = String.contains?(File.read!(path), ["sensitive-", "private-key", "private-id"])
send(Process.whereis(:ess_resource_error_trace), :stop)
await.(await, fn -> Process.whereis(:ess_resource_error_trace) == nil end, 100)
check_sentinel.()
:trace.session_destroy(sentinel)
send(sentinel_worker, :stop)

cap_path = Path.join(directory, "cap.jsonl")
:ok = ESSResourceErrorTrace.start(cap_path)

for _ <- 1..80,
    do:
      ExStorageServiceS3.Handlers.Shared.storage_error_response(
        :private,
        :timeout,
        :private,
        :private
      )

await.(await, fn -> Process.whereis(:ess_resource_error_trace) == nil end, 100)
cap_rows = File.read!(cap_path) |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)
64 = Enum.count(cap_rows, &Map.has_key?(&1, "classification"))
true = Enum.any?(cap_rows, &(&1["coverage"] == "event_limit_reached"))
true = File.stat!(cap_path).size < 16_384

started = Enum.map(Application.started_applications(), &elem(&1, 0))
true = Enum.all?([:concord, :viewstamped_replication, :ex_storage_service], &(&1 not in started))

IO.puts(
  JSON.encode!(%{status: "passed", redaction_cases: 5, event_cap: 64, sentinel_preserved: true})
)
