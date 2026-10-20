defmodule RetainedCopyWAL do
  # Offline trusted-fixture decoder. Never opens the adapter, starts Concord,
  # replays commands, or emits arbitrary request, credential or metadata values.
  def decode(<<>>, sequence, records), do: {sequence, Enum.reverse(records)}
  def decode(<<"VSRW", version, length::64, rest::binary>>, sequence, records) do
    true = version in [1, 2] and length <= 256 * 1024 * 1024
    {checksum, body} =
      case {version, rest} do
        {1, <<crc::32, tail::binary>>} -> {crc, tail}
        {2, <<header_crc::32, crc::32, tail::binary>>} ->
          true = header_crc == :erlang.crc32(<<2, length::64>>)
          {crc, tail}
      end
    <<payload::binary-size(length), tail::binary>> = body
    true = checksum == :erlang.crc32(payload)
    {{next, operation}, used} = :erlang.binary_to_term(payload, [:used])
    true = used == length and next == sequence + 1
    decode(tail, next, [{next, operation} | records])
  end

  def shape(value, depth \\ 0)
  def shape(_value, depth) when depth > 5, do: "depth_limit"
  def shape(value, _depth) when is_atom(value), do: Atom.to_string(value)
  def shape(value, _depth) when is_integer(value), do: value
  def shape(value, depth) when is_tuple(value) and tuple_size(value) <= 8,
    do: value |> Tuple.to_list() |> Enum.map(&shape(&1, depth + 1))
  def shape(_value, _depth), do: "redacted"

  def command({:concord_command, timestamp, value}), do: {timestamp, value}
  def command({:concord_command, _version, timestamp, value}), do: {timestamp, value}
  def command(_value), do: {nil, nil}

  def intent_writes({:txn, spec}) do
    for {:put, "ess:v2:operation_intent:" <> _key, value, _opts} <- spec.success do
      state = Map.get(value, :state, Map.get(value, "state"))
      %{state: shape(state), target_content: Map.get(value, :hash, Map.get(value, "hash")) == "2c06ade942ee3f17a048dd1064b2fab046a4bb95386d8bb41b68dc6711ac2af3"}
    end
  end
  def intent_writes(_value), do: []

  def result({:error, reason}), do: %{kind: "error", reason: shape(reason)}
  def result({:ok, %{succeeded: succeeded}}), do: %{kind: "transaction", succeeded: succeeded}
  def result(%{succeeded: succeeded}), do: %{kind: "transaction", succeeded: succeeded}
  def result(_value), do: %{kind: "other"}

  def summarize(records, first_ms, last_ms) do
    Enum.reduce(records, %{entries: %{}, outcomes: [], unique: MapSet.new(), failures: [], target: []}, fn
      {_sequence, {:append, entries}}, acc ->
        %{acc | entries: Enum.reduce(entries, acc.entries, &Map.put(&2, &1.op_number, &1))}
      {_sequence, {:applied, number, clients}}, acc ->
        acc = Enum.reduce(clients, acc, fn {client, record}, acc ->
          identity = {client, record.request_number}
          if record.status == :applied and not MapSet.member?(acc.unique, identity) do
            outcome = result(record.result)
            %{acc | unique: MapSet.put(acc.unique, identity), outcomes: [outcome | acc.outcomes],
              failures: if(outcome.kind == "error", do: [%{applied_number: number, outcome: outcome} | acc.failures], else: acc.failures)}
          else
            acc
          end
        end)
        case Map.get(acc.entries, number) do
          nil -> acc
          entry ->
            {timestamp, cmd} = command(entry.operation)
            record = Map.get(clients, entry.client_id)
            if is_integer(timestamp) and timestamp >= first_ms and timestamp <= last_ms do
              true = record.request_number == entry.request_number and record.status == :applied
              outcome = result(record.result)
              row = %{op_number: number, timestamp_ms: timestamp, command: if(is_tuple(cmd), do: shape(elem(cmd, 0)), else: "other"), intent_writes: intent_writes(cmd), outcome: outcome}
              %{acc | target: [row | acc.target]}
            else
              acc
            end
        end
      _, acc -> acc
    end)
  end
end

repo = "/home/gao/Workspace/gsmlg-opt/ex_storage_service"
path = "/fast/build/ess-closeout-c5-nvme-resources/tmp/ess-hardening-resource-closeout002-c5-nvme-r2-resources/data/metadata/vsr/ess_gateway_311c3b40d5_127.0.0.1/replica.wal"
out = Path.join(repo, "docs/reports/s3-hardening/closeout-002/candidate-c5/nvme-resources-r2/copy-retained-wal.json")
false = File.exists?(out)
data = File.read!(path)
true = byte_size(data) < 4 * 1024 * 1024
{count, records} = RetainedCopyWAL.decode(data, 0, [])
group = repo |> Path.join("docs/reports/s3-hardening/closeout002-c5-nvme-r2-resources/results.json") |> File.read!() |> JSON.decode!() |> Map.fetch!("groups") |> List.last()
summary = RetainedCopyWAL.summarize(records, div(group["start_ns"], 1_000_000) - 1_000, div(group["end_ns"], 1_000_000) + 1_000)
hash = fn bytes -> :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower) end
result = %{schema: 1, acceptance: false, kind: "read-only-CRC-verified-retained-WAL-forensics", inspected_at: DateTime.utc_now() |> DateTime.to_iso8601(), wal_bytes: byte_size(data), wal_sha256: hash.(data), input_script_sha256: File.read!(__ENV__.file) |> hash.(), frames: count, unique_results: MapSet.size(summary.unique), outcome_counts: Enum.frequencies_by(summary.outcomes, & &1.kind), deterministic_errors: Enum.reverse(summary.failures), target_window: %{first_ms: div(group["start_ns"], 1_000_000) - 1_000, last_ms: div(group["end_ns"], 1_000_000) + 1_000, applied: Enum.reverse(summary.target)}, limits: "A missing persisted error cannot exclude pre-submit/query/client wait or filesystem failures. Intended transaction writes are only applied when the persisted outcome succeeded."}
File.write!(out, JSON.encode!(result) <> "\n")
IO.puts(JSON.encode!(%{frames: count, unique_results: result.unique_results, outcome_counts: result.outcome_counts, errors: result.deterministic_errors, target_window: result.target_window}))
