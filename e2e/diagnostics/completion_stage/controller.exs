[action, root] = System.argv()
true = action in ["start", "stop"]
true = Path.type(root) == :absolute and File.dir?(root)
nodes = [ess_data_a: :"ess_data_a@127.0.0.1", ess_data_b: :"ess_data_b@127.0.0.1"]
source = File.read!(Path.join(__DIR__, "trace_module.ex"))

results =
  Enum.map(nodes, fn {label, target} ->
    true = Node.connect(target)

    case action do
      "start" ->
        compiled = :rpc.call(target, Code, :compile_string, [source], 5_000)
        true = is_list(compiled)

        {:ok, _} =
          :rpc.call(
            target,
            ESSClusterStageDiagnosticTrace,
            :start,
            [Path.join(root, "#{label}.jsonl")],
            5_000
          )

        %{node: label, action: :start, acknowledged: true}

      "stop" ->
        result = :rpc.call(target, ESSClusterStageDiagnosticTrace, :stop, [], 10_000)
        true = result == :ok or match?({:ok, _}, result)
        %{node: label, action: :stop, acknowledged: true}
    end
  end)

path = Path.join(root, "#{action}-receipt.json")
{:ok, fd} = File.open(path, [:write, :exclusive, :binary])
:ok = File.chmod(path, 0o600)
:ok = IO.binwrite(fd, [JSON.encode_to_iodata!(%{results: results}), "\n"])
:ok = :file.sync(fd)
:ok = File.close(fd)
