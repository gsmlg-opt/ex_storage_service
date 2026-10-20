defmodule ESSResourceErrorTrace do
  @moduledoc false
  @name :ess_resource_error_trace
  @target {ExStorageServiceS3.Handlers.Shared, :storage_error_response, 4}

  # The VM emits only bounded atom classifications, never arguments, connections,
  # credentials, keys, bodies or arbitrary error terms.
  def start(path) do
    nil = Process.whereis(@name)
    parent = self()

    pid =
      spawn(fn ->
        true = Process.register(self(), @name)
        session = :trace.session_create(@name, self(), [])
        {:module, module} = Code.ensure_loaded(elem(@target, 0))
        true = module == elem(@target, 0)

        specs = [
          {[:_, :"$1", :_, :_], [{:is_atom, :"$1"}], [{:message, :"$1"}]},
          {[:_, {:"$1", :"$2"}, :_, :_], [{:is_atom, :"$1"}, {:is_atom, :"$2"}],
           [{:message, {{:"$1", :"$2"}}}]},
          {[:_, {:"$1", {:"$2", :"$3"}}, :_, :_],
           [{:is_atom, :"$1"}, {:is_atom, :"$2"}, {:is_atom, :"$3"}],
           [{:message, {{:"$1", :"$2", :"$3"}}}]},
          {[:_, :_, :_, :_], [], [{:message, {:const, :opaque_error}}]}
        ]

        1 = :trace.function(session, @target, specs, [:local])
        :trace.process(session, :all, true, [:call, :arity, :monotonic_timestamp])

        File.write!(
          path,
          JSON.encode!(%{coverage: "started", wall_ms: System.system_time(:millisecond)}) <> "\n",
          [:exclusive]
        )

        send(parent, {:trace_ready, self()})

        try do
          loop(path, 0)
        after
          :trace.session_destroy(session)
        end
      end)

    receive do
      {:trace_ready, ^pid} -> :ok
    after
      5_000 -> raise "error trace setup did not acknowledge"
    end
  end

  defp loop(path, count) do
    receive do
      {:trace_ts, _pid, :call, @target, classification, timestamp} ->
        atoms =
          if is_tuple(classification), do: Tuple.to_list(classification), else: [classification]

        true = length(atoms) in 1..3 and Enum.all?(atoms, &is_atom/1)

        row = %{
          ordinal: count + 1,
          wall_ms: System.system_time(:millisecond),
          native_timestamp: timestamp,
          classification: Enum.map(atoms, &Atom.to_string/1)
        }

        File.write!(path, JSON.encode!(row) <> "\n", [:append])

        if count == 63 do
          File.write!(path, JSON.encode!(%{coverage: "event_limit_reached"}) <> "\n", [:append])
        else
          loop(path, count + 1)
        end

      :stop ->
        File.write!(path, JSON.encode!(%{coverage: "stopped"}) <> "\n", [:append])
    after
      900_000 ->
        File.write!(path, JSON.encode!(%{coverage: "time_limit_reached"}) <> "\n", [:append])
    end
  end
end
