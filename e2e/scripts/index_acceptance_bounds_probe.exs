# Plain CPU contract probe: elixir index_acceptance_bounds_probe.exs
# No Mix application, Concord runtime, network, filesystem data root or services.
Code.require_file("index_acceptance_bounds.exs", __DIR__)

defmodule ESSIndexAcceptance.ProbeNative do
  def loop do
    receive do
      {:query, from, reference, query, mode} ->
        result = query(query, %{}, mode)
        send(from, {reference, result})
        loop()

      :stop ->
        :ok
    end
  end

  def query(query, _state, mode) do
    if mode == :hidden_scan, do: scan()
    dispatch(query)
  end

  defp dispatch({:get_record, _}), do: {:ok, %{value: %{size: 1}}}
  defp dispatch(:get_all), do: {:ok, %{value: %{size: 1}}}
  def scan, do: :ok
end

defmodule ESSIndexAcceptance.ProbeClient do
  def query(query, opts) do
    reference = make_ref()
    native_query = if opts[:mode] == :bad_query, do: :get_all, else: query
    send(opts[:pid], {:query, self(), reference, native_query, opts[:mode]})

    receive do
      {^reference, result} -> result
    after
      1000 -> raise "probe native timeout"
    end
  end
end

defmodule ESSIndexAcceptance.Probe do
  alias ESSIndexAcceptance.{Bounds, ProbeClient, ProbeNative}

  def run do
    native = spawn(ProbeNative, :loop, [])
    monitor = Process.monitor(native)
    sentinel = :trace.session_create(:ess_index_sentinel, self(), [])

    try do
      1 =
        :trace.function(
          sentinel,
          {ProbeClient, :query, 2},
          [{:_, [], [{:message, {:const, :sentinel}}]}],
          [:local]
        )

      sentinel!(sentinel, native)

      specs = [
        {{ProbeClient, :query, 2}, :client, :query},
        {{ProbeNative, :query, 3}, :native, :query},
        {{ProbeNative, :dispatch, 1}, :dispatch, :query},
        {{ProbeNative, :scan, 0}, :scan, :forbidden}
      ]

      options = [native_pid: native, trace_specs: specs]

      request = fn mode ->
        fn ->
          result =
            ProbeClient.query({:get_record, "private-key-never-in-trace"},
              pid: native,
              mode: mode
            )

          Bounds.record_read("fixture-record", result)
          {:ok, %{keys: [{"one", %{size: 1}}], common_prefixes: [], index_stats: %{reads: 1}}}
        end
      end

      caller_isolation!(request, options)
      good = Bounds.measure(request.(:good), 1, options)

      %{
        reads: 1,
        native_exact_counts: %{client: 1, native: 1, dispatch: 1},
        forbidden_native_calls: 0
      } = good

      for {field, value, tag} <- [
            {:reads, Bounds.limits(1).reads + 1, :read_bound},
            {:bytes, Bounds.limits(1).bytes + 1, :returned_byte_bound},
            {:output_bytes, Bounds.limits(1).output + 1, :page_output_bound},
            {:output_heap_bytes, Bounds.limits(1).output_heap + 1, :page_output_heap_bound},
            {:output_entries, 2, :page_entry_bound},
            {:peak_heap_bytes, Bounds.limits(1).heap + 1, :peak_heap_bound},
            {:forbidden_native_calls, 1, :forbidden_native_boundary}
          ] do
        must_fail!(fn -> Bounds.check_sample!(Map.put(good, field, value), 1) end, tag)
      end

      collector_mailbox_bound!(request, options)

      # Worker API still says exact get_record; forbidden operation executes in native PID.
      must_fail!(fn -> Bounds.measure(request.(:bad_query), 1, options) end)
      must_fail!(fn -> Bounds.measure(request.(:hidden_scan), 1, options) end)
      # A failed request must not damage native PID or the next isolated session.
      %{reads: 1} = Bounds.measure(request.(:good), 1, options)
      sentinel!(sentinel, native)
      fixture!()

      IO.puts(
        "PASS A28 CPU: good sample, all sample bounds, private trace mailbox bound, caller heap/message isolation, replica query/scan detection, failure cleanup, sentinel preservation, offline fixture"
      )
    after
      :trace.session_destroy(sentinel)
      send(native, :stop)

      receive do
        {:DOWN, ^monitor, :process, ^native, :normal} -> :ok
      after
        1000 ->
          Process.exit(native, :kill)
          raise "probe native did not stop"
      end
    end
  end

  defp caller_isolation!(request, options) do
    token = make_ref()
    owner = self()
    history = for value <- 1..50_000, do: {value, value + 1, value + 2}
    Process.put(token, history)
    for value <- 1..600, do: send(owner, {token, :unrelated, value})
    caller_memory = Process.info(owner, [:memory, :message_queue_len])

    measured = fn mode ->
      fn ->
        {:monitored_by, [collector]} = Process.info(self(), :monitored_by)
        {:memory, memory} = Process.info(collector, :memory)
        send(owner, {token, :worker, self(), collector, memory})
        request.(mode).()
      end
    end

    try do
      %{reads: 1} = Bounds.measure(measured.(:good), 1, options)
      first_memory = stopped_worker!(token)
      must_fail!(fn -> Bounds.measure(measured.(:bad_query), 1, options) end)
      failed_memory = stopped_worker!(token)
      %{reads: 1} = Bounds.measure(measured.(:good), 1, options)
      last_memory = stopped_worker!(token)
      ^history = Process.get(token)

      IO.puts(
        "A28 caller isolation: " <>
          inspect(%{
            history_entries: length(history),
            caller_memory_bytes: caller_memory[:memory],
            unrelated_messages: caller_memory[:message_queue_len],
            collector_memory_bytes: [first_memory, failed_memory, last_memory]
          })
      )
    after
      Process.delete(token)

      for value <- 1..600 do
        receive do
          {^token, :unrelated, ^value} -> :ok
        after
          1000 -> raise "caller message lost or changed"
        end
      end
    end
  end

  defp stopped_worker!(token) do
    receive do
      {^token, :worker, pid, collector, memory} ->
        if Process.alive?(pid), do: raise("measurement worker survived completion")

        if collector == self() or Process.alive?(collector),
          do: raise("measurement collector did not stop in isolation")

        if memory >= 1_048_576, do: raise("collector inherited caller history heap")
        memory
    after
      1000 -> raise "measurement worker identity missing"
    end
  end

  defp collector_mailbox_bound!(request, options) do
    overloaded = fn ->
      {:monitored_by, [collector]} = Process.info(self(), :monitored_by)
      true = :erlang.suspend_process(collector)

      try do
        # 200 reads emit 600 real query trace events while delivery is paused.
        # The unchanged private mailbox limit must reject this backlog.
        for _ <- 1..200, do: request.(:good).()
      after
        true = :erlang.resume_process(collector)
      end

      {:ok, %{keys: [], common_prefixes: [], index_stats: %{reads: 200}}}
    end

    must_fail!(fn -> Bounds.measure(overloaded, 1000, options) end, :trace_mailbox_bound)
  end

  defp fixture! do
    id = <<0::128>>
    key = "n:" <> Base.url_encode64(id, padding: false)
    node = %{schema: 1, label: "one", terminal: %{metadata_key: "m"}, children: []}
    fixture = %{key => node, "m" => %{size: 1}}
    %{nodes: 1, terminals: 1, height: 1} = Bounds.fixture!(fixture, [{"n:", id}])
    bad = Map.put(fixture, key, %{node | label: String.duplicate("x", 161)})
    must_fail!(fn -> Bounds.fixture!(bad, [{"n:", id}]) end)
  end

  defp sentinel!(session, native) do
    {pid, monitor} =
      spawn_monitor(fn ->
        receive do
          :go -> ProbeClient.query({:get_record, "sentinel"}, pid: native)
        end
      end)

    1 = :trace.process(session, pid, true, [:call, :arity])
    send(pid, :go)

    receive do
      {:trace, ^pid, :call, {ProbeClient, :query, 2}, :sentinel} -> :ok
    after
      1000 -> raise "unrelated sentinel trace missing"
    end

    receive do
      {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
    after
      1000 -> raise "sentinel did not stop"
    end
  end

  defp must_fail!(fun, tag \\ nil) do
    failed =
      try do
        fun.()
        false
      rescue
        error in RuntimeError ->
          message = Exception.message(error)

          if tag,
            do: message == "A28 " <> Atom.to_string(tag),
            else: String.starts_with?(message, "A28 ")
      end

    if not failed, do: raise("CPU probe did not reject bad case")
  end
end

ESSIndexAcceptance.Probe.run()
