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

      good = Bounds.measure(request.(:good), 1, options)

      %{
        reads: 1,
        native_exact_counts: %{client: 1, native: 1, dispatch: 1},
        forbidden_native_calls: 0
      } = good

      must_fail!(fn -> Bounds.check_sample!(%{good | reads: Bounds.limits(1).reads + 1}, 1) end)

      must_fail!(fn ->
        Bounds.check_sample!(%{good | output_bytes: Bounds.limits(1).output + 1}, 1)
      end)

      must_fail!(fn ->
        Bounds.check_sample!(%{good | peak_heap_bytes: Bounds.limits(1).heap + 1}, 1)
      end)

      # Worker API still says exact get_record; forbidden operation executes in native PID.
      must_fail!(fn -> Bounds.measure(request.(:bad_query), 1, options) end)
      must_fail!(fn -> Bounds.measure(request.(:hidden_scan), 1, options) end)
      # A failed request must not damage native PID or the next isolated session.
      %{reads: 1} = Bounds.measure(request.(:good), 1, options)
      sentinel!(sentinel, native)
      fixture!()

      IO.puts(
        "PASS A28 CPU: good sample, deliberate bounds, replica query/scan detection, failure cleanup, sentinel preservation, offline fixture"
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

  defp must_fail!(fun) do
    failed =
      try do
        fun.()
        false
      rescue
        error in RuntimeError -> String.starts_with?(Exception.message(error), "A28 ")
      end

    if not failed, do: raise("CPU probe did not reject bad case")
  end
end

ESSIndexAcceptance.Probe.run()
