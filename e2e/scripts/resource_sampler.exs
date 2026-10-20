# Acceptance-only telemetry. Python owns scheduling, filesystem probes and
# a bounded serial export queue. BEAM probes preserve native observation windows.
root = System.fetch_env!("E2E_RESOURCE_ROOT")
python = System.fetch_env!("E2E_RESOURCE_PYTHON")
true = Path.type(python) == :absolute
observer = Path.expand("e2e/scripts/resource_observer.py")
beam_pid = System.pid()
tmp = System.fetch_env!("ESS_TMP_ROOT")
data = System.fetch_env!("ESS_DATA_ROOT")

:erlang.spawn_opt(
  fn ->
    Process.flag(:trap_exit, true)
    generation = Integer.to_string(System.system_time(:nanosecond))

    port =
      Port.open({:spawn_executable, python}, [
        :binary,
        :exit_status,
        :use_stdio,
        {:line, 4096},
        {:args,
         [
           "-B",
           observer,
           "sample",
           "--root",
           root,
           "--tmp",
           tmp,
           "--data",
           data,
           "--beam-pid",
           beam_pid,
           "--generation",
           generation
         ]}
      ])

    probe = fn fun ->
      start_ns = System.system_time(:nanosecond)
      start_monotonic_ns = System.monotonic_time(:nanosecond)
      value = fun.()

      {value,
       %{
         start_ns: start_ns,
         end_ns: System.system_time(:nanosecond),
         start_monotonic_ns: start_monotonic_ns,
         end_monotonic_ns: System.monotonic_time(:nanosecond)
       }}
    end

    loop = fn loop, sequence ->
      receive do
        {^port, {:data, {:eol, encoded}}} ->
          # Reject :noeol without concatenation: framing bounds allocation before
          # decoding. Exactly one generation/sequence request may be in flight.
          %{"generation" => ^generation, "sequence" => ^sequence} =
            request = JSON.decode!(encoded)

          true = map_size(request) == 2

          {queues, queues_window} =
            probe.(fn ->
              for pid <- Process.list(),
                  {:message_queue_len, n} <- [Process.info(pid, :message_queue_len)],
                  do: n
            end)

          {memory, memory_window} = probe.(fn -> :erlang.memory() end)

          reply =
            JSON.encode!(%{
              generation: generation,
              sequence: sequence,
              probes: %{
                binary: memory_window,
                total: memory_window,
                queue_max: queues_window,
                queue_total: queues_window
              },
              binary: memory[:binary],
              total: memory[:total],
              queue_max: Enum.max(queues, fn -> 0 end),
              queue_total: Enum.sum(queues)
            }) <> "\n"

          true = byte_size(reply) <= 4096
          true = Port.command(port, reply)
          loop.(loop, sequence + 1)

        {^port, {:exit_status, _status}} ->
          :ok

        {:EXIT, ^port, _reason} ->
          :ok

        {^port, {:data, {:noeol, _bounded_chunk}}} ->
          raise "oversized resource observer request"
      after
        10_000 -> raise "resource observer stopped requesting samples"
      end
    end

    try do
      loop.(loop, 0)
    after
      if Port.info(port), do: Port.close(port)
    end
  end,
  # Transient RPC evaluators return immediately. Detached ownership survives
  # their exit; owner/VM termination closes the pipes and stops the helper.
  [{:priority, :high}]
)
