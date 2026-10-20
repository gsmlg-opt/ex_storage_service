# Application-free diagnostic only. The production acceptance collector is evaluated unchanged.
root = System.fetch_env!("E2E_RESOURCE_ROOT")
count = String.to_integer(System.fetch_env!("DIAGNOSTIC_BLOCKERS"))
parent = self()
emit = fn value -> IO.puts(JSON.encode!(value)) end
{collector, _} = Code.eval_file("e2e/scripts/resource_sampler.exs")
Process.sleep(250)

readers =
  for index <- if(count == 0, do: [], else: 1..count) do
    spawn(fn ->
      {:ok, file} = :file.open(Path.join(root, "fifo-#{index}"), [:read, :raw, :binary])
      send(parent, {:reader_started, index})

      receive do
        :go -> :ok
      after
        5_000 -> raise "FIFO reader start timeout"
      end

      start_ns = System.system_time(:nanosecond)
      start_mono = System.monotonic_time(:nanosecond)
      result = :file.read(file, 1)
      end_mono = System.monotonic_time(:nanosecond)
      end_ns = System.system_time(:nanosecond)
      :ok = :file.close(file)

      send(
        parent,
        {:reader_done,
         %{
           index: index,
           start_ns: start_ns,
           end_ns: end_ns,
           duration_ns: end_mono - start_mono,
           result: inspect(result)
         }}
      )
    end)
  end

for _ <- readers do
  receive do
    {:reader_started, _} -> :ok
  after
    2_000 -> raise "FIFO reader did not enter read"
  end
end

spawn(fn ->
  rows =
    for _ <- 1..64 do
      observed = %{
        time_ns: System.system_time(:nanosecond),
        monotonic_ns: System.monotonic_time(:nanosecond),
        run_queue_lengths_all: :erlang.statistics(:run_queue_lengths_all)
      }

      Process.sleep(5)
      observed
    end

  send(parent, {:queues_done, rows})
end)

start_ns = System.system_time(:nanosecond)
emit.(%{event: "hold_ready", start_ns: start_ns, blockers: count})
# IO.puts completes before either FIFO read can occupy dirty I/O. Python's
# independent release timer is now armed; no subsequent BEAM output is needed.
Enum.each(readers, &send(&1, :go))

# Separate direct API timings complement, and never replace, unchanged collector windows.
operations = [
  {"directory",
   fn ->
     {:ok, names} =
       :erl_prim_loader.list_dir(String.to_charlist(System.fetch_env!("ESS_TMP_ROOT")))

     length(names)
   end},
  {"stat",
   fn ->
     {:ok, info} =
       :file.read_file_info(Path.join(System.fetch_env!("ESS_TMP_ROOT"), "probe.tmp"), [
         :raw,
         {:time, :posix}
       ])

     elem(info, 1)
   end},
  {"rss",
   fn ->
     {:ok, value} = :file.read_file("/proc/self/status", [:raw])
     byte_size(value)
   end},
  {"raw_export",
   fn ->
     {:ok, file} = :file.open(Path.join(root, "direct-export.jsonl"), [:append, :raw, :binary])
     :ok = :file.write(file, "{\"diagnostic\":true}\n")
     :ok = :file.close(file)
     20
   end}
]

for {label, operation} <- operations do
  spawn(fn ->
    begin_ns = System.system_time(:nanosecond)
    begin_mono = System.monotonic_time(:nanosecond)
    value = operation.()
    finish_mono = System.monotonic_time(:nanosecond)
    finish_ns = System.system_time(:nanosecond)

    send(
      parent,
      {:operation_done,
       %{
         label: label,
         start_ns: begin_ns,
         end_ns: finish_ns,
         duration_ns: finish_mono - begin_mono,
         value: value
       }}
    )
  end)
end

operations_result =
  for _ <- operations do
    receive do
      {:operation_done, result} -> result
    after
      5_000 -> raise "direct operation timeout"
    end
  end

readers_result =
  for _ <- readers do
    receive do
      {:reader_done, result} -> result
    after
      5_000 -> raise "FIFO release timeout"
    end
  end

queues =
  receive do
    {:queues_done, rows} -> rows
  after
    5_000 -> raise "queue observation timeout"
  end

end_ns = System.system_time(:nanosecond)
Process.sleep(150)
File.rm_rf!(System.fetch_env!("ESS_TMP_ROOT"))
File.rm_rf!(System.fetch_env!("ESS_DATA_ROOT"))
Process.sleep(150)
Process.exit(collector, :kill)

emit.(%{
  event: "complete",
  start_ns: start_ns,
  end_ns: end_ns,
  scheduler_counts: %{
    normal: :erlang.system_info(:schedulers_online),
    dirty_cpu: :erlang.system_info(:dirty_cpu_schedulers_online),
    dirty_io: :erlang.system_info(:dirty_io_schedulers)
  },
  otp: System.otp_release(),
  elixir: System.version(),
  operations: operations_result,
  fifo_reads: readers_result,
  queue_observations: queues
})
