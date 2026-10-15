defmodule ExStorageServiceCli.Transfer.Progress do
  @moduledoc "Throttled transfer progress on stderr; payload stdout stays exclusive."
  def with(options, opts, callback) do
    enabled =
      Map.get(options, :progress, true) and options[:quiet] != true and
        options[:only_show_errors] != true and
        options[:output] != "off"

    if enabled do
      state = :atomics.new(2, signed: true)
      :atomics.put(state, 1, System.monotonic_time(:millisecond))
      frequency = max(options[:progress_frequency] || 1, 1) * 1000
      write = opts[:progress_write] || fn text -> IO.write(:stderr, text) end
      separator = if options[:progress_multiline], do: "\n", else: "\r"

      progress = fn bytes ->
        :atomics.put(state, 2, bytes)
        now = System.monotonic_time(:millisecond)
        before = :atomics.get(state, 1)

        if now - before >= frequency and :atomics.compare_exchange(state, 1, before, now) == :ok do
          write.("Completed " <> Integer.to_string(bytes) <> " Bytes" <> separator)
        end
      end

      result = callback.(Keyword.put(opts, :on_progress, progress))

      if match?({:ok, _}, result),
        do: write.("Completed " <> Integer.to_string(:atomics.get(state, 2)) <> " Bytes\n")

      result
    else
      callback.(opts)
    end
  end
end
