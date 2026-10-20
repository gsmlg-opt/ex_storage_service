# Test-only bounded error diagnostics for a disposable acceptance process.
# Capture failure shapes, never function arguments, object metadata or credentials.
root = System.fetch_env!("E2E_RESOURCE_ROOT")

spawn(fn ->
  load = fn load ->
    if Code.ensure_loaded?(ExStorageService.CloudCache.WriteThrough) do
      :ok
    else
      Process.sleep(50)
      load.(load)
    end
  end

  load.(load)

  sanitize = fn sanitize, value, depth ->
    cond do
      depth > 5 -> "depth_limit"
      is_atom(value) -> Atom.to_string(value)
      is_integer(value) -> value
      is_tuple(value) -> Enum.map(Tuple.to_list(value), &sanitize.(sanitize, &1, depth + 1))
      true -> "redacted"
    end
  end

  :erlang.trace_pattern(
    {ExStorageService.CloudCache.WriteThrough, :put, 7},
    [{:_, [], [{:return_trace}]}],
    [:local]
  )

  :erlang.trace(:all, true, [:call, {:tracer, self()}])

  loop = fn loop ->
    receive do
      {:trace, _pid, :return_from, {ExStorageService.CloudCache.WriteThrough, :put, 7},
       {:error, reason}} ->
        File.write!(
          Path.join(root, "resource-errors.jsonl"),
          JSON.encode!(%{
            time: System.system_time(:millisecond),
            error: sanitize.(sanitize, reason, 0)
          }) <> "\n",
          [:append]
        )

      _ ->
        :ok
    end

    loop.(loop)
  end

  loop.(loop)
end)
