target = :"ess_data_a@127.0.0.1"
deadline = System.monotonic_time(:millisecond) + 30_000

connect = fn connect ->
  if Node.connect(target) do
    :ok
  else
    if System.monotonic_time(:millisecond) < deadline do
      Process.sleep(250)
      connect.(connect)
    else
      raise "could not connect to #{inspect(target)}"
    end
  end
end

:ok = connect.(connect)

case :rpc.call(target, Concord, :status, [], 10_000) do
  {:ok, %{cluster: %{primary_id: "meta"}}} ->
    IO.puts("confirmed metadata primary meta")

  other ->
    raise "expected metadata primary meta, got: #{inspect(other)}"
end
