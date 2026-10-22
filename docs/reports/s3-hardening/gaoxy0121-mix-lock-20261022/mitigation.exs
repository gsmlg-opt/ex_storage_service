Mix.start()
IO.puts("RUNTIME #{System.version()} OTP #{System.otp_release()} PID #{System.pid()}")
IO.puts("MIX_OS_CONCURRENCY_LOCK #{System.get_env("MIX_OS_CONCURRENCY_LOCK")}")
config = [build_path: Path.join(System.tmp_dir!(), "private-build"), build_per_environment: false]
for n <- 1..2 do
  Mix.Project.with_build_lock(config, fn ->
    {:ok, tcp} = File.read("/proc/self/net/tcp")
    listeners = tcp |> String.split("\n") |> Enum.drop(1) |> Enum.filter(&(String.split(&1) |> Enum.at(3) == "0A"))
    files = Path.wildcard(Path.join(System.tmp_dir!(), "mix_lock_*/*/*"))
    IO.puts("ACQUIRED #{n} LOCK_FILES #{inspect(files)} TCP_LISTENERS #{length(listeners)}")
    if files != [] or listeners != [], do: raise("unexpected lock resource")
  end)
end
IO.puts("MITIGATION_PASS")
