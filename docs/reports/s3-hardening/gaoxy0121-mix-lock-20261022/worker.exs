Mix.start()
IO.puts("RUNTIME #{System.version()} OTP #{System.otp_release()} PID #{System.pid()} MODULE #{:code.which(Mix.Sync.Lock)}")
for n <- 1..100 do
  IO.puts("ATTEMPT #{n}")
  Mix.Sync.Lock.with_lock("ess-isolated-lock-probe", fn -> IO.puts("ACQUIRED #{n}") end,
    on_taken: fn pid -> IO.puts("TAKEN #{pid} SELF #{System.pid()}") end)
end
