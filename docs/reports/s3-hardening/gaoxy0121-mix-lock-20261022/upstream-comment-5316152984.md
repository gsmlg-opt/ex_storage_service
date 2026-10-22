There are 2 further problems:
1. The crash is not self healing on retry. If an empty stale file is there the subsequent retries fail

reproducible with
```bash
docker run --rm -v /Users/myuser/elixir/lock_repro:/repro elixir:1.20 elixir /repro/persist.exs
```

```elixir
# Sibling 1: an empty lock_0 poisons ordinary subsequent same-key lockers.
#
# HONEST SCOPE. The TRIGGER for this state is a process dying in the window
# between the open(O_TRUNC) and the write() that switch_file_create!'s
# File.write!(port_P, _, [:raw]) at lib/mix/sync/lock.ex:406 performs. That is a
# genuine race (OTP does the open and write as separate dirty-NIF calls, with
# fstat/resource/scheduling work in between). This script MODELS the death; it
# does not catch a real kill landing in that window. The earlier 0/150 external
# SIGKILL experiment only shows that one signal strategy missed the window -- it
# does NOT establish the window as "sub-microsecond" or unhittable.
#
# What it DOES prove deterministically, on state produced by real Mix + real
# syscalls:
#   (a) a real Mix process leaves lock_0 and port_P as two hard links to one
#       inode (observed);
#   (b) the real O_TRUNC that line 406 performs (open port_P for writing)
#       empties lock_0 through the shared inode BEFORE any byte is written --
#       we then close WITHOUT writing, which models "the process died before
#       the write". This is the only modeled step, and it uses the real open,
#       not a fabricated payload;
#   (c) the empty lock_0 is not self-healed by ordinary use: each subsequent
#       real, unmodified Mix locker that is assigned a DIFFERENT port has its
#       with_lock/2 raise the reported MatchError "" and leaves lock_0 empty.
#
# NOT claimed: permanent poisoning. The empty state lasts until manual cleanup
# OR until some future locker happens to be assigned the original port P again
# -- that one rewrites the hard-linked port_P, making lock_0 non-empty (and
# likely transitions into the self-connect deadlock instead). With a full
# ephemeral range that P-recycle is rare, so ordinary same-key lockers keep
# raising in the meantime; this script uses the full range for that reason.

Mix.start()
elixir = System.get_env("ELIXIR_BIN") || System.find_executable("elixir")

lock_dir = fn key ->
  hash = key |> :erlang.md5() |> Base.url_encode64(padding: false)
  uid = case System.cmd("id", ["-u"]) do
    {out, 0} -> String.trim(out)
    _ -> "0"
  end
  Path.join([System.tmp_dir!(), "mix_lock_user#{uid}", hash])
end

run_locker = fn key ->
  code =
    "Mix.start(); " <>
      "try do Mix.Sync.Lock.with_lock(#{inspect(key)}, fn -> :ok end); IO.puts(\"ACQUIRED\") " <>
      "rescue e -> IO.puts(Exception.format(:error, e, __STACKTRACE__)) end"

  {out, status} = System.cmd(elixir, ["-e", code], stderr_to_stdout: true)
  {out, status}
end

size = fn path ->
  case File.stat(path) do
    {:ok, s} -> s.size
    _ -> :missing
  end
end

key = "persist_#{:erlang.unique_integer([:positive])}"
dir = lock_dir.(key)

# (a) real Mix process leaves the stale hard-linked pair
{_, 0} = run_locker.(key)
port_file = File.ls!(dir) |> Enum.find(&String.starts_with?(&1, "port_"))
lock0 = Path.join(dir, "lock_0")
port_path = Path.join(dir, port_file)
st = File.stat!(lock0)
stp = File.stat!(port_path)

IO.puts("== (a) real Mix left the stale pair ==")
IO.puts("  lock_0 size=#{st.size} inode=#{st.inode} nlinks=#{st.links}")
IO.puts("  #{port_file} inode=#{stp.inode}  shared_inode=#{st.inode == stp.inode}\n")

# (b) the real O_TRUNC from line 406, with the process "dying" before the write
IO.puts("== (b) model death inside switch_file_create!'s open(O_TRUNC)->write window ==")
IO.puts("  lock_0 before: #{size.(lock0)} bytes")
{:ok, fd} = :file.open(port_path, [:write, :raw, :binary])
IO.puts("  after open(#{port_file}, [:write]) -- O_TRUNC fired, NO write yet: lock_0 = #{size.(lock0)} bytes")
:file.close(fd)
IO.puts("  process 'died' before writing; lock_0 stays #{size.(lock0)} bytes (empty)\n")

# (c) each subsequent real Mix locker's with_lock/2 raises; lock_0 stays empty.
# Note: run_locker rescues the exception, so the OS process EXITS 0 -- the
# observable is "with_lock/2 raised", not "the process crashed".
IO.puts("== (c) N real, unmodified Mix lockers hit the empty lock_0 ==")

results =
  for n <- 1..5 do
    {out, _status} = run_locker.(key)
    raised = String.contains?(out, "MatchError") and String.contains?(out, "switch_file_read")
    acquired = String.contains?(out, "ACQUIRED")
    sz = size.(lock0)
    IO.puts("  locker #{n}: with_lock raised MatchError\"\"=#{raised} acquired=#{acquired} lock_0_after=#{sz} bytes")
    if n == 1, do: IO.puts("\n  ---- locker 1 output ----\n" <> out)
    {raised, acquired, sz}
  end

all_raised = Enum.all?(results, fn {r, _a, _s} -> r end)
none_acquired = Enum.all?(results, fn {_r, a, _s} -> not a end)
still_empty = Enum.all?(results, fn {_r, _a, sz} -> sz == 0 end)

IO.puts("\nSUMMARY")
IO.puts("  every tested lock acquisition raised MatchError \"\": #{all_raised}")
IO.puts("  none of the 5 lockers acquired the lock: #{none_acquired}")
IO.puts("  lock_0 stayed empty across all 5 attempts (not self-healed by ordinary use): #{still_empty}")

File.rm_rf(dir)

if all_raised and none_acquired and still_empty do
  IO.puts("VERDICT: a killed-mid-write process can leave a durable empty lock_0 that makes")
  IO.puts("         ordinary same-key with_lock/2 calls raise the reported MatchError. Consequence")
  IO.puts("         proven; the kill trigger is modeled; poisoning lasts until cleanup or a P-recycle.")
  System.halt(0)
else
  System.halt(2)
end
```

Result:
```
== (a) real Mix left the stale pair ==
  lock_0 size=75 inode=3073806 nlinks=2
  port_34815 inode=3073806  shared_inode=true

== (b) model death inside switch_file_create!'s open(O_TRUNC)->write window ==
  lock_0 before: 75 bytes
  after open(port_34815, [:write]) -- O_TRUNC fired, NO write yet: lock_0 = 0 bytes
  process 'died' before writing; lock_0 stays 0 bytes (empty)

== (c) N real, unmodified Mix lockers hit the empty lock_0 ==
  locker 1: with_lock raised MatchError""=true acquired=false lock_0_after=0 bytes

  ---- locker 1 output ----
** (MatchError) no match of right hand side value:

    ""

    (mix 1.20.2) lib/mix/sync/lock.ex:436: Mix.Sync.Lock.switch_file_read/1
    (mix 1.20.2) lib/mix/sync/lock.ex:253: Mix.Sync.Lock.fetch_probe_port/1
    (mix 1.20.2) lib/mix/sync/lock.ex:245: Mix.Sync.Lock.probe/1
    (mix 1.20.2) lib/mix/sync/lock.ex:208: Mix.Sync.Lock.grab_lock/3
    (mix 1.20.2) lib/mix/sync/lock.ex:179: Mix.Sync.Lock.try_lock/4
    (mix 1.20.2) lib/mix/sync/lock.ex:147: Mix.Sync.Lock.lock/2
    (mix 1.20.2) lib/mix/sync/lock.ex:118: Mix.Sync.Lock.with_lock/3
    nofile:1: (file)


  locker 2: with_lock raised MatchError""=true acquired=false lock_0_after=0 bytes
  locker 3: with_lock raised MatchError""=true acquired=false lock_0_after=0 bytes
  locker 4: with_lock raised MatchError""=true acquired=false lock_0_after=0 bytes
  locker 5: with_lock raised MatchError""=true acquired=false lock_0_after=0 bytes

SUMMARY
  every tested lock acquisition raised MatchError "": true
  none of the 5 lockers acquired the lock: true
  lock_0 stayed empty across all 5 attempts (not self-healed by ordinary use): true
VERDICT: a killed-mid-write process can leave a durable empty lock_0 that makes
         ordinary same-key with_lock/2 calls raise the reported MatchError. Consequence
         proven; the kill trigger is modeled; poisoning lasts until cleanup or a P-recycle.
```

2. Self deadlock

reproducible with
```bash
docker run --rm --sysctl net.ipv4.ip_local_port_range="50000 50063" -v /Users/myuser/elixir/lock_repro:/repro elixir:1.20 elixir /repro/deadlock.exs 3000
```

```elixir
# Sibling 2: self-connect deadlock, naturally reproduced with rigorous proof.
#
# Same storm as coordinator.exs (only real, unmodified Mix workers calling
# Mix.Sync.Lock.with_lock/2; the kernel recycles the ephemeral port). When the
# kernel hands worker B the port P that a stale lock file still names, B:
#   * truncates+rewrites lock_0 with its OWN port P (switch_file_create!),
#   * links port_P -> lock_1, then probe()s lock_0, reads port P,
#   * connect()s to P -- its OWN listen socket -- gets its own probe bytes,
#   * concludes "lock is taken", and await_close()s a socket that only closes
#     when B itself exits. B waits for itself forever.
#
# A timeout alone is NOT taken as proof. When a worker hangs, this establishes
# ALL of the following from the kernel's own tables before declaring proof:
#   1. lock_0 decodes to port P and os_pid PID.
#   2. There is a LISTEN socket on P (inode L).
#   3. There is an ESTABLISHED loopback pair: client (local Q, remote P, inode C)
#      AND accept (local P, remote Q, inode A) for the SAME Q -- a real
#      connection to P whose accept side is also on P.
#   4. ONE hung worker owns L, C AND A (all three inodes appear in its
#      /proc/<pid>/fd) -- so the listener, the connector and the accepted socket
#      are the same process.
#   5. That worker's BEAM pid (System.pid(), reported at startup) equals PID
#      decoded from lock_0.
# Only then is it {:proven}. If any check fails we keep hunting.

defmodule Proc do
  # 127.0.0.1 as it appears in /proc/net/tcp (little-endian hex)
  @loopback_v4 "0100007F"

  def loopback, do: @loopback_v4

  # Parse /proc/net/tcp{,6}: returns [{st, lip, lport, rip, rport, inode}]
  def sockets do
    ["/proc/net/tcp", "/proc/net/tcp6"]
    |> Enum.flat_map(fn path ->
      case File.read(path) do
        {:ok, data} ->
          data
          |> String.split("\n", trim: true)
          |> Enum.drop(1)
          |> Enum.flat_map(fn line ->
            case String.split(line, ~r/\s+/, trim: true) do
              [_sl, local, rem, st, _txrx, _trwhen, _retr, _uid, _to, inode | _] ->
                {lip, lport} = split_addr(local)
                {rip, rport} = split_addr(rem)
                [{st, lip, lport, rip, rport, inode}]

              _ ->
                []
            end
          end)

        _ ->
          []
      end
    end)
  end

  defp split_addr(addr) do
    [ip, port_hex] = String.split(addr, ":")
    {ip, String.to_integer(port_hex, 16)}
  end

  # socket inodes owned by pid, via /proc/<pid>/fd/* -> "socket:[NNN]"
  def socket_inodes(pid) do
    dir = "/proc/#{pid}/fd"

    case File.ls(dir) do
      {:ok, fds} ->
        fds
        |> Enum.flat_map(fn fd ->
          case File.read_link("#{dir}/#{fd}") do
            {:ok, "socket:[" <> rest} -> [String.trim_trailing(rest, "]")]
            _ -> []
          end
        end)
        |> MapSet.new()

      _ ->
        MapSet.new()
    end
  end
end

defmodule DL do
  @elixir System.get_env("ELIXIR_BIN") || System.find_executable("elixir")
  @workers 12
  @round_timeout 1_500
  @seed_timeout 10_000

  def lock_dir(key) do
    hash = key |> :erlang.md5() |> Base.url_encode64(padding: false)

    uid =
      case System.cmd("id", ["-u"]) do
        {out, 0} -> String.trim(out)
        _ -> "0"
      end

    Path.join([System.tmp_dir!(), "mix_lock_user#{uid}", hash])
  end

  # decode -> {port, os_pid_string} or nil for empty/short data
  def decode(<<switch, rest::binary>>) when byte_size(rest) > 0 and rem(byte_size(rest), 2) == 0 do
    n = div(byte_size(rest), 2)
    <<c1::binary-size(^n), c2::binary-size(^n)>> = rest
    seg = if switch == 0, do: c1, else: c2

    case seg do
      <<port::unsigned-integer-32, psz::8, _pad::binary-size(psz), os_pid::binary>> ->
        {port, os_pid}

      _ ->
        nil
    end
  end

  def decode(_), do: nil

  def spawn_worker(i) do
    port = Port.open({:spawn_executable, @elixir}, [:binary, :exit_status, {:line, 16384}, args: ["/repro/worker.exs"]])
    beam = await_start(port, nil)
    {i, port, beam}
  end

  defp await_start(port, beam) do
    receive do
      {^port, {:data, {:eol, "PID " <> pid}}} -> await_start(port, String.trim(pid))
      {^port, {:data, {:eol, "READY"}}} -> beam
      {^port, {:data, {:eol, _}}} -> await_start(port, beam)
    after
      @seed_timeout -> raise "worker did not start"
    end
  end

  defp collect(port, deadline) do
    left = deadline - System.monotonic_time(:millisecond)

    if left <= 0 do
      :timeout
    else
      receive do
        {^port, {:data, {:eol, "DONE ok"}}} -> :ok
        {^port, {:data, {:eol, "DONE crash"}}} -> :crash
        {^port, {:data, {:eol, _}}} -> collect(port, deadline)
        {^port, {:exit_status, st}} -> {:exited, st}
      after
        left -> :timeout
      end
    end
  end

  defp kill({_i, port, beam}) do
    if beam, do: System.cmd("kill", ["-9", beam])
    (try do Port.close(port) catch _, _ -> :ok end)
    flush(port)
  end

  defp flush(port) do
    receive do
      {^port, _} -> flush(port)
    after
      0 -> :ok
    end
  end

  # Rigorous proof (see module header for the 5 required checks).
  defp prove(hung, dir) do
    lock0 = Path.join(dir, "lock_0")

    with {:ok, data} <- File.read(lock0),
         {p, lock_pid} when p > 0 <- decode(data) do
      socks = Proc.sockets()
      lo = Proc.loopback()

      # LISTEN on 127.0.0.1:P
      listener =
        Enum.find(socks, fn {st, lip, lport, _rip, _rport, _i} ->
          st == "0A" and lip == lo and lport == p
        end)

      # client side: 127.0.0.1:Q -> 127.0.0.1:P  (both loopback)
      client =
        Enum.find(socks, fn {st, lip, _lport, rip, rport, _i} ->
          st == "01" and lip == lo and rip == lo and rport == p
        end)

      # accept side: 127.0.0.1:P -> 127.0.0.1:Q  (same Q, both loopback)
      accept =
        case client do
          {_st, _lip, q, _rip, _rport, _i} ->
            Enum.find(socks, fn {st, lip, lport, rip, rport, _i} ->
              st == "01" and lip == lo and rip == lo and lport == p and rport == q
            end)

          _ ->
            nil
        end

      inodes =
        for x <- [listener, client, accept], is_tuple(x), do: elem(x, 5)

      if listener && client && accept && length(inodes) == 3 do
        needed = MapSet.new(inodes)

        owner =
          Enum.find(hung, fn {_i, _port, beam} ->
            beam && MapSet.subset?(needed, Proc.socket_inodes(beam))
          end)

        case owner do
          {i, _port, beam} when beam == lock_pid ->
            {:proven,
             %{
               worker: i,
               beam_pid: beam,
               lock_pid: lock_pid,
               lock_port: p,
               listen_inode: elem(listener, 5),
               client_inode: elem(client, 5),
               accept_inode: elem(accept, 5),
               client_local_port: elem(client, 2)
             }}

          {_i, _port, beam} ->
            {:no_proof, "owner beam #{inspect(beam)} != lock_0 pid #{inspect(lock_pid)}"}

          nil ->
            {:no_proof, "no single hung worker owns all of listener+client+accept"}
        end
      else
        {:no_proof, "did not find listener+client+accept trio for port #{p}"}
      end
    else
      _ -> {:no_proof, "lock_0 unreadable / empty / no port"}
    end
  end

  def run(max_rounds) do
    workers = Enum.map(1..@workers, &spawn_worker/1)
    IO.puts("#{@workers} real Mix workers ready; hunting self-connect deadlock")
    loop(workers, 1, max_rounds, 0)
  end

  defp loop(workers, round, max, hung_total) when round > max do
    Enum.each(workers, &kill/1)
    IO.puts("\nnot proven within #{max} rounds (#{hung_total} hung observations)")
    System.halt(2)
  end

  defp loop(workers, round, max, hung_total) do
    key = "dl_#{:erlang.unique_integer([:positive])}"
    dir = lock_dir(key)
    [{_, seed_port, _} | _] = workers

    Port.command(seed_port, key <> "\n")

    case collect(seed_port, System.monotonic_time(:millisecond) + @seed_timeout) do
      :ok ->
        deadline = System.monotonic_time(:millisecond) + @round_timeout
        Enum.each(workers, fn {_, p, _} -> Port.command(p, key <> "\n") end)
        results = Enum.map(workers, fn {_i, p, _b} = w -> {w, collect(p, deadline)} end)
        hung = for {w, :timeout} <- results, do: w

        proof = if hung != [], do: prove(hung, dir), else: {:no_proof, "no hung workers"}

        case proof do
          {:proven, info} ->
            IO.puts("\n############# SELF-CONNECT DEADLOCK PROVEN (round #{round}) #############")
            IO.puts("  lock_0 decodes to port P=#{info.lock_port}, os_pid=#{info.lock_pid}")
            IO.puts("  hung worker #{info.worker} has BEAM pid #{info.beam_pid} == lock_0 os_pid  [check 5]")
            IO.puts("  LISTEN on P: inode #{info.listen_inode}  [check 2]")
            IO.puts("  ESTABLISHED client 127.0.0.1:#{info.client_local_port}->:#{info.lock_port} inode #{info.client_inode}")
            IO.puts("  ESTABLISHED accept 127.0.0.1:#{info.lock_port}->:#{info.client_local_port} inode #{info.accept_inode}  [check 3]")
            IO.puts("  all three inodes are in /proc/#{info.beam_pid}/fd  [check 4]")
            IO.puts("  => the worker holding the lock's port connected that port to ITSELF and is")
            IO.puts("     blocked in await_close on its own socket, for the full #{@round_timeout}ms window.")
            IO.puts("########################################################################")
            Enum.each(workers, &kill/1)
            System.halt(0)

          {:no_proof, _why} ->
            Enum.each(hung, &kill/1)
            workers = Enum.map(workers, fn {i, _, _} = w -> if w in hung, do: spawn_worker(i), else: w end)
            if rem(round, 50) == 0, do: IO.puts("round #{round}: hung observations so far #{hung_total + length(hung)}")
            loop(workers, round + 1, max, hung_total + length(hung))
        end

      _ ->
        [seed | rest] = workers
        kill(seed)
        loop([spawn_worker(elem(seed, 0)) | rest], round + 1, max, hung_total)
    end
  end
end

[max_rounds] = System.argv()
DL.run(String.to_integer(max_rounds))
```

Result:
```
12 real Mix workers ready; hunting self-connect deadlock

############# SELF-CONNECT DEADLOCK PROVEN (round 11) #############
  lock_0 decodes to port P=50035, os_pid=3242
  hung worker 1 has BEAM pid 3242 == lock_0 os_pid  [check 5]
  LISTEN on P: inode 14416  [check 2]
  ESTABLISHED client 127.0.0.1:50018->:50035 inode 14419
  ESTABLISHED accept 127.0.0.1:50035->:50018 inode 14420  [check 3]
  all three inodes are in /proc/3242/fd  [check 4]
  => the worker holding the lock's port connected that port to ITSELF and is
     blocked in await_close on its own socket, for the full 1500ms window.
########################################################################
```

Mechanism of the deadlock
1. Process A acquires port P and creates `port_P`.
2. Mix hard-links `port_P` to `lock_0`.
3. A unlocks. Both paths remain linked to the same inode. `lock_0` is marked stale with port 0.
4. Later, the kernel recycles port P and assigns it to process B’s lock listener.
5. B calls `File.write!(port_P, data, [:raw])`. Because stale `port_P` is still hard-linked to `lock_0`, this also rewrites `lock_0` with B’s port P. This also destroys the stale marker that would make B ignore `lock_0`
6. B tries to link `port_P` to `lock_0`, but `lock_0` but get's `:eexist`.
7. B probes `lock_0`, reads its own port P and OS pid. Tries to listen on P and fails then connects to P.
8. B accepts the connection and sends the valid "mixlock" probe response.
9. B concludes that another process owns the lock and calls `await_close/1`.
10. B waits for its own connection to close. This normally happens during `unlock/1` or process terminate but here B is blocked in `lock/2`