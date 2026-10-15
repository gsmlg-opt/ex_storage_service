defmodule ExStorageServiceCli.Credentials.OwnerTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Credentials.{Owner, Record}
  alias ExStorageServiceCli.Error

  test "concurrent waiters share one refresh and subsequent calls reuse unexpired credentials" do
    parent = self()

    {:ok, owner} =
      Owner.start_link(
        fn ->
          send(parent, {:loading, self()})

          receive do
            :release -> {:ok, record(), %{profile: "test"}}
          end
        end,
        clock: fn -> ~U[2026-10-15 00:00:00Z] end
      )

    on_exit(fn -> if Process.alive?(owner), do: GenServer.stop(owner) end)
    tasks = for _ <- 1..8, do: Task.async(fn -> Owner.resolve(owner) end)
    assert_receive {:loading, loader}
    refute_receive {:loading, _}, 50
    send(loader, :release)
    results = Enum.map(tasks, &Task.await/1)
    assert Enum.all?(results, &match?({:ok, %Record{}, _}, &1))
    assert {:ok, %Record{}, _} = Owner.resolve(owner)
    refute_receive {:loading, _}, 50
  end

  test "expiry skew triggers fresh credentials and failed refresh does not return expired values" do
    {:ok, clock} = Agent.start_link(fn -> ~U[2026-10-15 00:00:00Z] end)
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    {:ok, owner} =
      Owner.start_link(
        fn ->
          n = Agent.get_and_update(counter, &{&1 + 1, &1 + 1})

          if n == 1,
            do: {:ok, record(), %{}},
            else: {:error, Error.new(:credentials, "refresh refused")}
        end,
        clock: fn -> Agent.get(clock, & &1) end
      )

    assert {:ok, %Record{}, _} = Owner.resolve(owner)
    Agent.update(clock, fn _ -> ~U[2026-10-15 00:56:00Z] end)
    assert {:error, %Error{category: :credentials}} = Owner.resolve(owner)
    assert Agent.get(counter, & &1) == 2
    GenServer.stop(owner)
  end

  test "waiter bound rejects overload and owner shutdown stops owned refresher" do
    parent = self()

    {:ok, owner} =
      Owner.start_link(
        fn ->
          send(parent, {:loading, self()})

          receive do
            :release -> {:ok, record(), %{}}
          end
        end,
        max_waiters: 1,
        clock: fn -> ~U[2026-10-15 00:00:00Z] end
      )

    task = Task.async(fn -> Owner.resolve(owner) end)
    assert_receive {:loading, loader}
    assert {:error, %Error{code: :credential_refresh_busy}} = Owner.resolve(owner)
    monitor = Process.monitor(loader)
    GenServer.stop(owner)
    assert_receive {:DOWN, ^monitor, :process, ^loader, :killed}
    assert {:error, %Error{code: :credential_refresh_unavailable}} = Task.await(task)
  end

  test "diagnostic state hides loader credentials" do
    {:ok, owner} =
      Owner.start_link(fn -> {:ok, record(), %{}} end, clock: fn -> ~U[2026-10-15 00:00:00Z] end)

    assert {:ok, _, _} = Owner.resolve(owner)
    refute inspect(:sys.get_status(owner)) =~ "secret-marker"
    GenServer.stop(owner)
  end

  test "advisory caller refreshes while concurrent callers reuse valid snapshot; mandatory callers wait" do
    {:ok, clock} = Agent.start_link(fn -> ~U[2026-10-15 00:00:00Z] end)
    {:ok, count} = Agent.start_link(fn -> 0 end)
    parent = self()

    {:ok, owner} =
      Owner.start_link(
        fn ->
          attempt = Agent.get_and_update(count, &{&1 + 1, &1 + 1})

          if attempt == 1 do
            {:ok, record(), %{}}
          else
            send(parent, {:refresh, self()})

            receive do
              :fail -> {:error, Error.new(:credentials, "refused")}
            end
          end
        end,
        clock: fn -> Agent.get(clock, & &1) end
      )

    assert {:ok, old, _} = Owner.resolve(owner)
    Agent.update(clock, fn _ -> ~U[2026-10-15 00:46:00Z] end)
    first = Task.async(fn -> Owner.resolve(owner) end)
    assert_receive {:refresh, worker}
    assert {:ok, ^old, _} = Owner.resolve(owner)
    send(worker, :fail)
    assert {:ok, ^old, _} = Task.await(first)
    Agent.update(clock, fn _ -> ~U[2026-10-15 00:51:00Z] end)
    mandatory = Task.async(fn -> Owner.resolve(owner) end)
    assert_receive {:refresh, worker}
    second = Task.async(fn -> Owner.resolve(owner) end)
    assert Task.yield(second, 30) == nil
    send(worker, :fail)
    assert {:error, %Error{category: :credentials}} = Task.await(mandatory)
    assert {:error, %Error{category: :credentials}} = Task.await(second)
    GenServer.stop(owner)
  end

  test "SSO advisory failures throttle for sixty seconds while base credentials can retry" do
    for provider <- [:sso, :environment] do
      {:ok, clock} = Agent.start_link(fn -> ~U[2026-10-15 00:00:00Z] end)
      {:ok, count} = Agent.start_link(fn -> 0 end)

      {:ok, owner} =
        Owner.start_link(
          fn ->
            attempt = Agent.get_and_update(count, &{&1 + 1, &1 + 1})

            if attempt == 1,
              do: {:ok, %{record() | provenance: %{provider: provider}}, %{}},
              else: {:error, Error.new(:credentials, "refused")}
          end,
          clock: fn -> Agent.get(clock, & &1) end
        )

      assert {:ok, _, _} = Owner.resolve(owner)
      Agent.update(clock, fn _ -> ~U[2026-10-15 00:46:00Z] end)
      assert {:ok, _, _} = Owner.resolve(owner)
      assert {:ok, _, _} = Owner.resolve(owner)
      assert Agent.get(count, & &1) == if(provider == :sso, do: 2, else: 3)
      Agent.update(clock, fn _ -> ~U[2026-10-15 01:01:00Z] end)
      assert {:error, %Error{}} = Owner.resolve(owner)
      GenServer.stop(owner)
    end
  end

  test "timed-out live callers abandon waiters and terminate the sole refresher" do
    parent = self()

    {:ok, owner} =
      Owner.start_link(fn ->
        send(parent, {:refresh, self()})

        receive do
          :never -> :ok
        end
      end)

    task = Task.async(fn -> Owner.resolve(owner, 100) end)
    assert_receive {:refresh, worker}
    monitor = Process.monitor(worker)
    assert {:error, %Error{code: :credential_refresh_unavailable}} = Task.await(task)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}
    GenServer.stop(owner)
  end

  defp record,
    do: %Record{
      access_key_id: "a",
      secret_access_key: "secret-marker",
      session_token: "t",
      expires_at: ~U[2026-10-15 01:00:00Z],
      provenance: %{provider: :test}
    }
end
