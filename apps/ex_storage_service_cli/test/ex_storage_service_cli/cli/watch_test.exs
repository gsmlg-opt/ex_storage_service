defmodule ExStorageServiceCli.CLI.WatchTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.CLI.{Invocation, Parser, Watch}
  alias ExStorageServiceCli.Transfer.{Coordinator, Manifest}

  @moduletag :tmp_dir

  test "requires explicit polling and validates bounds before taking any inventory" do
    for {options, code} <- [
          {%{}, :watch_adapter},
          {%{adapter: "notifications"}, :watch_adapter},
          {%{adapter: "polling", interval: 0}, :watch_interval},
          {%{adapter: "polling", interval: 3601}, :watch_interval},
          {%{adapter: "polling", count: -1}, :watch_count},
          {%{adapter: "polling", count: "1"}, :watch_count},
          {%{adapter: "polling", events: "get"}, :watch_events},
          {%{adapter: "polling", events: "put,get"}, :watch_events},
          {%{adapter: "polling", events: "put,"}, :watch_events},
          {%{adapter: "polling", events: ""}, :watch_events},
          {%{adapter: "polling", events: 1}, :watch_events},
          {%{adapter: "polling", prefix: 1}, :watch_selection},
          {%{adapter: "polling", suffix: 1}, :watch_selection},
          {%{adapter: "polling", recursive: "true"}, :watch_selection}
        ] do
      assert {:error, %Error{code: ^code}} =
               Watch.run(%Invocation{options: options}, target(), nil,
                 inventory: fn _, _, _ -> flunk("invalid watch took an inventory") end
               )
    end

    assert {:error, %Error{code: :watch_target}} =
             Watch.run(invocation(), %Target{kind: :stdio}, nil)
  end

  test "native watch parses event and name selectors independently of the polling source" do
    assert {:ok,
            %Invocation{
              options: %{
                adapter: "polling",
                events: "put,delete",
                prefix: "a",
                suffix: ".bin",
                recursive: false
              }
            }} =
             Parser.parse([
               "watch",
               "./source",
               "--adapter",
               "polling",
               "--events",
               "put,delete",
               "--prefix",
               "a",
               "--suffix",
               ".bin",
               "--no-recursive"
             ])
  end

  test "put selection filters output after complete scans and retains reconciliation", %{
    tmp_dir: root
  } do
    inventory =
      sequence(
        [
          [entry("a-modified.bin"), entry("a-removed.bin")],
          [
            entry("a-modified.bin", 2),
            entry("a-created.bin"),
            entry("a-nested/hidden.bin"),
            entry("a-other.txt"),
            entry("other.bin")
          ]
        ],
        root,
        fn _, number -> send(self(), {:scan_complete, number}) end
      )

    assert :ok =
             Watch.run(
               invocation(events: "put", prefix: "a-", suffix: ".bin", recursive: false),
               target(),
               nil,
               inventory: inventory,
               sleep: fn _ -> :ok end,
               emit: fn event ->
                 assert_received {:scan_complete, 2}
                 send(self(), {:scan_complete, 2})
                 send(self(), {:event, event})
                 :ok
               end,
               on_reconcile: fn ->
                 send(self(), :reconcile)
                 :ok
               end
             )

    assert_received {:event, %{"Event" => "create", "Name" => "a-created.bin"}}
    assert_received {:event, %{"Event" => "modify", "Name" => "a-modified.bin"}}
    refute_received {:event, _}
    assert_received :reconcile
    assert_received :reconcile
    assert File.ls!(root) == []
  end

  test "delete selection includes nested names only with recursive selection", %{tmp_dir: root} do
    for recursive <- [false, true] do
      assert :ok =
               Watch.run(
                 invocation(
                   events: "delete",
                   prefix: "logs",
                   suffix: ".bin",
                   recursive: recursive
                 ),
                 target(),
                 nil,
                 inventory:
                   sequence(
                     [[entry("logs.bin"), entry("logs/deep/gone.bin"), entry("other.bin")], []],
                     root
                   ),
                 sleep: fn _ -> :ok end,
                 emit: fn event ->
                   send(self(), {:event, event})
                   :ok
                 end
               )

      assert_received {:event, %{"Event" => "remove", "Name" => "logs.bin"}}

      if recursive,
        do: assert_received({:event, %{"Event" => "remove", "Name" => "logs/deep/gone.bin"}}),
        else: refute_received({:event, %{"Name" => "logs/deep/gone.bin"}})

      refute_received {:event, _}
      assert File.ls!(root) == []
    end
  end

  test "real local inventories retain nested changes while output recursion is disabled", %{
    tmp_dir: root
  } do
    source = Path.join(root, "source")
    temporary = Path.join(root, "temporary")
    File.mkdir_p!(Path.join(source, "nested"))
    File.mkdir!(temporary)
    reconciliations = :atomics.new(1, signed: false)

    assert :ok =
             Watch.run(invocation(recursive: false), %Target{kind: :local, path: source}, nil,
               tmp_dir: temporary,
               sleep: fn _ -> :ok end,
               on_reconcile: fn ->
                 if :atomics.add_get(reconciliations, 1, 1) == 1 do
                   File.write!(Path.join(source, "visible.bin"), "visible")
                   File.write!(Path.join(source, "nested/hidden.bin"), "hidden")
                 end

                 :ok
               end,
               emit: fn event ->
                 send(self(), {:event, event})
                 :ok
               end
             )

    assert_received {:event, %{"Event" => "create", "Name" => "visible.bin"}}
    refute_received {:event, _}
    assert :atomics.get(reconciliations, 1) == 2
    assert File.ls!(temporary) == []
  end

  test "baseline emits nothing; completed scans stream sorted creates, removals and modifications",
       %{tmp_dir: root} do
    inventory =
      sequence(
        [
          [entry("unchanged"), entry("removed"), entry("modified", 1)],
          [entry("unchanged"), entry("created"), entry("modified", 2)],
          [entry("unchanged"), entry("created"), entry("modified", 2)]
        ],
        root
      )

    assert :ok =
             Watch.run(invocation(count: 2), target(), nil,
               inventory: inventory,
               sleep: fn _ -> :ok end,
               emit: fn event ->
                 send(self(), {:event, event})
                 :ok
               end,
               on_reconcile: fn ->
                 send(self(), :reconcile)
                 :ok
               end
             )

    assert_receive :reconcile

    assert_receive {:event,
                    %{
                      "Adapter" => "polling",
                      "Event" => "create",
                      "Name" => "created",
                      "Before" => nil
                    }}

    assert_receive {:event,
                    %{
                      "Event" => "modify",
                      "Name" => "modified",
                      "Before" => %{"size" => 1},
                      "After" => %{"size" => 2}
                    }}

    assert_receive {:event, %{"Event" => "remove", "Name" => "removed", "After" => nil}}
    assert_receive :reconcile
    refute_receive :reconcile
    refute_receive {:event, _}
    assert File.ls!(root) == []
  end

  test "baseline precedes initial reconciliation so changes during transfer appear next scan", %{
    tmp_dir: root
  } do
    source = Path.join(root, "source")
    temporary = Path.join(root, "temporary")
    File.mkdir!(source)
    File.mkdir!(temporary)
    File.write!(Path.join(source, "a"), "before")
    invocations = :atomics.new(1, signed: false)

    assert :ok =
             Watch.run(invocation(), %Target{kind: :local, path: source}, nil,
               tmp_dir: temporary,
               sleep: fn _ -> :ok end,
               on_reconcile: fn ->
                 if :atomics.add_get(invocations, 1, 1) == 1,
                   do: File.write!(Path.join(source, "during-transfer"), "created")

                 :ok
               end,
               emit: fn event ->
                 send(self(), {:event, event})
                 :ok
               end
             )

    assert_receive {:event, %{"Event" => "create", "Name" => "during-transfer"}}
    assert :atomics.get(invocations, 1) == 2
    assert File.ls!(temporary) == []
  end

  test "local snapshots cannot be created inside their watched directory", %{tmp_dir: root} do
    temporary = Path.join(root, "temporary")
    File.mkdir!(temporary)

    assert {:error, %Error{code: :watch_snapshot_location}} =
             Watch.run(invocation(), %Target{kind: :local, path: root}, nil, tmp_dir: temporary)

    assert File.ls!(temporary) == []

    assert {:error, %Error{code: :watch_snapshot_location}} =
             Watch.run(invocation(), %Target{kind: :local, path: root}, nil, tmp_dir: root)
  end

  test "previous and new snapshots remain private until successful reconciliation", %{
    tmp_dir: root
  } do
    count = :atomics.new(1, signed: false)
    inventory = sequence([[entry("a")], [entry("b")], [entry("c")]], root)

    assert :ok =
             Watch.run(invocation(count: 2), target(), nil,
               inventory: inventory,
               sleep: fn _ -> :ok end,
               emit: fn _ -> :ok end,
               on_reconcile: fn ->
                 call = :atomics.add_get(count, 1, 1)
                 snapshots = File.ls!(root)
                 assert length(snapshots) == if(call == 1, do: 1, else: 2)

                 for snapshot <- snapshots,
                     do:
                       assert(
                         Bitwise.band(File.stat!(Path.join(root, snapshot)).mode, 0o777) == 0o700
                       )

                 :ok
               end
             )

    assert File.ls!(root) == []
  end

  test "incomplete next inventory never emits removals or reconciles again", %{tmp_dir: root} do
    inventory =
      sequence([[entry("a")], {:error, Error.new(:transport, "later page failed")}], root)

    assert {:error, %Error{category: :transport}} =
             Watch.run(invocation(count: 3), target(), nil,
               inventory: inventory,
               sleep: fn _ -> :ok end,
               emit: fn _ -> flunk("incomplete scan emitted an event") end,
               on_reconcile: fn ->
                 send(self(), :reconcile)
                 :ok
               end
             )

    assert_receive :reconcile
    refute_receive :reconcile
    assert File.ls!(root) == []
  end

  test "initial and later reconciliation failures stop and clean all snapshots", %{tmp_dir: root} do
    for fail_at <- [1, 2] do
      calls = :atomics.new(1, signed: false)
      inventory = sequence([[entry("a")], [entry("b")]], root)

      assert {:error, %Error{code: :reconcile_failed}} =
               Watch.run(invocation(count: 3), target(), nil,
                 inventory: inventory,
                 sleep: fn _ -> :ok end,
                 emit: fn _ -> :ok end,
                 on_reconcile: fn ->
                   if :atomics.add_get(calls, 1, 1) == fail_at,
                     do:
                       {:error,
                        Error.new(:client, "reconciliation failed", code: :reconcile_failed)},
                     else: :ok
                 end
               )

      assert :atomics.get(calls, 1) == fail_at
      assert File.ls!(root) == []
    end
  end

  test "callback exceptions and invalid results become typed errors without future scans", %{
    tmp_dir: root
  } do
    for {callback, code} <- [
          {[emit: fn _ -> raise "sensitive callback detail" end], :watch_emit},
          {[emit: fn _ -> :invalid end], :watch_emit},
          {[sleep: fn _ -> throw(:failed) end], :watch_sleep},
          {[on_reconcile: fn -> exit(:failed) end], :watch_reconcile}
        ] do
      inventory = sequence([[entry("a")], [entry("b")]], root)

      opts =
        Keyword.merge(
          [inventory: inventory, sleep: fn _ -> :ok end, emit: fn _ -> :ok end],
          callback
        )

      assert {:error, %Error{code: ^code, message: message}} =
               Watch.run(invocation(), target(), nil, opts)

      refute message =~ "sensitive"
      assert File.ls!(root) == []
    end
  end

  test "failed manifest comparison cleans both snapshots and prevents reconciliation", %{
    tmp_dir: root
  } do
    inventory =
      sequence([[entry("a")], [entry("b")]], root, fn manifest, index ->
        if index == 2, do: File.write!(manifest.path, "invalid JSON\n")
      end)

    assert {:error, %Error{code: :watch_scan}} =
             Watch.run(invocation(), target(), nil,
               inventory: inventory,
               sleep: fn _ -> :ok end,
               on_reconcile: fn ->
                 send(self(), :reconcile)
                 :ok
               end
             )

    assert_receive :reconcile
    refute_receive :reconcile
    assert File.ls!(root) == []
  end

  test "idle cancellation checks at most 50ms slices and never takes a later snapshot", %{
    tmp_dir: root
  } do
    coordinator = start_supervised!(Coordinator)
    owner = self()
    inventory = sequence([[entry("a")]], root)

    task =
      Task.async(fn ->
        Watch.run(invocation(interval: 3600, count: 0), target(), nil,
          coordinator: coordinator,
          inventory: inventory,
          sleep: fn duration ->
            send(owner, {:sleep, duration})
            Process.sleep(duration)
          end
        )
      end)

    # Inventory setup may wait for schedulers; the requested sleep slice is bounded.
    assert_receive {:sleep, duration}, 1000
    assert duration <= 50
    assert :ok = Coordinator.cancel(coordinator)
    assert {:error, %Error{category: :interrupted}} = Task.await(task, 1000)
    assert File.ls!(root) == []
  end

  test "infinite mode stops on cancellation and counts scans independently of events", %{
    tmp_dir: root
  } do
    coordinator = start_supervised!(Coordinator)
    scans = :atomics.new(1, signed: false)

    assert {:error, %Error{category: :interrupted}} =
             Watch.run(invocation(count: 0), target(), nil,
               coordinator: coordinator,
               sleep: fn _ -> :ok end,
               inventory: fn _, _, _ ->
                 if :atomics.add_get(scans, 1, 1) == 3, do: Coordinator.cancel(coordinator)
                 Manifest.build([entry("a")], tmp_dir: root)
               end
             )

    assert :atomics.get(scans, 1) == 3
    assert File.ls!(root) == []
  end

  test "default remote adapter completes all pages before observing removals", %{tmp_dir: root} do
    pages = :atomics.new(1, signed: false)

    assert :ok =
             Watch.run(
               invocation(),
               %Target{kind: :remote, bucket: "bucket", key: "prefix"},
               %Context{},
               tmp_dir: root,
               sleep: fn _ -> :ok end,
               fetch: fn "ListObjectsV2", input ->
                 assert input["Prefix"] == "prefix/"
                 page = :atomics.add_get(pages, 1, 1)
                 key = if page <= 2, do: "prefix/old", else: "prefix/new"

                 if input["ContinuationToken"],
                   do: {:ok, %{"Contents" => [], "IsTruncated" => false}},
                   else:
                     {:ok,
                      %{
                        "Contents" => [%{"Key" => key, "Size" => 1}],
                        "IsTruncated" => true,
                        "NextContinuationToken" => "next"
                      }}
               end,
               emit: fn event ->
                 assert :atomics.get(pages, 1) == 4
                 send(self(), {:event, event})
                 :ok
               end
             )

    assert_receive {:event, %{"Event" => "create", "Name" => "new"}}
    assert_receive {:event, %{"Event" => "remove", "Name" => "old"}}
    assert File.ls!(root) == []
  end

  test "large snapshots stream events without retaining event lists or more than two inventories",
       %{tmp_dir: root} do
    entries = fn size ->
      Stream.map(1..12_000, fn n -> entry(String.pad_leading(to_string(n), 5, "0"), size) end)
    end

    inventory = sequence([entries.(1), entries.(2)], root)
    events = :atomics.new(1, signed: false)

    assert :ok =
             Watch.run(invocation(), target(), nil,
               inventory: inventory,
               sleep: fn _ -> :ok end,
               emit: fn %{"Event" => "modify"} ->
                 if rem(:atomics.add_get(events, 1, 1), 1000) == 0,
                   do: assert(length(File.ls!(root)) == 2)

                 :ok
               end
             )

    assert :atomics.get(events, 1) == 12_000
    assert File.ls!(root) == []
  end

  defp target, do: %Target{kind: :local, path: "unused"}
  defp entry(name, size \\ 1), do: %{"name" => name, "size" => size}

  defp invocation(options \\ []),
    do: %Invocation{
      command: ["watch"],
      options: Map.new([adapter: "polling", count: 1] ++ options)
    }

  defp sequence(snapshots, root, after_build \\ fn _, _ -> :ok end) do
    index = :atomics.new(1, signed: false)

    fn _, _, _ ->
      number = :atomics.add_get(index, 1, 1)

      case Enum.fetch!(snapshots, number - 1) do
        {:error, _} = error ->
          error

        entries ->
          with {:ok, manifest} <- Manifest.build(entries, tmp_dir: root) do
            after_build.(manifest, number)
            {:ok, manifest}
          end
      end
    end
  end
end
