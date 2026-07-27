defmodule ExStorageService.Cluster.ReadCoordinatorTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.Source
  alias ExStorageService.Cluster.{Node, ReadCoordinator}
  alias ExStorageService.Metadata.Models.BlobLocation
  alias ExStorageService.{Context, InstanceConfig}

  defmodule BlobStoreDouble do
    def stat(_hash, opts), do: state(opts).local_stat

    def verify(_hash, opts) do
      Agent.get(engine(opts), fn state ->
        send(state.test_pid, :local_verify)
        state.local_verify
      end)
    end

    def open(_hash, range, opts) do
      Agent.get(engine(opts), fn state ->
        send(state.test_pid, {:local_open, range})
        state.local_open
      end)
    end

    defp state(opts), do: Agent.get(engine(opts), & &1)
    defp engine(opts), do: Keyword.fetch!(opts, :engine)
  end

  defmodule LocationsDouble do
    def list(_hash, opts), do: {:ok, Agent.get(engine(opts), & &1.locations)}

    def mark_unhealthy(hash, node_id, state, reason, opts) do
      Agent.update(engine(opts), fn current ->
        send(current.test_pid, {:location_unhealthy, hash, node_id, state, reason})
        current
      end)
    end

    def mark_ready(hash, node_id, generation, size, opts) do
      Agent.update(engine(opts), fn current ->
        send(current.test_pid, {:location_ready, hash, node_id, generation, size})
        current
      end)
    end

    defp engine(opts), do: Keyword.fetch!(opts, :engine)
  end

  defmodule TransportDouble do
    def open_blob(_context, node, hash, range, opts) do
      Agent.get_and_update(Keyword.fetch!(opts, :engine), fn state ->
        send(state.test_pid, {:remote_open, node.node_id, hash, range, opts})
        result = Map.fetch!(state.remote_results, node.node_id)
        result = if is_function(result, 1), do: result.(range), else: result
        {result, state}
      end)
    end
  end

  defmodule PlacementDouble do
    def select(_hash, nodes, replication_factor) do
      selected =
        nodes
        |> Enum.sort_by(& &1.node_id)
        |> Enum.take(replication_factor)

      {:ok, selected}
    end
  end

  defmodule ReadRepairDouble do
    def wrap(_source, _hash, size, _opts) do
      Source.stateful_stream(
        fn initial, _reducer -> {:error, :repair_wrapped, initial} end,
        size
      )
    end
  end

  @tag :tmp_dir
  test "prefers a checksum-valid local source without contacting a remote replica", %{
    tmp_dir: tmp_dir
  } do
    hash = sha256("local")
    path = Path.join(tmp_dir, "local")

    engine =
      start_engine(%{
        local_stat: {:ok, %{size: 5}},
        local_verify: :ok,
        local_open: {:ok, Source.file(path, 0, 5)},
        locations: [location(hash, "node-a", 7, 5)],
        remote_results: %{}
      })

    assert {:ok, {:file, ^path, 0, 5}} =
             ReadCoordinator.open(context(tmp_dir), hash, 5, nil, opts(engine))

    assert_received {:local_open, nil}
    refute_received :local_verify
    refute_received {:remote_open, _, _, _, _}
    refute_received {:location_unhealthy, _, _, _, _}
  end

  @tag :tmp_dir
  test "an untracked v1-compatible local blob is verified before it is served", %{
    tmp_dir: tmp_dir
  } do
    hash = sha256("legacy")
    path = Path.join(tmp_dir, "legacy")

    engine =
      start_engine(%{
        local_stat: {:ok, %{size: 6}},
        local_verify: :ok,
        local_open: {:ok, Source.file(path, 0, 6)},
        locations: [],
        remote_results: %{}
      })

    assert {:ok, {:file, ^path, 0, 6}} =
             ReadCoordinator.open(context(tmp_dir), hash, 6, nil, opts(engine))

    assert_received :local_verify
    refute_received {:remote_open, _, _, _, _}
  end

  @tag :tmp_dir
  test "missing and corrupt local locations are marked before remote fallback", %{
    tmp_dir: tmp_dir
  } do
    hash = sha256("fallback")

    for {local_stat, local_verify, local_state, expected_state, expected_reason} <- [
          {{:error, :not_found}, :ok, :ready, :unavailable, :not_found},
          {{:ok, %{size: 8}}, {:error, :checksum_mismatch}, :suspect, :suspect,
           :checksum_mismatch}
        ] do
      engine =
        start_engine(%{
          local_stat: local_stat,
          local_verify: local_verify,
          local_open: {:error, :not_found},
          locations: [
            location(hash, "node-a", 7, 8, local_state),
            location(hash, "node-b", 9, 8)
          ],
          remote_results: %{
            "node-b" => {:ok, Source.stream(["fallback"], 8)}
          }
        })

      assert {:ok, {:stream, _producer, 8}} =
               ReadCoordinator.open(context(tmp_dir), hash, 8, nil, opts(engine))

      assert_received {:location_unhealthy, ^hash, "node-a", ^expected_state, ^expected_reason}
      assert_received {:remote_open, "node-b", ^hash, {0, 8}, _transport_opts}
    end
  end

  @tag :tmp_dir
  test "an old local generation cannot poison the current location record", %{tmp_dir: tmp_dir} do
    hash = sha256("generation-fence")

    engine =
      start_engine(%{
        local_stat: {:error, :not_found},
        local_verify: :ok,
        local_open: {:error, :not_found},
        locations: [
          location(hash, "node-a", 7, 16),
          location(hash, "node-b", 9, 16)
        ],
        remote_results: %{
          "node-b" => {:ok, Source.stream(["generation-fence"], 16)}
        }
      })

    current_members = [
      %{node: node("node-a", 8), mod_revision: 21},
      %{node: node("node-b", 9), mod_revision: 22}
    ]

    assert {:ok, _source} =
             ReadCoordinator.open(
               context(tmp_dir),
               hash,
               16,
               nil,
               opts(engine, placement_records: current_members)
             )

    refute_received {:location_unhealthy, ^hash, "node-a", _, _}
  end

  @tag :tmp_dir
  test "remote candidates use deterministic bounded retry order", %{tmp_dir: tmp_dir} do
    hash = sha256("retry")

    engine =
      start_engine(%{
        local_stat: {:error, :not_found},
        local_verify: :ok,
        local_open: {:error, :not_found},
        locations: [
          location(hash, "node-a", 7, 5),
          location(hash, "node-b", 9, 5),
          location(hash, "node-c", 4, 5)
        ],
        remote_results: %{
          "node-b" => {:error, :timeout},
          "node-c" => {:ok, Source.stream(["retry"], 5)}
        }
      })

    assert {:ok, {:stream, _producer, 5}} =
             ReadCoordinator.open(
               context(tmp_dir),
               hash,
               5,
               nil,
               opts(engine,
                 source_order: ["node-b", "node-c"],
                 max_remote_attempts: 2
               )
             )

    assert_received {:remote_open, "node-b", ^hash, {0, 5}, first_opts}
    assert_received {:remote_open, "node-c", ^hash, {0, 5}, second_opts}
    assert first_opts[:expected_node_generation] == 9
    assert second_opts[:expected_node_generation] == 4
    refute_received {:remote_open, _, _, _, _}
  end

  @tag :tmp_dir
  test "retries another ready replica when the first body fails before a byte", %{
    tmp_dir: tmp_dir
  } do
    hash = sha256("retry-body")

    failed_body =
      Source.stateful_stream(
        fn initial, _reducer -> {:error, :connection_reset, initial} end,
        10
      )

    engine =
      start_engine(%{
        local_stat: {:error, :not_found},
        local_verify: :ok,
        local_open: {:error, :not_found},
        locations: [
          location(hash, "node-b", 9, 10),
          location(hash, "node-c", 4, 10)
        ],
        remote_results: %{
          "node-b" => {:ok, failed_body},
          "node-c" => {:ok, Source.stream(["retry-body"], 10)}
        }
      })

    assert {:ok, source} =
             ReadCoordinator.open(
               context(tmp_dir),
               hash,
               10,
               nil,
               opts(engine, source_order: ["node-b", "node-c"])
             )

    assert {:ok, "retry-body"} =
             Source.reduce(source, "", fn chunk, body -> {:cont, body <> chunk} end)

    assert_received {:remote_open, "node-b", ^hash, {0, 10}, _opts}
    assert_received {:remote_open, "node-c", ^hash, {0, 10}, _opts}
  end

  @tag :tmp_dir
  test "all lazy body failures are unavailable before a source is returned", %{tmp_dir: tmp_dir} do
    hash = sha256("body-unavailable")

    failed_body =
      Source.stateful_stream(
        fn initial, _reducer -> {:error, :connection_reset, initial} end,
        16
      )

    engine =
      start_engine(%{
        local_stat: {:error, :not_found},
        local_verify: :ok,
        local_open: {:error, :not_found},
        locations: [
          location(hash, "node-b", 9, 16),
          location(hash, "node-c", 4, 16)
        ],
        remote_results: %{
          "node-b" => {:ok, failed_body},
          "node-c" => {:ok, failed_body}
        }
      })

    assert {:error, :all_blob_replicas_unavailable} =
             ReadCoordinator.open(
               context(tmp_dir),
               hash,
               16,
               nil,
               opts(engine, source_order: ["node-b", "node-c"])
             )

    assert_received {:remote_open, "node-b", ^hash, {0, 16}, _opts}
    assert_received {:remote_open, "node-c", ^hash, {0, 16}, _opts}
  end

  @tag :tmp_dir
  test "forwards the exact range and validates the returned source length", %{tmp_dir: tmp_dir} do
    hash = sha256("0123456789")

    engine =
      start_engine(%{
        local_stat: {:error, :not_found},
        local_verify: :ok,
        local_open: {:error, :not_found},
        locations: [location(hash, "node-b", 9, 10)],
        remote_results: %{
          "node-b" => {:ok, Source.stream(["2345"], 4)}
        }
      })

    assert {:ok, {:stream, _producer, 4}} =
             ReadCoordinator.open(
               context(tmp_dir),
               hash,
               10,
               {2, 4},
               opts(engine, source_order: ["node-b"])
             )

    assert_received {:remote_open, "node-b", ^hash, {2, 4}, transport_opts}
    assert transport_opts[:expected_size] == 10
  end

  @tag :tmp_dir
  test "prefetches only a bounded prefix before lazily streaming the remainder", %{
    tmp_dir: tmp_dir
  } do
    hash = sha256("0123456789")

    engine =
      start_engine(%{
        local_stat: {:error, :not_found},
        local_verify: :ok,
        local_open: {:error, :not_found},
        locations: [location(hash, "node-b", 9, 10)],
        remote_results: %{
          "node-b" => fn
            {0, 4} -> {:ok, Source.stream(["0123"], 4)}
            {4, 6} -> {:ok, Source.stream(["456789"], 6)}
          end
        }
      })

    assert {:ok, source} =
             ReadCoordinator.open(
               context(tmp_dir),
               hash,
               10,
               nil,
               opts(engine, source_order: ["node-b"], prefetch_bytes: 4)
             )

    assert_received {:remote_open, "node-b", ^hash, {0, 4}, _opts}
    refute_received {:remote_open, "node-b", ^hash, {4, 6}, _opts}

    assert {:ok, "0123456789"} =
             Source.reduce(source, "", fn chunk, body -> {:cont, body <> chunk} end)

    assert_received {:remote_open, "node-b", ^hash, {4, 6}, _opts}
  end

  @tag :tmp_dir
  test "caps and validates an injected prefetch size", %{tmp_dir: tmp_dir} do
    body = :binary.copy("x", 65_537)
    hash = sha256(body)

    engine =
      start_engine(%{
        local_stat: {:error, :not_found},
        local_verify: :ok,
        local_open: {:error, :not_found},
        locations: [location(hash, "node-b", 9, byte_size(body))],
        remote_results: %{
          "node-b" => fn
            {0, 65_536} -> {:ok, Source.stream([binary_part(body, 0, 65_536)], 65_536)}
            {65_536, 1} -> {:ok, Source.stream(["x"], 1)}
          end
        }
      })

    assert {:ok, _source} =
             ReadCoordinator.open(
               context(tmp_dir),
               hash,
               byte_size(body),
               nil,
               opts(engine, source_order: ["node-b"], prefetch_bytes: byte_size(body))
             )

    assert_received {:remote_open, "node-b", ^hash, {0, 65_536}, _opts}

    engine =
      start_engine(%{
        local_stat: {:error, :not_found},
        local_verify: :ok,
        local_open: {:error, :not_found},
        locations: [location(hash, "node-b", 9, 1)],
        remote_results: %{"node-b" => {:ok, Source.stream(["x"], 1)}}
      })

    assert {:ok, _source} =
             ReadCoordinator.open(
               context(tmp_dir),
               hash,
               1,
               nil,
               opts(engine, source_order: ["node-b"], prefetch_bytes: 0)
             )

    assert_received {:remote_open, "node-b", ^hash, {0, 1}, _opts}
  end

  @tag :tmp_dir
  test "returns storage-unavailable when every ready replica fails", %{tmp_dir: tmp_dir} do
    hash = sha256("unavailable")

    engine =
      start_engine(%{
        local_stat: {:error, :not_found},
        local_verify: :ok,
        local_open: {:error, :not_found},
        locations: [
          location(hash, "node-b", 9, 11),
          location(hash, "node-c", 4, 11)
        ],
        remote_results: %{
          "node-b" => {:error, :timeout},
          "node-c" => {:error, :not_found}
        }
      })

    assert {:error, :all_blob_replicas_unavailable} =
             ReadCoordinator.open(
               context(tmp_dir),
               hash,
               11,
               nil,
               opts(engine, source_order: ["node-b", "node-c"])
             )

    assert_received {:remote_open, "node-b", ^hash, {0, 11}, _opts}
    assert_received {:remote_open, "node-c", ^hash, {0, 11}, _opts}
    assert_received {:location_unhealthy, ^hash, "node-c", :unavailable, :not_found}
  end

  @tag :tmp_dir
  test "wraps full remote reads for repair but never repairs a partial range", %{
    tmp_dir: tmp_dir
  } do
    hash = sha256("repair-me")

    engine =
      start_engine(%{
        local_stat: {:error, :not_found},
        local_verify: :ok,
        local_open: {:error, :not_found},
        locations: [
          location(hash, "node-a", 7, 9),
          location(hash, "node-b", 9, 9)
        ],
        remote_results: %{
          "node-b" => {:ok, Source.stream(["repair-me"], 9)}
        }
      })

    read_opts =
      opts(engine,
        source_order: ["node-b"],
        placement: PlacementDouble,
        read_repair_module: ReadRepairDouble,
        read_repair: true
      )

    assert {:ok, full_source} =
             ReadCoordinator.open(context(tmp_dir), hash, 9, nil, read_opts)

    assert {:error, :repair_wrapped, :consumer} =
             Source.reduce(full_source, :consumer, fn _chunk, state -> {:cont, state} end)

    Agent.update(engine, fn state ->
      %{state | locations: [location(hash, "node-b", 9, 9)]}
    end)

    assert {:ok, absent_location_source} =
             ReadCoordinator.open(context(tmp_dir), hash, 9, nil, read_opts)

    assert {:error, :repair_wrapped, :consumer} =
             Source.reduce(absent_location_source, :consumer, fn _chunk, state ->
               {:cont, state}
             end)

    Agent.update(engine, fn state ->
      put_in(state.remote_results["node-b"], {:ok, Source.stream(["pair"], 4)})
    end)

    assert {:ok, range_source} =
             ReadCoordinator.open(context(tmp_dir), hash, 9, {2, 4}, read_opts)

    assert {:ok, "pair"} =
             Source.reduce(range_source, "", fn chunk, body -> {:cont, body <> chunk} end)
  end

  defp opts(engine, extra \\ []) do
    [
      engine: engine,
      locations: LocationsDouble,
      blob_store: BlobStoreDouble,
      blob_store_opts: [engine: engine],
      placement_records: member_records(),
      transport: TransportDouble,
      transport_opts: [engine: engine],
      read_repair: false
    ]
    |> Keyword.merge(extra)
  end

  defp start_engine(initial) do
    test_pid = self()

    start_supervised!(%{
      id: make_ref(),
      start: {Agent, :start_link, [fn -> Map.put(initial, :test_pid, test_pid) end]}
    })
  end

  defp context(tmp_dir) do
    members = [
      %{id: "node-a", endpoint: :"node-a@127.0.0.1"},
      %{id: "node-b", endpoint: :"node-b@127.0.0.1"},
      %{id: "node-c", endpoint: :"node-c@127.0.0.1"}
    ]

    {:ok, config} =
      InstanceConfig.new(
        mode: :cluster,
        node_role: :data,
        node_id: "node-a",
        node_generation: 7,
        cluster_name: "read-test",
        cluster_topology: :static,
        cluster_members: members,
        cluster_seeds: [:"node-b@127.0.0.1", :"node-c@127.0.0.1"],
        erlang_node: :"node-a@127.0.0.1",
        erlang_cookie: :read_test_cookie,
        internal_advertised_url: "http://node-a.internal:9100",
        public_s3_enabled: true,
        web_enabled: false,
        cluster_data_plane_enabled: true,
        replication_factor: 2,
        write_quorum: 2,
        data_root: Path.join(tmp_dir, "data"),
        blob_root: Path.join(tmp_dir, "blob"),
        tmp_root: Path.join(tmp_dir, "tmp")
      )

    Context.new(config)
  end

  defp member_records do
    [
      %{node: node("node-a", 7), mod_revision: 11},
      %{node: node("node-b", 9), mod_revision: 12},
      %{node: node("node-c", 4), mod_revision: 13}
    ]
  end

  defp node(id, generation) do
    %Node{
      schema: 2,
      node_id: id,
      generation: generation,
      role: :data,
      erlang_endpoint: String.to_atom("#{id}@127.0.0.1"),
      internal_endpoint: "http://#{id}.internal:9100",
      enabled: true,
      draining: false,
      zone: nil,
      capacity: nil,
      updated_at: "2026-07-28T00:00:00Z"
    }
  end

  defp location(hash, node_id, generation, size) do
    location(hash, node_id, generation, size, :ready)
  end

  defp location(hash, node_id, generation, size, state) do
    %{
      key: "location:#{node_id}",
      location: %BlobLocation{
        hash: hash,
        node_id: node_id,
        node_generation: generation,
        state: state,
        size: size,
        verified_at: 1
      }
    }
  end

  defp sha256(data), do: :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)
end
