defmodule ExStorageService.Cluster.MembershipTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.{Membership, Node, NodeRegistrar}
  alias ExStorageService.InstanceConfig
  alias ExStorageService.Metadata.Keys

  defmodule TestBackend do
    def start_link(opts \\ []) do
      Agent.start_link(fn ->
        %{
          records: %{},
          revision: 0,
          put_attempts: 0,
          failures_left: Keyword.get(opts, :failures, 0)
        }
      end)
    end

    def get(key, opts) do
      Agent.get(Keyword.fetch!(opts, :engine), fn state ->
        {:ok, Map.get(state.records, key)}
      end)
    end

    def put(key, value, opts) do
      update(key, value, opts)
    end

    def transaction(spec, opts) do
      Agent.get_and_update(Keyword.fetch!(opts, :engine), fn state ->
        state = %{state | put_attempts: state.put_attempts + 1}

        if state.failures_left > 0 do
          {{:error, :cluster_not_ready}, %{state | failures_left: state.failures_left - 1}}
        else
          if Enum.all?(spec.compare, &compare?(&1, state)) do
            state = Enum.reduce(spec.success, state, &apply_operation/2)
            {{:ok, %{succeeded: true}}, state}
          else
            {{:ok, %{succeeded: false}}, state}
          end
        end
      end)
    end

    defp update(key, value, opts) do
      Agent.get_and_update(Keyword.fetch!(opts, :engine), fn state ->
        if state.failures_left > 0 do
          {{:error, :cluster_not_ready}, %{state | failures_left: state.failures_left - 1}}
        else
          revision = state.revision + 1
          record = %{value: value, mod_revision: revision}

          {:ok,
           %{
             state
             | revision: revision,
               records: Map.put(state.records, key, record)
           }}
        end
      end)
    end

    defp compare?({:mod_revision, key, :==, expected}, state) do
      revision = state.records |> Map.get(key, %{mod_revision: 0}) |> Map.fetch!(:mod_revision)
      revision == expected
    end

    defp compare?({:field, key, fields, :==, expected}, state) do
      value =
        state.records
        |> Map.get(key, %{})
        |> Map.get(:value, %{})
        |> then(fn value -> Enum.reduce(fields, value, &Map.get(&2, &1)) end)

      case value do
        ^expected -> true
        _other -> false
      end
    end

    defp apply_operation({:put, key, value, _opts}, state) do
      revision = state.revision + 1

      %{
        state
        | revision: revision,
          records: Map.put(state.records, key, %{value: value, mod_revision: revision})
      }
    end

    def seed(engine, key, value) do
      put(key, value, engine: engine)
    end

    def put_attempts(engine), do: Agent.get(engine, & &1.put_attempts)
  end

  test "registration persists the complete local node generation" do
    {:ok, backend} = TestBackend.start_link()
    config = cluster_config()
    timestamp = "2026-07-27T00:00:00Z"

    assert :ok =
             Membership.register(config,
               backend: TestBackend,
               engine: backend,
               timestamp: timestamp
             )

    assert {:ok, %{value: node_record, mod_revision: 1}} =
             TestBackend.get(Keys.cluster_node("node-a"), engine: backend)

    assert node_record == config |> Node.from_config(timestamp: timestamp) |> Map.from_struct()
  end

  test "membership reads only exact configured IDs and retains revisions" do
    {:ok, backend} = TestBackend.start_link()
    config = cluster_config()

    assert :ok =
             TestBackend.seed(backend, Keys.cluster_node("node-a"), cluster_node("node-a"))

    assert :ok =
             TestBackend.seed(backend, Keys.cluster_node("node-c"), cluster_node("node-c"))

    assert :ok =
             TestBackend.seed(backend, Keys.cluster_node("node-x"), cluster_node("node-x"))

    assert {:ok,
            [
              %{node: %Node{node_id: "node-a"}, mod_revision: 1},
              %{node: %Node{node_id: "node-c"}, mod_revision: 2}
            ]} =
             Membership.members(config, backend: TestBackend, engine: backend)
  end

  test "routine registration preserves persisted control state" do
    {:ok, backend} = TestBackend.start_link()
    config = cluster_config()

    controlled = %{
      cluster_node("node-a")
      | enabled: false,
        draining: true,
        zone: "maintenance-rack",
        capacity: 42
    }

    assert :ok = TestBackend.seed(backend, Keys.cluster_node("node-a"), controlled)
    assert :ok = Membership.register(config, backend: TestBackend, engine: backend)

    assert {:ok, %{value: registered}} =
             TestBackend.get(Keys.cluster_node("node-a"), engine: backend)

    assert {:ok,
            %Node{
              generation: 3,
              enabled: false,
              draining: true,
              zone: "maintenance-rack",
              capacity: 42
            }} = Node.cast(registered)
  end

  test "registration replaces control state only when explicitly requested" do
    {:ok, backend} = TestBackend.start_link()
    config = cluster_config()
    controlled = %{cluster_node("node-a") | enabled: false, draining: true}

    assert :ok = TestBackend.seed(backend, Keys.cluster_node("node-a"), controlled)

    assert :ok =
             Membership.register(config,
               backend: TestBackend,
               engine: backend,
               replace_control_state: true
             )

    assert {:ok, %{value: %{enabled: true, draining: false}}} =
             TestBackend.get(Keys.cluster_node("node-a"), engine: backend)
  end

  test "drain control is an idempotent compare-and-swap update" do
    {:ok, backend} = TestBackend.start_link()
    config = cluster_config()
    current = %{cluster_node("node-a") | generation: config.node_generation}

    assert :ok = TestBackend.seed(backend, Keys.cluster_node("node-a"), current)

    assert {:ok, %{node: %Node{draining: true}}} =
             Membership.set_draining(config, "node-a", true,
               backend: TestBackend,
               engine: backend,
               timestamp: "2026-07-29T00:00:00Z"
             )

    attempts = TestBackend.put_attempts(backend)

    assert {:ok, %{node: %Node{draining: true}}} =
             Membership.set_draining(config, "node-a", true,
               backend: TestBackend,
               engine: backend,
               timestamp: "2026-07-29T00:00:01Z"
             )

    assert TestBackend.put_attempts(backend) == attempts
  end

  test "metadata-only voters cannot enter the data-node drain workflow" do
    {:ok, backend} = TestBackend.start_link()
    config = cluster_config()
    metadata_node = %{cluster_node("node-c") | role: :metadata, internal_endpoint: nil}

    assert :ok = TestBackend.seed(backend, Keys.cluster_node("node-c"), metadata_node)

    assert {:error, :metadata_node_cannot_drain} =
             Membership.set_draining(config, "node-c", true,
               backend: TestBackend,
               engine: backend
             )

    assert TestBackend.put_attempts(backend) == 0
  end

  test "membership rejects a configured ID with a different fixed Erlang endpoint" do
    {:ok, backend} = TestBackend.start_link()
    config = cluster_config()

    mismatched = %{cluster_node("node-a") | erlang_endpoint: :"other@127.0.0.1"}
    assert :ok = TestBackend.seed(backend, Keys.cluster_node("node-a"), mismatched)

    assert {:error, {:cluster_node_endpoint_mismatch, "node-a"}} =
             Membership.members(config, backend: TestBackend, engine: backend)
  end

  test "membership rejects duplicate internal transport endpoints" do
    {:ok, backend} = TestBackend.start_link()
    config = cluster_config()
    node_a = cluster_node("node-a")
    node_b = %{cluster_node("node-b") | internal_endpoint: node_a.internal_endpoint}

    assert :ok = TestBackend.seed(backend, Keys.cluster_node("node-a"), node_a)
    assert :ok = TestBackend.seed(backend, Keys.cluster_node("node-b"), node_b)

    assert {:error,
            {:duplicate_cluster_node_endpoint, :internal_endpoint, "http://node-a.internal:9100"}} =
             Membership.members(config, backend: TestBackend, engine: backend)
  end

  test "registrar retries until registration succeeds and then emits no heartbeat" do
    {:ok, backend} = TestBackend.start_link(failures: 2)

    registrar =
      start_supervised!(
        {NodeRegistrar,
         config: cluster_config(),
         backend: TestBackend,
         engine: backend,
         retry_interval: 10,
         timestamp: "2026-07-27T00:00:00Z",
         name: nil}
      )

    assert eventually(fn -> NodeRegistrar.registered?(registrar) end)
    assert TestBackend.put_attempts(backend) == 3

    Process.sleep(40)
    assert TestBackend.put_attempts(backend) == 3
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(5)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false

  defp cluster_config do
    assert {:ok, config} =
             InstanceConfig.new(
               mode: :cluster,
               node_id: "node-a",
               node_generation: 3,
               node_zone: "rack-a",
               node_capacity: 1_000_000,
               cluster_name: "ess-test",
               cluster_topology: :static,
               cluster_members: [
                 %{id: "node-a", endpoint: :"node-a@127.0.0.1"},
                 %{id: "node-b", endpoint: :"node-b@127.0.0.1"},
                 %{id: "node-c", endpoint: :"node-c@127.0.0.1"}
               ],
               cluster_seeds: [:"node-b@127.0.0.1", :"node-c@127.0.0.1"],
               erlang_node: :"node-a@127.0.0.1",
               erlang_cookie: :ess_test_cookie,
               internal_advertised_url: "http://node-a.internal:9100",
               public_s3_enabled: false,
               web_enabled: false
             )

    config
  end

  defp cluster_node(node_id) do
    %Node{
      schema: 2,
      node_id: node_id,
      generation: 1,
      role: :data,
      erlang_endpoint: :"#{node_id}@127.0.0.1",
      internal_endpoint: "http://#{node_id}.internal:9100",
      enabled: true,
      draining: false,
      zone: nil,
      capacity: nil,
      updated_at: "2026-07-27T00:00:00Z"
    }
  end
end
