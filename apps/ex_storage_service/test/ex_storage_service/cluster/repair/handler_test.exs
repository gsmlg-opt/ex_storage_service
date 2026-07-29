defmodule ExStorageService.Cluster.Repair.HandlerTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.Source
  alias ExStorageService.Cluster.{Node, ReplicaAck, Repair.Handler}
  alias ExStorageService.Metadata.Models.{Blob, BlobLocation, Job}
  alias ExStorageService.{Context, InstanceConfig}

  defmodule CatalogDouble do
    def get(hash, opts) do
      descriptor = Agent.get(opts[:engine], & &1.descriptor)
      {:ok, %{key: "blob:#{hash}", descriptor: descriptor, mod_revision: 1}}
    end
  end

  defmodule MembershipDouble do
    def members(_config, opts), do: {:ok, Agent.get(opts[:engine], & &1.members)}
  end

  defmodule LocationsDouble do
    def list(_hash, opts), do: {:ok, Agent.get(opts[:engine], & &1.locations)}

    def mark_ready(hash, node_id, generation, size, opts) do
      send(Agent.get(opts[:engine], & &1.owner), {:marked_ready, node_id})

      Agent.update(opts[:engine], fn state ->
        location = location(hash, node_id, generation, size, :ready)
        %{state | locations: replace(state.locations, location)}
      end)
    end

    def mark_draining(_hash, node_id, opts) do
      send(Agent.get(opts[:engine], & &1.owner), {:marked_draining, node_id})

      Agent.update(opts[:engine], fn state ->
        locations =
          Enum.map(state.locations, fn
            %{location: %{node_id: ^node_id} = current} = record ->
              %{record | location: %{current | state: :draining}}

            record ->
              record
          end)

        %{state | locations: locations}
      end)
    end

    def mark_unhealthy(_hash, node_id, state, reason, opts) do
      send(Agent.get(opts[:engine], & &1.owner), {:marked_unhealthy, node_id, state, reason})

      Agent.update(opts[:engine], fn current ->
        locations =
          Enum.map(current.locations, fn
            %{location: %{node_id: ^node_id} = location} = record ->
              %{record | location: %{location | state: state, last_error: reason}}

            record ->
              record
          end)

        %{current | locations: locations}
      end)
    end

    def retire(_hash, node_id, _retained, opts) do
      send(Agent.get(opts[:engine], & &1.owner), {:retired, node_id})

      Agent.update(opts[:engine], fn state ->
        %{state | locations: Enum.reject(state.locations, &(&1.location.node_id == node_id))}
      end)
    end

    def authorize_cleanup(
          _hash,
          _node_id,
          _target_generation,
          _job_id,
          _owner,
          _generation,
          _token,
          _opts
        ),
        do: :ok

    defp replace(locations, %{location: new_location} = replacement) do
      locations
      |> Enum.reject(&(&1.location.node_id == new_location.node_id))
      |> Kernel.++([replacement])
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
  end

  defmodule ScrubberDouble do
    def scrub(_hash, opts), do: Agent.get(opts[:engine], & &1.scrub_result)
  end

  defmodule TransportDouble do
    def open_blob(_context, node, _hash, _range, opts) do
      state = Agent.get(opts[:engine], & &1)
      send(state.owner, {:opened, node.node_id})
      {:ok, Source.stateful_stream(producer(state.body), byte_size(state.body))}
    end

    def put_blob(_context, node, source, descriptor, opts) do
      attempt =
        Agent.get_and_update(opts[:engine], fn state ->
          next = state.transfer_attempts + 1
          {next, %{state | transfer_attempts: next}}
        end)

      if attempt == 1 and Agent.get(opts[:engine], & &1.interrupt_first) do
        {:error, :interrupted_copy}
      else
        with {:ok, body} <- collect(source),
             true <- sha256(body) == descriptor.hash do
          {:ok,
           %ReplicaAck{
             node_id: node.node_id,
             node_generation: node.generation,
             hash: descriptor.hash,
             size: byte_size(body),
             verified_at: 1,
             fencing_or_request_id: opts[:request_id]
           }}
        else
          _ -> {:error, :invalid_copy}
        end
      end
    end

    def delete_blob(_context, node, _hash, opts) do
      send(
        Agent.get(opts[:engine], & &1.owner),
        {:deleted, node.node_id, opts[:cleanup_job_id], opts[:cleanup_fencing_token]}
      )

      :ok
    end

    defp producer(body) do
      fn initial, reducer ->
        body
        |> :binary.bin_to_list()
        |> Enum.chunk_every(7)
        |> Enum.map(&:erlang.list_to_binary/1)
        |> Enum.reduce_while({:ok, initial}, fn chunk, {:ok, state} ->
          case reducer.(chunk, state) do
            {:cont, next} -> {:cont, {:ok, next}}
            {:halt, reason, next} -> {:halt, {:error, reason, next}}
          end
        end)
      end
    end

    defp collect(source) do
      case Source.reduce(source, [], fn chunk, chunks -> {:cont, [chunk | chunks]} end) do
        {:ok, chunks} -> {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}
        {:error, reason, _chunks} -> {:error, reason}
      end
    end

    defp sha256(value),
      do: :sha256 |> :crypto.hash(value) |> Base.encode16(case: :lower)
  end

  test "interrupted repair retries from an excess draining source and marks ready once" do
    body = "bounded-stateful-repair-source"
    hash = sha256(body)
    members = [member("node-a", draining: true), member("node-b"), member("node-c")]
    descriptor = blob(hash, byte_size(body), 2)
    owner = self()

    {:ok, engine} =
      Agent.start_link(fn ->
        %{
          owner: owner,
          body: body,
          descriptor: descriptor,
          members: members,
          locations: [location(hash, "node-a", :ready, byte_size(body))],
          interrupt_first: true,
          transfer_attempts: 0
        }
      end)

    context = context("node-c")
    job = job(:repair_blob, hash, "node-b", ["node-a"])
    opts = opts(engine)

    assert {:error, :interrupted_copy} = Handler.perform(job, context, opts)
    refute_receive {:marked_ready, _node_id}

    assert :ok = Handler.perform(job, context, opts)
    assert_receive {:opened, "node-a"}
    assert_receive {:opened, "node-a"}
    assert_receive {:marked_ready, "node-b"}
    refute_receive {:marked_ready, "node-b"}
  end

  test "cleanup does not retire a draining source until replacement RF is confirmed" do
    body = "drain-rf"
    hash = sha256(body)
    members = [member("node-a", draining: true), member("node-b"), member("node-c")]
    descriptor = blob(hash, byte_size(body), 2)
    owner = self()

    {:ok, engine} =
      Agent.start_link(fn ->
        %{
          owner: owner,
          body: body,
          descriptor: descriptor,
          members: members,
          locations: [
            location(hash, "node-a", :ready, byte_size(body)),
            location(hash, "node-b", :ready, byte_size(body))
          ],
          interrupt_first: false,
          transfer_attempts: 0
        }
      end)

    context = context("node-b")
    job = job(:cleanup, hash, "node-a", [])
    opts = opts(engine)

    assert {:error, :replication_factor_not_confirmed} = Handler.perform(job, context, opts)
    refute_receive {:deleted, "node-a", _job_id, _token}
    refute_receive {:retired, "node-a"}

    Agent.update(engine, fn state ->
      %{state | locations: [location(hash, "node-c", :ready, byte_size(body)) | state.locations]}
    end)

    assert :ok = Handler.perform(job, context, opts)
    assert_receive {:marked_draining, "node-a"}
    assert_receive {:deleted, "node-a", "job-cleanup", 1}
    assert_receive {:retired, "node-a"}
  end

  test "cleanup retires stale-generation metadata without deleting current-generation bytes" do
    body = "stale-generation-drain"
    hash = sha256(body)

    members = [
      member("node-a", draining: true, generation: 2),
      member("node-b"),
      member("node-c")
    ]

    descriptor = blob(hash, byte_size(body), 2)
    owner = self()

    {:ok, engine} =
      Agent.start_link(fn ->
        %{
          owner: owner,
          body: body,
          descriptor: descriptor,
          members: members,
          locations: [
            location(hash, "node-a", :suspect, byte_size(body), 1),
            location(hash, "node-b", :ready, byte_size(body)),
            location(hash, "node-c", :ready, byte_size(body))
          ],
          interrupt_first: false,
          transfer_attempts: 0
        }
      end)

    assert :ok =
             Handler.perform(
               job(:cleanup, hash, "node-a", []),
               context("node-b"),
               opts(engine)
             )

    assert_receive {:marked_draining, "node-a"}
    assert_receive {:retired, "node-a"}
    refute_receive {:deleted, "node-a", _job_id, _token}
  end

  test "checksum corruption is fenced, marked suspect, and handed to repair without head changes" do
    body = "corrupt-replica"
    hash = sha256(body)
    descriptor = blob(hash, byte_size(body), 2)
    owner = self()

    {:ok, engine} =
      Agent.start_link(fn ->
        %{
          owner: owner,
          body: body,
          descriptor: descriptor,
          members: [member("node-a"), member("node-b")],
          locations: [
            location(hash, "node-a", :ready, byte_size(body)),
            location(hash, "node-b", :ready, byte_size(body))
          ],
          head: %{version_id: "unchanged"},
          scrub_result: {:error, :checksum_mismatch},
          interrupt_first: false,
          transfer_attempts: 0
        }
      end)

    job = job(:scrub, hash, "node-a", [])

    assert :ok =
             Handler.perform(
               job,
               context("node-a"),
               opts(engine) ++ [scrubber: ScrubberDouble, scrubber_opts: [engine: engine]]
             )

    assert_receive {:marked_unhealthy, "node-a", :suspect, :checksum_mismatch}
    refute_receive {:marked_ready, _node_id}

    assert :ok =
             Handler.perform(
               job(:repair_blob, hash, "node-a", ["node-b"]),
               context("node-c"),
               opts(engine)
             )

    assert_receive {:opened, "node-b"}
    assert_receive {:marked_ready, "node-a"}

    assert %{descriptor: ^descriptor, head: %{version_id: "unchanged"}} =
             Agent.get(engine, & &1)
  end

  defp opts(engine) do
    [
      catalog: CatalogDouble,
      membership: MembershipDouble,
      locations: LocationsDouble,
      transport: TransportDouble,
      backend: CatalogDouble,
      engine: engine,
      transport_opts: [engine: engine]
    ]
  end

  defp context(node_id) do
    {:ok, config} = InstanceConfig.new(auto_start: false)
    Context.new(%{config | mode: :cluster, node_id: node_id, node_generation: 1})
  end

  defp job(kind, hash, target, sources) do
    {:ok, pending} =
      Job.new(
        "operation-#{kind}",
        %{
          id: "job-#{kind}",
          kind: kind,
          state: :pending,
          payload: %{
            hash: hash,
            target_node_id: target,
            target_node_generation: 1,
            source_node_ids: sources
          }
        },
        0
      )

    %{
      pending
      | state: :running,
        owner_node: "worker",
        owner_generation: 1,
        lease_until_ms: System.system_time(:millisecond) + 60_000,
        fencing_token: 1
    }
  end

  defp blob(hash, size, replication_factor) do
    %Blob{
      hash: hash,
      size: size,
      created_at: "2026-07-29T00:00:00Z",
      desired_replication_factor: replication_factor
    }
  end

  defp location(hash, node_id, state, size, generation \\ 1) do
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

  defp member(node_id, opts \\ []) do
    %{
      mod_revision: :erlang.phash2(node_id),
      node: %Node{
        schema: 2,
        node_id: node_id,
        generation: Keyword.get(opts, :generation, 1),
        role: :data,
        erlang_endpoint: :"#{node_id}@127.0.0.1",
        internal_endpoint: "http://#{node_id}.internal:9100",
        enabled: true,
        draining: Keyword.get(opts, :draining, false),
        zone: nil,
        capacity: nil,
        updated_at: "2026-07-29T00:00:00Z"
      }
    }
  end

  defp sha256(value),
    do: :sha256 |> :crypto.hash(value) |> Base.encode16(case: :lower)
end
