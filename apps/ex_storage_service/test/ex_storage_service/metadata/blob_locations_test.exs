defmodule ExStorageService.Metadata.BlobLocationsTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.BlobLocations
  alias ExStorageService.Metadata.Keys

  defmodule Backend do
    use Agent
    @behaviour ExStorageService.Metadata.Backend

    def start_link(opts) do
      initial =
        opts
        |> Keyword.get(:entries, [])
        |> Enum.with_index(1)
        |> Map.new(fn {{key, value}, revision} ->
          {key, %{value: value, mod_revision: revision}}
        end)

      Agent.start_link(fn ->
        %{
          entries: initial,
          revision: map_size(initial),
          transactions: [],
          compare_failures: Keyword.get(opts, :compare_failures, 0)
        }
      end)
    end

    @impl true
    def get(key, opts),
      do: Agent.get(engine(opts), &{:ok, Map.get(&1.entries, key)})

    @impl true
    def put(key, value, opts) do
      Agent.update(engine(opts), &put_entry(&1, key, value))
      :ok
    end

    @impl true
    def delete(key, opts) do
      Agent.update(
        engine(opts),
        &update_in(&1.entries, fn entries -> Map.delete(entries, key) end)
      )

      :ok
    end

    @impl true
    def get_all(opts),
      do: {:ok, entries(engine(opts))}

    @impl true
    def prefix_scan(prefix, opts),
      do: {:ok, entries(engine(opts)) |> Enum.filter(&(elem(&1, 0) =~ prefix))}

    @impl true
    def scan(prefix, opts), do: prefix_scan(prefix, opts)

    @impl true
    def transaction(spec, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        state = update_in(state.transactions, &[spec | &1])

        if state.compare_failures > 0 do
          {{:ok, %{succeeded: false}}, %{state | compare_failures: state.compare_failures - 1}}
        else
          state = Enum.reduce(spec.success, state, &apply_operation/2)
          {{:ok, %{succeeded: true}}, state}
        end
      end)
    end

    @impl true
    def resolve_transaction(_key, _opts), do: {:error, :not_found}

    @impl true
    def resolve_operation(key, opts), do: get(key, opts)

    def snapshot(engine), do: Agent.get(engine, & &1)

    defp apply_operation({:put, key, value, _opts}, state), do: put_entry(state, key, value)

    defp apply_operation({:delete, key, _opts}, state),
      do: update_in(state.entries, &Map.delete(&1, key))

    defp apply_operation({:get, _range, _opts}, state), do: state

    defp put_entry(state, key, value) do
      revision = state.revision + 1

      %{
        state
        | revision: revision,
          entries: Map.put(state.entries, key, %{value: value, mod_revision: revision})
      }
    end

    defp entries(engine) do
      Agent.get(engine, fn state ->
        state.entries
        |> Enum.map(fn {key, record} -> {key, record.value} end)
        |> Enum.sort()
      end)
    end

    defp engine(opts), do: Keyword.fetch!(opts, :engine)
  end

  test "strong prefix reads return typed, deterministic location records" do
    hash = String.duplicate("a", 64)

    engine =
      start_supervised!(
        {Backend,
         entries: [
           {Keys.blob_location(hash, "node-b"), location(hash, "node-b")},
           {Keys.blob_location(hash, "node-a"), location(hash, "node-a")}
         ]}
      )

    assert {:ok, records} = BlobLocations.list(hash, backend: Backend, engine: engine)
    assert Enum.map(records, & &1.location.node_id) == ["node-a", "node-b"]
  end

  test "suspect transition and repair intent are one atomic transaction" do
    hash = String.duplicate("b", 64)
    key = Keys.blob_location(hash, "node-a")
    engine = start_supervised!({Backend, entries: [{key, location(hash, "node-a")}]})

    assert :ok =
             BlobLocations.mark_unhealthy(
               hash,
               "node-a",
               :suspect,
               :checksum_mismatch,
               backend: Backend,
               engine: engine,
               node_record: %{
                 node: %{node_id: "node-a", generation: 1},
                 mod_revision: 99
               },
               expected_generation: 1,
               timestamp: "2026-07-28T00:00:00Z"
             )

    snapshot = Backend.snapshot(engine)
    assert snapshot.entries[key].value.state == :suspect
    assert snapshot.entries[key].value.last_error == :checksum_mismatch

    [{outbox_key, outbox}] =
      snapshot.entries
      |> Enum.filter(fn {entry_key, _record} ->
        String.starts_with?(entry_key, "ess:v2:outbox:")
      end)

    assert outbox.value.events == [
             %{
               id: outbox.value.events |> hd() |> Map.fetch!(:id),
               kind: :repair_blob,
               state: :pending,
               payload: %{hash: hash, target_node_id: "node-a", source_node_ids: []}
             }
           ]

    [spec] = snapshot.transactions
    success_keys = Enum.map(spec.success, &elem(&1, 1))
    assert key in success_keys
    assert outbox_key in success_keys
    assert {:mod_revision, Keys.cluster_node("node-a"), :==, 99} in spec.compare
    assert {:field, Keys.cluster_node("node-a"), [:generation], :==, 1} in spec.compare
  end

  test "compare failure rereads and retries the transition" do
    hash = String.duplicate("c", 64)
    key = Keys.blob_location(hash, "node-a")

    engine =
      start_supervised!(
        {Backend, entries: [{key, location(hash, "node-a")}], compare_failures: 1}
      )

    assert :ok =
             BlobLocations.mark_unhealthy(hash, "node-a", :unavailable, :not_found,
               backend: Backend,
               engine: engine
             )

    snapshot = Backend.snapshot(engine)
    assert snapshot.entries[key].value.state == :unavailable
    assert length(snapshot.transactions) == 2
  end

  test "completed read repair publishes the current generation as ready" do
    hash = String.duplicate("d", 64)
    key = Keys.blob_location(hash, "node-a")

    initial =
      location(hash, "node-a")
      |> Map.merge(%{state: :suspect, node_generation: 1, last_error: :checksum_mismatch})

    engine = start_supervised!({Backend, entries: [{key, initial}]})

    assert :ok =
             BlobLocations.mark_ready(hash, "node-a", 2, 42,
               backend: Backend,
               engine: engine,
               timestamp: "2026-07-28T00:00:00Z"
             )

    ready = Backend.snapshot(engine).entries[key].value
    assert ready.state == :ready
    assert ready.node_generation == 2
    assert ready.size == 42
    assert is_nil(ready.last_error)
  end

  test "completed read repair creates an absent location with compare revision zero" do
    hash = String.duplicate("e", 64)
    key = Keys.blob_location(hash, "node-a")
    engine = start_supervised!({Backend, entries: []})

    assert :ok =
             BlobLocations.mark_ready(hash, "node-a", 3, 42,
               backend: Backend,
               engine: engine,
               timestamp: "2026-07-28T00:00:00Z"
             )

    snapshot = Backend.snapshot(engine)
    assert snapshot.entries[key].value.state == :ready
    assert snapshot.entries[key].value.node_generation == 3

    [spec] = snapshot.transactions
    assert {:mod_revision, key, :==, 0} in spec.compare
  end

  test "cleanup preparation and authorization persist and revalidate the durable job fence" do
    hash = String.duplicate("f", 64)
    key = Keys.blob_location(hash, "node-old")
    retained_key = Keys.blob_location(hash, "node-b")

    engine =
      start_supervised!(
        {Backend,
         entries: [
           {key, location(hash, "node-old")},
           {retained_key, location(hash, "node-b")},
           {Keys.job("cleanup-job"),
            %{
              state: :running,
              owner_node: "worker-a",
              owner_generation: 4,
              fencing_token: 9,
              lease_until_ms: 1_000
            }}
         ]}
      )

    job = %{
      job_id: "cleanup-job",
      owner_node: "worker-a",
      owner_generation: 4,
      fencing_token: 9
    }

    retained = [
      %{
        key: retained_key,
        mod_revision: 2,
        location:
          struct!(
            ExStorageService.Metadata.Models.BlobLocation,
            location(hash, "node-b")
          )
      }
    ]

    desired = [
      %{
        mod_revision: 17,
        node: %{
          node_id: "node-b",
          generation: 1,
          role: :data,
          enabled: true,
          draining: false
        }
      }
    ]

    descriptor_record = %{
      mod_revision: 23,
      descriptor: %{desired_replication_factor: 1}
    }

    assert :ok =
             BlobLocations.mark_draining(hash, "node-old",
               backend: Backend,
               engine: engine,
               retained_locations: retained,
               desired_members: desired,
               descriptor_record: descriptor_record,
               job_fence: job,
               now_ms: 100
             )

    prepared = Backend.snapshot(engine).entries[key].value
    assert prepared.state == :draining
    assert prepared.cleanup_job_id == "cleanup-job"
    assert prepared.cleanup_owner_node == "worker-a"
    assert prepared.cleanup_owner_generation == 4
    assert prepared.cleanup_fencing_token == 9
    assert prepared.cleanup_descriptor_revision == 23
    assert prepared.cleanup_replication_factor == 1

    assert :ok =
             BlobLocations.authorize_cleanup(
               hash,
               "node-old",
               1,
               "cleanup-job",
               "worker-a",
               4,
               9,
               backend: Backend,
               engine: engine,
               now_ms: 100
             )

    [authorize_spec, prepare_spec] = Backend.snapshot(engine).transactions

    assert [
             {:put, job_key, %{lease_until_ms: 30_100}, %{}},
             {:put, ^key, %{state: :deleting}, %{}}
           ] = authorize_spec.success

    assert job_key == Keys.job("cleanup-job")
    assert {:field, key, [:cleanup_fencing_token], :==, 9} in authorize_spec.compare
    assert {:field, retained_key, [:state], :==, :ready} in authorize_spec.compare
    assert {:field, Keys.cluster_node("node-b"), [:generation], :==, 1} in authorize_spec.compare
    assert {:mod_revision, Keys.blob(hash), :==, 23} in authorize_spec.compare

    assert {:field, Keys.blob(hash), [:desired_replication_factor], :==, 1} in authorize_spec.compare

    assert {:field, Keys.job("cleanup-job"), [:lease_until_ms], :>, 100} in authorize_spec.compare
    assert {:field, Keys.job("cleanup-job"), [:fencing_token], :==, 9} in prepare_spec.compare
    assert {:mod_revision, Keys.blob(hash), :==, 23} in prepare_spec.compare

    assert {:error, :cleanup_in_progress} =
             BlobLocations.mark_ready(hash, "node-old", 1, 42,
               backend: Backend,
               engine: engine,
               timestamp: "2026-07-28T00:00:01Z"
             )

    assert :ok =
             BlobLocations.retire(hash, "node-old", retained,
               backend: Backend,
               engine: engine,
               desired_members: desired,
               job_fence: job,
               now_ms: 100,
               timestamp: "2026-07-28T00:00:02Z"
             )

    assert {:ok, %{value: tombstone, mod_revision: tombstone_revision}} =
             Backend.get(key, engine: engine)

    assert tombstone.state == :absent
    assert tombstone.retired_at_ms == 100

    assert {:error, :blob_location_absent} =
             BlobLocations.mark_ready(hash, "node-old", 1, 42,
               backend: Backend,
               engine: engine,
               timestamp: "2026-07-28T00:00:03Z"
             )

    assert :ok =
             BlobLocations.mark_ready(hash, "node-old", 1, 42,
               backend: Backend,
               engine: engine,
               resurrect_absent: true,
               expected_location_revision: tombstone_revision,
               timestamp: "2026-07-28T00:00:04Z"
             )

    assert Backend.snapshot(engine).entries[key].value.state == :ready

    assert {:error, :stale_cleanup_fence} =
             BlobLocations.authorize_cleanup(
               hash,
               "node-old",
               1,
               "cleanup-job",
               "worker-a",
               4,
               9,
               backend: Backend,
               engine: engine,
               now_ms: 30_100
             )
  end

  defp location(hash, node_id) do
    %{
      schema: 2,
      hash: hash,
      node_id: node_id,
      node_generation: 1,
      state: :ready,
      size: 42,
      verified_at: "2026-07-28T00:00:00Z"
    }
  end
end
