# Minimal dependency reproduction; no applications, effects, storage or ESS fixtures start.
defmodule ESSIndexUpstreamProbe do
  alias Concord.StateMachine.Core
  alias Concord.StateMachine.Core.Context
  alias ViewstampedReplication.{Configuration, Log, LogEntry, Member, Protocol}
  alias ViewstampedReplication.Protocol.State

  def run(output) do
    before_apps = Application.started_applications() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    context = %Context{op_number: 1, timestamp_ms: 1_761_000_000_000}
    initial = Core.init()

    {:ok, direct, direct_changes} =
      Core.apply_with_changes(0, context, {:put, "target", "value"}, initial)

    specification = %{compare: [], success: [{:put, "target", "value", %{}}], failure: []}

    {{:ok, %{succeeded: true}}, transaction, transaction_changes} =
      Core.apply_with_changes(0, context, {:txn, specification}, initial)

    true = direct.store == transaction.store
    true = is_list(direct_changes)
    :all = transaction_changes

    rows =
      for count <- [1_000, 2_000, 4_000] do
        state = fixture(count)
        # Recovery populates the real derived request index; no forged index or fallback fixture.
        %{writer: requests} = state.applied_request_index
        ^count = map_size(requests)
        {^state, [{:read, :route, {:get, "constant"}}]} = read(state)

        samples =
          for _ <- 1..30 do
            {:reductions, before_reductions} = Process.info(self(), :reductions)
            {duration, result} = :timer.tc(fn -> read(state) end)
            {:reductions, after_reductions} = Process.info(self(), :reductions)
            {^state, [{:read, :route, {:get, "constant"}}]} = result
            %{microseconds: duration, reductions: after_reductions - before_reductions}
          end

        times = Enum.sort_by(samples, & &1.microseconds)
        reductions = Enum.sort(Enum.map(samples, & &1.reductions))

        %{
          retained_entries: count,
          samples: samples,
          p50_microseconds: Enum.at(times, 14).microseconds,
          p95_microseconds: Enum.at(times, 28).microseconds,
          p50_reductions: Enum.at(reductions, 14),
          p95_reductions: Enum.at(reductions, 28)
        }
      end

    after_apps = Application.started_applications() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    true = before_apps == after_apps

    false =
      Enum.any?(after_apps, &(&1 in [:viewstamped_replication, :concord, :ex_storage_service]))

    File.write!(
      output,
      JSON.encode!(%{
        status: "dependency_defects_reproduced",
        diagnostic_only: true,
        acceptance_promotion: false,
        applications_unchanged: true,
        otp_release: to_string(:erlang.system_info(:otp_release)),
        transaction_change_tracking: %{
          direct_changes: inspect(direct_changes),
          transaction_changes: "all",
          exact_store_equal: true
        },
        singleton_reads: rows
      }) <> "\n",
      [:exclusive]
    )
  end

  defp read(state), do: Protocol.step(state, {:read_request, :route, {:get, "constant"}})

  defp fixture(count) do
    configuration =
      Configuration.new!(
        group_id: :index_upstream_probe,
        replica_id: :replica,
        members: [%Member{id: :replica, endpoint: :pure_fixture}]
      )

    entries =
      for number <- 1..count do
        %LogEntry{
          view_number: 0,
          op_number: number,
          client_id: :writer,
          request_number: number,
          operation: {:put, "constant", <<0>>}
        }
      end

    log = %Log{base_op_number: 0, entries: entries}
    initial = State.new(configuration)

    {state, effects} =
      Protocol.step(
        initial,
        {:storage_recovered,
         %{
           replica_id: :replica,
           log: log,
           commit_number: count,
           applied_number: count,
           client_table: %{writer: %{request_number: count, status: :applied, result: :ok}},
           hard_state: %{view_number: 0, last_normal_view: 0, status: :normal}
         }}
      )

    :normal = state.status
    ^count = state.applied_number
    ^log = state.log

    [
      {:persist, {:hard_state, %{view_number: 0, status: :normal, last_normal_view: 0}}},
      {:schedule_timer, :heartbeat, 500, {:heartbeat, 0, 1}}
    ] = effects

    ^state = Protocol.assert_invariants!(state)
    state
  end
end

[output] = System.argv()
ESSIndexUpstreamProbe.run(output)
