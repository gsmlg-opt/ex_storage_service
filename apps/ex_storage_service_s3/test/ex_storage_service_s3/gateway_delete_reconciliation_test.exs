defmodule ExStorageServiceS3.GatewayDeleteReconciliationTest do
  use ExUnit.Case, async: false

  alias ExStorageService.CloudCache.{Config, DeleteThrough}
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.Backend.Concord, as: Backend

  defmodule Upstream do
    def init(agent), do: agent

    def call(conn, agent) do
      {status, headers} =
        Agent.get_and_update(agent, fn state ->
          event = {conn.method, Plug.Conn.get_req_header(conn, "if-match")}
          state = %{state | events: state.events ++ [event]}

          case {conn.method, state.present} do
            {"HEAD", true} -> {{200, [{"etag", "\"original\""}, {"content-length", "4"}]}, state}
            {"HEAD", false} -> {{404, []}, state}
            {"DELETE", _} -> {{204, []}, %{state | present: false}}
          end
        end)

      conn =
        Enum.reduce(headers, conn, fn {name, value}, conn ->
          Plug.Conn.put_resp_header(conn, name, value)
        end)

      Plug.Conn.send_resp(conn, status, "")
    end
  end

  setup do
    bucket = "delete-reconcile-#{System.unique_integer([:positive])}"
    operation = "delete-reconcile-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    :ok = Concord.put("obj:#{bucket}:key", %{etag: "original", cloud_backed: true})
    :ok = Concord.put("cloud_cache_index:#{bucket}", %{"key" => %{content_hash: "old", size: 4}})
    agent = start_supervised!({Agent, fn -> %{present: true, events: []} end})

    server =
      start_supervised!(
        {Gaoxy.HTTP, plug: {Upstream, agent}, port: 0, limits: %{max_bytes: 4_194_304}}
      )

    %{local: {:ok, {_, port}}} = Gaoxy.info(server, :http)

    :ok =
      Config.set_config(bucket, %{
        enabled: true,
        provider: :s3_compat,
        endpoint: "http://localhost:#{port}",
        bucket: "upstream",
        access_key_id: "test",
        secret_access_key: "test",
        cache_enabled: true
      })

    {:ok, config} = Config.get_active_config(bucket)

    on_exit(fn ->
      Concord.delete("obj:#{bucket}:key")
      Concord.delete("ess:gateway-delete:" <> Base.url_encode64(operation, padding: false))
      Config.delete_config(bucket)
      Metadata.delete_bucket(bucket)
    end)

    {:ok, bucket: bucket, operation: operation, config: config, agent: agent}
  end

  test "caller death after upstream success leaves a Concord intent recoverable by another process",
       context do
    parent = self()

    {worker, monitor} =
      spawn_monitor(fn ->
        DeleteThrough.delete(context.bucket, "key", context.config, context.operation,
          before_local_commit: fn ->
            send(parent, {:before_local_commit, self()})

            receive do
              :continue -> :ok
            end
          end
        )
      end)

    assert_receive {:before_local_commit, ^worker}, 5000
    assert {:ok, %{etag: "original"}} = Metadata.get_v1_object_meta(context.bucket, "key")
    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}, 1000

    journal_key = "ess:gateway-delete:" <> Base.url_encode64(context.operation, padding: false)
    assert {:ok, %{value: %{state: :prepared}}} = Backend.get(journal_key)
    recovered = Task.async(fn -> DeleteThrough.reconcile_page(nil, 100) end) |> Task.await(5000)
    assert {:ok, %{results: results}} = recovered
    assert {context.operation, :ok} in results
    assert {:ok, nil} = Backend.get(journal_key)
    assert {:error, :not_found} = Metadata.get_v1_object_meta(context.bucket, "key")
    assert {:ok, %{}} = Concord.get("cloud_cache_index:#{context.bucket}")

    assert Agent.get(context.agent, & &1.events) == [
             {"HEAD", []},
             {"DELETE", ["\"original\""]},
             {"HEAD", []},
             {"HEAD", []}
           ]
  end

  test "recovery preserves a newer local object after a known external success", context do
    assert {:error, {:local_reconciliation_required, :injected_failure}} =
             DeleteThrough.delete(context.bucket, "key", context.config, context.operation,
               before_local_commit: fn -> {:error, :injected_failure} end
             )

    :ok = Concord.put("obj:#{context.bucket}:key", %{etag: "newer", cloud_backed: true})
    assert {:ok, %{results: results}} = DeleteThrough.reconcile_page(nil, 100)

    assert {context.operation,
            {:error, {:local_reconciliation_required, :local_metadata_conflict}}} in results

    assert {:ok, %{etag: "newer"}} = Metadata.get_v1_object_meta(context.bucket, "key")

    assert 1 ==
             Enum.count(Agent.get(context.agent, & &1.events), fn {method, _} ->
               method == "DELETE"
             end)
  end
end
