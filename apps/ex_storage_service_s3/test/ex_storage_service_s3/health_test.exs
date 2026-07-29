defmodule ExStorageServiceS3.HealthTest do
  use ExUnit.Case, async: false
  import Plug.Test

  alias ExStorageServiceS3.Router

  setup do
    health = Application.get_env(:ex_storage_service_s3, :health_options)
    status = Application.get_env(:ex_storage_service_s3, :status_options)
    auth = Application.get_env(:ex_storage_service, :s3_auth_enabled)

    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)

    on_exit(fn ->
      restore_env(:ex_storage_service_s3, :health_options, health)
      restore_env(:ex_storage_service_s3, :status_options, status)
      restore_env(:ex_storage_service, :s3_auth_enabled, auth)
    end)

    :ok
  end

  test "legacy health remains unauthenticated liveness" do
    conn = request("/health")

    assert conn.status == 200
    assert JSON.decode!(conn.resp_body) == %{"status" => "ok"}
  end

  test "readiness returns 200 only when metadata and data are ready" do
    ready = fn _opts -> {:ok, %{}} end

    Application.put_env(:ex_storage_service_s3, :health_options,
      metadata_checker: ready,
      data_checker: ready
    )

    conn = request("/health/ready")
    assert conn.status == 200
    assert %{"status" => "ready"} = JSON.decode!(conn.resp_body)

    Application.put_env(:ex_storage_service_s3, :health_options,
      metadata_checker: ready,
      data_checker: fn _opts -> {:error, :insufficient_healthy_nodes} end
    )

    conn = request("/health/ready")
    assert conn.status == 503

    assert %{
             "status" => "not_ready",
             "checks" => %{"data" => %{"reason" => "insufficient_healthy_nodes"}}
           } = JSON.decode!(conn.resp_body)
  end

  test "status is public and sanitizes provider output" do
    Application.put_env(:ex_storage_service_s3, :status_options,
      provider: fn ->
        {:ok,
         %{
           complete: true,
           desired_replicas: 2,
           actual_replicas: 2,
           repair_backlog: %{pending: 0},
           internal_secret: "hidden"
         }}
      end
    )

    conn = request("/health/status")
    body = JSON.decode!(conn.resp_body)

    assert conn.status == 200
    assert body["status"] == "ok"
    assert body["desired_replicas"] == 2
    refute Map.has_key?(body, "internal_secret")
  end

  test "mutating health-prefixed routes are not authentication bypasses" do
    conn =
      :post
      |> conn("/health/cloud_cache", "{}")
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Router.call(Router.init([]))

    assert conn.status == 403
  end

  defp request(path) do
    :get
    |> conn(path)
    |> Router.call(Router.init([]))
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
