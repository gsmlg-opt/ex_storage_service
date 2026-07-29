defmodule ExStorageServiceS3.Handlers.Health do
  @moduledoc false

  import Plug.Conn

  alias ExStorageService.Cluster.{Health, Status}

  def live(conn) do
    json(conn, 200, %{status: :ok})
  end

  def ready(conn) do
    case Health.readiness(health_options()) do
      {:ok, result} -> json(conn, 200, result)
      {:error, result} -> json(conn, 503, result)
    end
  end

  def status(conn) do
    json(conn, 200, Status.snapshot(status_options()))
  end

  defp health_options,
    do: Application.get_env(:ex_storage_service_s3, :health_options, [])

  defp status_options,
    do: Application.get_env(:ex_storage_service_s3, :status_options, [])

  defp json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, JSON.encode!(body))
  end
end
