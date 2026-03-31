defmodule ExStorageServiceWeb.MetricsController do
  use ExStorageServiceWeb, :controller

  def index(conn, _params) do
    metrics = ExStorageService.Metrics.format_metrics()

    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, metrics)
  end
end
