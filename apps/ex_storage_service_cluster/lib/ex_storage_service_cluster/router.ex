defmodule ExStorageServiceCluster.Router do
  @moduledoc """
  Private, content-hash-addressed blob routes.

  This listener must be bound to a trusted internal network and is never a
  public S3 endpoint.
  """

  use Plug.Router

  alias ExStorageServiceCluster.BlobHandler

  plug(:match)
  plug(:dispatch)

  @impl Plug
  def call(conn, opts) do
    conn
    |> Plug.Conn.put_private(:ess_router_options, opts)
    |> super(opts)
  end

  put "/internal/v1/blobs/:sha256" do
    BlobHandler.put(conn, sha256, router_options(conn))
  end

  head "/internal/v1/blobs/:sha256" do
    BlobHandler.head(conn, sha256, router_options(conn))
  end

  get "/internal/v1/blobs/:sha256" do
    BlobHandler.get(conn, sha256, router_options(conn))
  end

  match _ do
    Plug.Conn.send_resp(conn, 404, "not found")
  end

  defp router_options(conn), do: conn.private.ess_router_options
end
