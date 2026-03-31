defmodule ExStorageServiceWeb.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      ExStorageServiceWeb.Endpoint
    ]

    opts = [strategy: :one_for_one, name: ExStorageServiceWeb.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def config_change(changed, _new, removed) do
    ExStorageServiceWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
