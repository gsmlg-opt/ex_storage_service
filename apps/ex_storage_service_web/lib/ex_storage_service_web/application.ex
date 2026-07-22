defmodule ExStorageServiceWeb.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    opts = [strategy: :one_for_one, name: ExStorageServiceWeb.Supervisor]
    Supervisor.start_link(children(), opts)
  end

  @doc false
  def children(opts \\ []) do
    enabled? =
      Keyword.get_lazy(opts, :enabled, fn ->
        Application.get_env(:ex_storage_service_web, :enabled, true)
      end)

    node_role =
      Keyword.get_lazy(opts, :node_role, fn ->
        Application.get_env(:ex_storage_service, :node_role, :data)
      end)

    if enabled? and node_role == :data, do: [ExStorageServiceWeb.Endpoint], else: []
  end

  @impl true
  def config_change(changed, _new, removed) do
    ExStorageServiceWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
