defmodule ExStorageServiceCluster.Application do
  @moduledoc false

  use Application

  alias ExStorageServiceCluster.{InternalAuth, ReplayCache, Router}

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(),
      strategy: :rest_for_one,
      name: ExStorageServiceCluster.Supervisor
    )
  end

  @doc false
  def children(opts \\ Application.get_all_env(:ex_storage_service_cluster)) do
    if Keyword.get(opts, :enabled, false) do
      replay_table = Keyword.get(opts, :replay_table, InternalAuth.ReplayTable)
      skew_seconds = Keyword.get(opts, :auth_skew_seconds, 60)

      router_opts = [
        secret: Keyword.fetch!(opts, :secret),
        replay_table: replay_table,
        auth_skew_seconds: skew_seconds,
        node_id: Keyword.fetch!(opts, :node_id),
        node_generation: Keyword.get(opts, :node_generation, 0),
        blob_store_opts: Keyword.fetch!(opts, :blob_store_opts),
        max_blob_size: Keyword.fetch!(opts, :max_blob_size),
        read_timeout: Keyword.get(opts, :read_timeout, 60_000)
      ]

      [
        {ReplayCache,
         name: ReplayCache, table: replay_table, sweep_interval: skew_seconds * 1_000},
        {Bandit, bandit_options(opts, router_opts)}
      ]
    else
      []
    end
  end

  defp bandit_options(opts, router_opts) do
    base = [
      plug: {Router, router_opts},
      scheme: if(Keyword.get(opts, :tls), do: :https, else: :http),
      ip: Keyword.get(opts, :bind, {127, 0, 0, 1}),
      port: Keyword.get(opts, :port, 9100)
    ]

    case Keyword.get(opts, :tls) do
      %{certfile: certfile, keyfile: keyfile} ->
        base ++ [certfile: certfile, keyfile: keyfile]

      nil ->
        base
    end
  end
end
