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
    if Keyword.get(opts, :enabled, false) and not recovery_quarantined?() do
      replay_table = Keyword.get(opts, :replay_table, InternalAuth.ReplayTable)
      skew_seconds = Keyword.get(opts, :auth_skew_seconds, 60)

      router_opts = [
        secret: Keyword.fetch!(opts, :secret),
        replay_table: replay_table,
        auth_skew_seconds: skew_seconds,
        node_id: Keyword.fetch!(opts, :node_id),
        node_generation: Keyword.get(opts, :node_generation, 1),
        blob_store_opts: Keyword.fetch!(opts, :blob_store_opts),
        max_blob_size: Keyword.fetch!(opts, :max_blob_size),
        read_timeout: Keyword.get(opts, :read_timeout, 60_000)
      ]

      [
        {ReplayCache,
         name: ReplayCache, table: replay_table, sweep_interval: skew_seconds * 1_000},
        # TODO(upstream): gsmlg-dev/gaoxy#15 gracefully handle already-closed upload peers.
        {Gaoxy.HTTP, http_options(opts, router_opts)}
      ]
    else
      []
    end
  end

  defp recovery_quarantined? do
    case ExStorageService.InstanceConfig.from_application_env() do
      {:ok, config} -> ExStorageService.Operations.Backup.quarantined?(config)
      {:error, _} -> true
    end
  end

  defp http_options(opts, router_opts) do
    base = [
      id: Router,
      plug: {Router, router_opts},
      protocols: [:http1],
      ip: Keyword.get(opts, :bind, {127, 0, 0, 1}),
      port: Keyword.get(opts, :port, 9100),
      # Aggregate retained-byte budget across concurrent bounded transfer windows.
      limits: %{max_bytes: 64 * 1_024 * 1_024},
      http_limits: %{
        max_body_bytes: Keyword.fetch!(opts, :max_blob_size),
        max_read_chunk: 65_536,
        request_timeout: :infinity,
        read_timeout: Keyword.get(opts, :read_timeout, 60_000),
        write_timeout: 60_000
      }
    ]

    case Keyword.get(opts, :tls) do
      %{certfile: certfile, keyfile: keyfile} ->
        base ++ [transport: :tls, tls: %{certfile: certfile, keyfile: keyfile}]

      nil ->
        base ++ [transport: :tcp]
    end
  end
end
