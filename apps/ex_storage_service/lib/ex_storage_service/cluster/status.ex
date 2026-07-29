defmodule ExStorageService.Cluster.Status do
  @moduledoc """
  Sanitized cluster status boundary.

  Replica and repair summaries are supplied by an injected provider. Until the
  Phase 9 planner supplies a complete snapshot, the default response is
  explicitly partial and never guesses physical replica health.
  """

  alias ExStorageService.Cluster.StatusProvider
  alias ExStorageService.Context

  @allowed_fields [
    :actual_replicas,
    :complete,
    :desired_replicas,
    :eligible_nodes,
    :healthy_nodes,
    :invalid_owners,
    :owners_current,
    :owners_expected,
    :repair_backlog,
    :required_write_quorum,
    :under_replicated_blobs,
    :unavailable_blobs,
    :updated_at
  ]

  @spec snapshot(keyword()) :: map()
  def snapshot(opts \\ []) do
    provider =
      Keyword.get(
        opts,
        :provider,
        Application.get_env(:ex_storage_service, :cluster_status_provider)
      )

    case safe_provider_snapshot(provider, opts) do
      {:ok, snapshot} when is_map(snapshot) ->
        snapshot
        |> Map.take(@allowed_fields)
        |> Map.put(:status, if(Map.get(snapshot, :complete, false), do: :ok, else: :partial))

      {:error, reason} ->
        default_snapshot(opts)
        |> Map.put(:reason, reason_string(reason))
    end
  end

  defp safe_provider_snapshot(provider, opts) do
    provider_snapshot(provider, opts)
  rescue
    _error -> {:error, :status_provider_failed}
  catch
    _kind, _reason -> {:error, :status_provider_failed}
  end

  defp provider_snapshot(nil, opts) do
    StatusProvider.snapshot(opts)
  end

  defp provider_snapshot(provider, opts) when is_function(provider, 1), do: provider.(opts)
  defp provider_snapshot(provider, _opts) when is_function(provider, 0), do: provider.()
  defp provider_snapshot(provider, opts) when is_atom(provider), do: provider.snapshot(opts)
  defp provider_snapshot(_provider, _opts), do: {:error, :invalid_status_provider}

  defp default_snapshot(opts) do
    case context(opts) do
      {:ok, context} -> config_snapshot(context)
      _error -> %{status: :unavailable, complete: false}
    end
  end

  defp context(opts) do
    case Keyword.get(opts, :context) do
      %Context{} = context -> {:ok, context}
      nil -> Context.default()
    end
  end

  defp config_snapshot(context) do
    %{
      status: :unavailable,
      complete: false,
      desired_replicas: context.config.replication_factor,
      required_write_quorum: context.config.write_quorum
    }
  end

  defp reason_string(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_string(_reason), do: "unavailable"
end
