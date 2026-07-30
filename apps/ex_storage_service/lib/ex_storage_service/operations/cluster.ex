defmodule ExStorageService.Operations.Cluster do
  @moduledoc """
  Operator-facing cluster bootstrap and status operations.

  These functions are deliberately process-free. They compose the existing
  Concord readiness, persistent membership, and sanitized status boundaries.
  """

  alias ExStorageService.Cluster.{Membership, Readiness, Status}
  alias ExStorageService.Context

  @spec bootstrap(Context.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def bootstrap(context, opts \\ [])

  def bootstrap(%Context{config: %{mode: :cluster}} = context, opts) do
    with {:ok, metadata} <- readiness(opts).await(readiness_opts(opts)),
         :ok <- membership(opts).register(context.config, metadata_opts(opts)),
         {:ok, snapshot} <- status(context, opts) do
      {:ok,
       snapshot
       |> Map.put(:bootstrap, :complete)
       |> Map.put(:metadata, normalize_readiness(metadata))}
    end
  end

  def bootstrap(%Context{}, _opts), do: {:error, :standalone_mode}

  @spec status(Context.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def status(context, opts \\ [])

  def status(%Context{config: %{mode: :cluster}} = context, opts) do
    snapshot = %{
      metadata: metadata_status(opts),
      cluster: status_provider(opts).snapshot(status_opts(context, opts))
    }

    {:ok, Map.merge(snapshot, membership_status(context, opts))}
  end

  def status(%Context{}, _opts), do: {:error, :standalone_mode}

  defp metadata_status(opts) do
    case readiness(opts).check(readiness_opts(opts)) do
      {:ok, value} -> normalize_readiness(value)
      {:error, reason} -> %{status: :unavailable, reason: reason}
    end
  end

  defp normalize_readiness(%{cluster: cluster}) when is_map(cluster) do
    %{
      status: Map.get(cluster, :status, :unknown),
      primary_id: Map.get(cluster, :primary_id)
    }
  end

  defp normalize_readiness(_value), do: %{status: :ready}

  defp member_summary(%{node: node, mod_revision: revision}) do
    %{
      node_id: node.node_id,
      generation: node.generation,
      role: node.role,
      enabled: node.enabled,
      draining: node.draining,
      mod_revision: revision
    }
  end

  defp membership_status(context, opts) do
    case membership(opts).members(context.config, metadata_opts(opts)) do
      {:ok, members} ->
        %{membership_status: :ok, members: Enum.map(members, &member_summary/1)}

      {:error, reason} ->
        %{membership_status: :unavailable, membership_error: reason, members: []}
    end
  end

  defp metadata_opts(opts),
    do: Keyword.take(opts, [:backend, :consistency, :timeout, :engine, :barrier, :revision])

  defp readiness_opts(opts) do
    opts
    |> Keyword.take([:timeout, :await_timeout, :interval])
    |> maybe_put_readiness_backend(opts)
  end

  defp maybe_put_readiness_backend(readiness_opts, opts) do
    case Keyword.fetch(opts, :readiness_backend) do
      {:ok, backend} -> Keyword.put(readiness_opts, :backend, backend)
      :error -> readiness_opts
    end
  end

  defp status_opts(context, opts) do
    opts
    |> Keyword.take([
      :backend,
      :consistency,
      :timeout,
      :engine,
      :barrier,
      :revision,
      :freshness_ms,
      :now_ms,
      :job_page_size,
      :max_job_pages,
      :job_store,
      :membership,
      :provider
    ])
    |> Keyword.put(:context, context)
  end

  defp membership(opts), do: Keyword.get(opts, :membership, Membership)
  defp readiness(opts), do: Keyword.get(opts, :readiness, Readiness)
  defp status_provider(opts), do: Keyword.get(opts, :status, Status)
end
