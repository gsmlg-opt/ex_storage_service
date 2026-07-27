defmodule ExStorageService.Cluster.Membership do
  @moduledoc """
  Persistent cluster-node registration and exact configured membership reads.

  Only configured voter IDs are read. Discovery and arbitrary metadata records
  never expand the placement membership.
  """

  alias ExStorageService.Cluster.Node
  alias ExStorageService.InstanceConfig
  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys

  @type member_record :: %{node: Node.t(), mod_revision: non_neg_integer()}
  @max_register_attempts 4

  @spec register(InstanceConfig.t(), keyword()) :: :ok | {:error, term()}
  def register(config, opts \\ [])

  def register(%InstanceConfig{mode: :standalone}, _opts),
    do: {:error, :standalone_mode}

  def register(%InstanceConfig{} = config, opts) do
    do_register(config, opts, @max_register_attempts)
  end

  @spec members(InstanceConfig.t(), keyword()) :: {:ok, [member_record()]} | {:error, term()}
  def members(config, opts \\ [])

  def members(%InstanceConfig{mode: :standalone}, _opts),
    do: {:error, :standalone_mode}

  def members(%InstanceConfig{} = config, opts) do
    config.cluster_members
    |> Enum.reduce_while({:ok, []}, fn configured, {:ok, records} ->
      case read_member(configured, opts) do
        {:ok, nil} -> {:cont, {:ok, records}}
        {:ok, record} -> {:cont, {:ok, [record | records]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, records} ->
        records = Enum.reverse(records)

        with :ok <- validate_unique_endpoint(records, :erlang_endpoint),
             :ok <- validate_unique_endpoint(records, :internal_endpoint) do
          {:ok, records}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp do_register(_config, _opts, 0), do: {:error, :registration_compare_failed}

  defp do_register(config, opts, attempts_left) do
    key = Keys.cluster_node(config.node_id)

    with {:ok, observed} <- backend(opts).get(key, read_opts(opts)),
         {:ok, current} <- current_node(observed),
         :ok <- validate_current_identity(current, config.node_id),
         :ok <- validate_generation(current, config.node_generation),
         node = registration_node(config, current, opts) do
      if already_registered?(current, node) do
        :ok
      else
        node_record = Map.from_struct(node)

        spec = %{
          compare: [revision_compare(key, observed)],
          success: [{:put, key, node_record, %{}}],
          failure: []
        }

        transaction_opts =
          write_opts(opts)
          |> Keyword.put(:idempotency_key, registration_attempt_key(config.node_id, spec))

        case backend(opts).transaction(spec, transaction_opts) do
          {:ok, %{succeeded: true}} ->
            :ok

          {:ok, %{succeeded: false}} ->
            do_register(config, opts, attempts_left - 1)

          {:error, reason} ->
            {:error, reason}
        end
      end
    end
  end

  defp current_node(nil), do: {:ok, nil}
  defp current_node(%{value: value}), do: Node.cast(value)

  defp validate_current_identity(nil, _node_id), do: :ok
  defp validate_current_identity(%Node{node_id: node_id}, node_id), do: :ok

  defp validate_current_identity(_current, node_id),
    do: {:error, {:cluster_node_identity_mismatch, node_id}}

  defp validate_generation(nil, _generation), do: :ok

  defp validate_generation(%Node{generation: current}, generation) when current <= generation,
    do: :ok

  defp validate_generation(%Node{generation: current}, generation),
    do: {:error, {:stale_node_generation, generation, current}}

  defp registration_node(config, nil, opts), do: Node.from_config(config, opts)

  defp registration_node(config, current, opts) do
    configured = Node.from_config(config, opts)

    if Keyword.get(opts, :replace_control_state, false) do
      configured
    else
      %{
        configured
        | enabled: current.enabled,
          draining: current.draining,
          zone: current.zone,
          capacity: current.capacity
      }
    end
  end

  defp already_registered?(nil, _node), do: false

  defp already_registered?(current, node) do
    Map.delete(Map.from_struct(current), :updated_at) ==
      Map.delete(Map.from_struct(node), :updated_at)
  end

  defp read_member(configured, opts) do
    case backend(opts).get(Keys.cluster_node(configured.id), read_opts(opts)) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, %{value: value, mod_revision: revision}} ->
        with {:ok, node} <- Node.cast(value),
             :ok <- validate_configured_member(node, configured) do
          {:ok, %{node: node, mod_revision: revision}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_configured_member(
         %Node{node_id: id, erlang_endpoint: endpoint},
         %{id: id, endpoint: endpoint}
       ),
       do: :ok

  defp validate_configured_member(%Node{node_id: id}, %{id: id}),
    do: {:error, {:cluster_node_endpoint_mismatch, id}}

  defp validate_configured_member(_node, %{id: id}),
    do: {:error, {:cluster_node_identity_mismatch, id}}

  defp validate_unique_endpoint(records, field) do
    duplicate =
      records
      |> Enum.map(&Map.fetch!(&1.node, field))
      |> Enum.reject(&is_nil/1)
      |> Enum.frequencies()
      |> Enum.find_value(fn
        {endpoint, count} when count > 1 -> endpoint
        {_endpoint, _count} -> nil
      end)

    if duplicate,
      do: {:error, {:duplicate_cluster_node_endpoint, field, duplicate}},
      else: :ok
  end

  defp revision_compare(key, nil), do: {:mod_revision, key, :==, 0}

  defp revision_compare(key, %{mod_revision: revision}),
    do: {:mod_revision, key, :==, revision}

  defp registration_attempt_key(node_id, spec) do
    fingerprint =
      spec
      |> :erlang.term_to_binary([:deterministic])
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    "cluster-node:#{node_id}:#{fingerprint}"
  end

  defp read_opts(opts),
    do:
      opts
      |> Keyword.take([:consistency, :timeout, :engine, :barrier])
      |> Keyword.put_new(:consistency, :strong)

  defp write_opts(opts), do: Keyword.take(opts, [:timeout, :engine, :barrier])
  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
end
