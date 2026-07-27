defmodule ExStorageService.Cluster.Node do
  @moduledoc """
  Persistent control-plane identity for one configured cluster member.

  Records change only when node control state changes or a new generation
  starts. They are not heartbeat records.
  """

  alias ExStorageService.InstanceConfig

  @enforce_keys [
    :schema,
    :node_id,
    :generation,
    :role,
    :erlang_endpoint,
    :internal_endpoint,
    :enabled,
    :draining,
    :zone,
    :capacity,
    :updated_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          schema: 2,
          node_id: String.t(),
          generation: pos_integer(),
          role: :data | :metadata,
          erlang_endpoint: node(),
          internal_endpoint: String.t() | nil,
          enabled: boolean(),
          draining: boolean(),
          zone: String.t() | nil,
          capacity: pos_integer() | nil,
          updated_at: DateTime.t() | String.t() | integer()
        }

  @spec from_config(InstanceConfig.t(), keyword()) :: t()
  def from_config(%InstanceConfig{} = config, opts \\ []) do
    %__MODULE__{
      schema: 2,
      node_id: config.node_id,
      generation: config.node_generation,
      role: config.node_role,
      erlang_endpoint: config.erlang_node,
      internal_endpoint: config.internal_advertised_url,
      enabled: config.node_enabled,
      draining: config.node_draining,
      zone: config.node_zone,
      capacity: config.node_capacity,
      updated_at:
        Keyword.get_lazy(opts, :timestamp, fn ->
          DateTime.utc_now() |> DateTime.to_iso8601()
        end)
    }
  end

  @spec cast(term()) :: {:ok, t()} | {:error, :invalid_cluster_node}
  def cast(%__MODULE__{} = node), do: validate(node)

  def cast(%{} = value) do
    keys = [
      :schema,
      :node_id,
      :generation,
      :role,
      :erlang_endpoint,
      :internal_endpoint,
      :enabled,
      :draining,
      :zone,
      :capacity,
      :updated_at
    ]

    if Enum.all?(keys, &Map.has_key?(value, &1)),
      do: value |> Map.take(keys) |> then(&struct(__MODULE__, &1)) |> validate(),
      else: {:error, :invalid_cluster_node}
  end

  def cast(_value), do: {:error, :invalid_cluster_node}

  @spec eligible?(t()) :: boolean()
  def eligible?(%__MODULE__{
        role: :data,
        enabled: true,
        draining: false,
        internal_endpoint: endpoint
      })
      when is_binary(endpoint) and endpoint != "",
      do: true

  def eligible?(%__MODULE__{}), do: false

  defp validate(%__MODULE__{} = node) do
    if node.schema == 2 and is_binary(node.node_id) and node.node_id != "" and
         is_integer(node.generation) and node.generation >= 1 and
         node.role in [:data, :metadata] and is_atom(node.erlang_endpoint) and
         valid_optional_string?(node.internal_endpoint) and is_boolean(node.enabled) and
         is_boolean(node.draining) and valid_optional_string?(node.zone) and
         valid_capacity?(node.capacity) do
      {:ok, node}
    else
      {:error, :invalid_cluster_node}
    end
  end

  defp valid_optional_string?(nil), do: true
  defp valid_optional_string?(value), do: is_binary(value) and value != ""

  defp valid_capacity?(nil), do: true
  defp valid_capacity?(value), do: is_integer(value) and value >= 1
end
