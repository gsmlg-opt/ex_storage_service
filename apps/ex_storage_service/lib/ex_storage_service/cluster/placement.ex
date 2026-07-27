defmodule ExStorageService.Cluster.Placement do
  @moduledoc """
  Pure deterministic rendezvous placement for immutable blob hashes.
  """

  alias ExStorageService.Cluster.Node

  @spec select(binary(), [Node.t()], pos_integer()) ::
          {:ok, [Node.t()]}
          | {:error,
             :duplicate_node_id | :insufficient_eligible_nodes | :invalid_replication_factor}
  def select(hash, nodes, replication_factor)
      when is_binary(hash) and is_list(nodes) and is_integer(replication_factor) and
             replication_factor >= 1 do
    eligible = Enum.filter(nodes, &Node.eligible?/1)

    cond do
      duplicate_node_id?(eligible) ->
        {:error, :duplicate_node_id}

      length(eligible) < replication_factor ->
        {:error, :insufficient_eligible_nodes}

      true ->
        selected =
          eligible
          |> Enum.sort_by(
            fn node -> {score(hash, node.node_id), node.node_id} end,
            :desc
          )
          |> Enum.take(replication_factor)

        {:ok, selected}
    end
  end

  def select(_hash, _nodes, _replication_factor),
    do: {:error, :invalid_replication_factor}

  @doc false
  @spec score(binary(), binary()) :: binary()
  def score(hash, node_id) when is_binary(hash) and is_binary(node_id) do
    :crypto.hash(:sha256, hash <> <<0>> <> node_id)
  end

  defp duplicate_node_id?(nodes) do
    ids = Enum.map(nodes, & &1.node_id)
    length(ids) != length(Enum.uniq(ids))
  end
end
