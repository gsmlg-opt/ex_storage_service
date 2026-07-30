defmodule ExStorageService.Operations.Node do
  @moduledoc """
  Operator-facing node drain controls and bounded progress reads.
  """

  alias ExStorageService.Cluster.Drain
  alias ExStorageService.Context

  @spec drain(Context.t(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def drain(context, node_id, opts \\ [])

  def drain(%Context{config: %{mode: :cluster}} = context, node_id, opts)
      when is_binary(node_id) and node_id != "" do
    Drain.start(context, node_id, opts)
  end

  def drain(%Context{}, _node_id, _opts), do: {:error, :standalone_mode}

  @spec status(Context.t(), binary(), binary() | nil, pos_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def status(context, node_id, cursor \\ nil, limit \\ 100, opts \\ [])

  def status(
        %Context{config: %{mode: :cluster}} = context,
        node_id,
        cursor,
        limit,
        opts
      )
      when is_binary(node_id) and node_id != "" and
             (is_binary(cursor) or is_nil(cursor)) and is_integer(limit) and limit > 0 do
    Drain.status_page(context, node_id, cursor, limit, opts)
  end

  def status(%Context{}, _node_id, _cursor, _limit, _opts),
    do: {:error, :standalone_mode}
end
