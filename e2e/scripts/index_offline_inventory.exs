# Offline A28 fixture acquisition only. This fresh, non-TTL store uses coherent
# plain writes and transactions; arbitrary unreconciled command-v0 stores may
# differ from KV Records. The complete map remains outside measured requests.
defmodule ESSIndexAcceptance.Inventory do
  def snapshot(client \\ Concord.KV) do
    with {:ok, revision} <- client.revision(consistency: :linearizable) do
      pages(client, revision, [prefix: ""], nil, %{})
    end
  end

  defp pages(client, revision, selector, upper, snapshot) do
    # Retain the public API's default page size and per-operation timeout.
    opts = [consistency: :linearizable, revision: revision, order: :desc] ++ selector

    with {:ok, records, page} <- client.list(opts),
         true <- valid_page?(records, page),
         {:ok, snapshot} <- consume(records, upper, snapshot) do
      if page.has_more do
        pages(client, revision, [range: {"", page.last_key}], page.last_key, snapshot)
      else
        {:ok, snapshot}
      end
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_inventory_page}
    end
  end

  defp valid_page?([], %{has_more: false, last_key: nil}), do: true

  defp valid_page?([_ | _] = records, %{has_more: has_more, last_key: last_key})
       when is_boolean(has_more) and is_binary(last_key) do
    match?(%{key: ^last_key}, List.last(records))
  end

  defp valid_page?(_records, _page), do: false

  defp consume([], _upper, snapshot), do: {:ok, snapshot}

  defp consume([%{key: key, value: value} | rest], upper, snapshot)
       when is_binary(key) and byte_size(key) in 1..4_096 do
    if is_nil(upper) or key < upper do
      consume(rest, key, Map.put(snapshot, key, value))
    else
      {:error, :invalid_inventory_page}
    end
  end

  defp consume(_records, _upper, _snapshot), do: {:error, :invalid_inventory_page}
end
