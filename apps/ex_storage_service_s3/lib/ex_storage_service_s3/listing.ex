defmodule ExStorageServiceS3.Listing do
  @moduledoc false

  def options(params) do
    with {:ok, limit} <- limit(params["max-keys"] || "1000"),
         true <- params["list-type"] in [nil, "2"],
         true <- params["encoding-type"] in [nil, "url"],
         true <- is_nil(params["continuation-token"]) or params["list-type"] == "2" do
      {:ok,
       [
         prefix: params["prefix"] || "",
         delimiter: empty_nil(params["delimiter"]),
         max_keys: limit,
         list_type: if(params["list-type"] == "2", do: 2, else: 1),
         encoding_type: params["encoding-type"],
         marker: params["marker"] || "",
         start_after: params["start-after"],
         continuation_token: params["continuation-token"]
       ]}
    else
      _ -> {:error, :invalid_pagination}
    end
  end

  def local_options(bucket, opts) do
    token = opts[:continuation_token]
    context = context(bucket, opts)
    cursor = if opts[:list_type] == 1, do: opts[:marker], else: opts[:start_after]

    if is_nil(token) do
      {:ok, Keyword.put(opts, :continuation_token, cursor)}
    else
      with true <- byte_size(token) <= 8192,
           {:ok, payload} <- Base.url_decode64(token, padding: false),
           {:ok, %{"v" => 1, "context" => ^context, "cursor" => cursor}} <- JSON.decode(payload),
           true <- is_binary(cursor) and byte_size(cursor) <= 1024 do
        {:ok, Keyword.put(opts, :continuation_token, cursor)}
      else
        _ -> {:error, :invalid_pagination}
      end
    end
  end

  def next_token(_bucket, nil, _opts), do: nil

  def next_token(bucket, cursor, opts) do
    if opts[:list_type] == 1 do
      cursor
    else
      JSON.encode!(%{v: 1, context: context(bucket, opts), cursor: cursor})
      |> Base.url_encode64(padding: false)
    end
  end

  def response_options(opts, result, token) do
    %{
      prefix: opts[:prefix],
      delimiter: opts[:delimiter] || "",
      max_keys: opts[:max_keys],
      list_type: opts[:list_type],
      encoding_type: opts[:encoding_type],
      marker: opts[:marker],
      start_after: opts[:start_after],
      continuation_token: opts[:continuation_token],
      next_continuation_token: token,
      is_truncated: result.is_truncated,
      key_count: length(result.keys) + length(result.common_prefixes),
      common_prefixes: result.common_prefixes
    }
  end

  defp context(bucket, opts) do
    JSON.encode!([bucket, opts[:prefix] || "", opts[:delimiter] || ""])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp limit(value) do
    case Integer.parse(value) do
      {number, ""} when number >= 0 -> {:ok, min(number, 1000)}
      _ -> {:error, :invalid_pagination}
    end
  end

  defp empty_nil(""), do: nil
  defp empty_nil(value), do: value
end
