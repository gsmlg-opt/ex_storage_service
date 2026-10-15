defmodule ExStorageServiceCli.S3.WriteResponse do
  @moduledoc "Object Lambda WriteGetObjectResponse with raw, explicitly sized bodies."
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.S3.Client

  def call(context, input, opts \\ []) do
    with :ok <- route(input["RequestRoute"]),
         {:ok, input, opts} <- body(input, opts),
         {:ok, hash} <- payload_hash(context, input, opts) do
      opts =
        opts
        |> Keyword.put(:allow_streaming, true)
        |> Keyword.put(:payload_hash, hash)
        |> Keyword.put(:max_attempts, 1)

      Client.call(context, "WriteGetObjectResponse", input, opts)
    end
  end

  @doc "Validates the modeled host-prefix label; endpoint resolution remains in the shared client."
  def route(route) when is_binary(route) do
    if Regex.match?(~r/\A[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\z/, route),
      do: :ok,
      else: failure(:invalid_request_route)
  end

  def route(_route), do: failure(:invalid_request_route)

  defp body(input, opts) do
    if Keyword.has_key?(opts, :body_source) do
      length = opts[:content_length] || input["ContentLength"]

      cond do
        not is_integer(length) or length < 0 ->
          failure(:stream_length_required)

        Map.has_key?(input, "Body") and input["Body"] != "" ->
          failure(:conflicting_response_body)

        input["ContentLength"] != nil and input["ContentLength"] != length ->
          failure(:response_length_mismatch)

        true ->
          {:ok, input |> Map.put("Body", "") |> Map.put("ContentLength", length),
           Keyword.put(opts, :content_length, length)}
      end
    else
      bytes = input["Body"] || ""

      cond do
        not is_binary(bytes) ->
          failure(:invalid_response_body)

        input["ContentLength"] != nil and input["ContentLength"] != byte_size(bytes) ->
          failure(:response_length_mismatch)

        true ->
          {:ok, input |> Map.put("Body", bytes) |> Map.put("ContentLength", byte_size(bytes)),
           opts}
      end
    end
  end

  # The modeled unsigned-body auth applies over TLS. The pinned SDK still hashes
  # payloads over HTTP; file callers provide their bounded precomputed hash.
  defp payload_hash(context, input, opts) do
    if not context.unsigned and is_binary(context.endpoint) and
         URI.parse(context.endpoint).scheme == "http" do
      if Keyword.has_key?(opts, :body_source) do
        case opts[:payload_hash] do
          hash when is_binary(hash) ->
            if Regex.match?(~r/\A[0-9a-f]{64}\z/, hash),
              do: {:ok, hash},
              else: failure(:payload_hash_required)

          _ ->
            failure(:payload_hash_required)
        end
      else
        {:ok, Base.encode16(:crypto.hash(:sha256, input["Body"]), case: :lower)}
      end
    else
      {:ok, "UNSIGNED-PAYLOAD"}
    end
  end

  defp failure(code),
    do:
      {:error,
       Error.new(:client, "Invalid Object Lambda response body or route",
         operation: "WriteGetObjectResponse",
         code: code
       )}
end
