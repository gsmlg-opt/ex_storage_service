defmodule ExStorageServiceS3.AwsChunked do
  @moduledoc false
  import Plug.Conn, only: [get_req_header: 2]
  alias ExStorageServiceS3.Auth.SigV4
  @empty_hash Base.encode16(:crypto.hash(:sha256, ""), case: :lower)
  @max_line_bytes 8_192
  @max_trailers 5

  # Every data chunk has at least one decoded byte, a header line and two CRLFs.
  # Include the terminal header, all allowed trailers, and the final CRLF.
  def max_wire_size(decoded_bytes) do
    decoded_bytes * (@max_line_bytes + 5) +
      (@max_trailers + 1) * (@max_line_bytes + 2) + 2
  end

  def new(conn) do
    mode = get_req_header(conn, "x-amz-content-sha256")
    context = conn.assigns[:sigv4_streaming]

    cond do
      get_req_header(conn, "x-amz-decoded-content-length") == [] ->
        {:error, :invalid_content_length}

      mode == ["STREAMING-AWS4-HMAC-SHA256-PAYLOAD"] and is_map(context) and
          get_req_header(conn, "x-amz-trailer") == [] ->
        {:ok, initial(:signed, context)}

      mode == ["STREAMING-UNSIGNED-PAYLOAD-TRAILER"] and
          get_req_header(conn, "x-amz-trailer") != [] ->
        {:ok, initial(:unsigned_trailer, nil)}

      true ->
        {:error, :unsupported_payload_mode}
    end
  end

  defp initial(kind, context),
    do: %{
      mode: :header,
      kind: kind,
      context: context,
      buffer: "",
      hash: nil,
      signature: nil,
      trailers: %{}
    }

  def feed(data, state) do
    try do
      {chunks, state} = consume(data, state, [])
      {:ok, IO.iodata_to_binary(chunks), state}
    catch
      {:error, reason} -> {:error, reason}
    end
  end

  def finished?(%{mode: :done}), do: true
  def finished?(_), do: false

  defp consume(data, %{mode: :header} = state, output) do
    line(data, state, output, fn header, rest, state, output ->
      {size, signature} = parse_header(header, state.kind)
      state = %{state | hash: :crypto.hash_init(:sha256), signature: signature}

      if size == 0 do
        state = verify_signature(state)
        mode = if state.kind == :unsigned_trailer, do: :trailers, else: :terminal_crlf
        consume(rest, %{state | mode: mode}, output)
      else
        consume(rest, %{state | mode: {:data, size}}, output)
      end
    end)
  end

  defp consume(<<>>, %{mode: {:data, _}} = state, output), do: {Enum.reverse(output), state}

  defp consume(data, %{mode: {:data, remaining}} = state, output) do
    take = min(byte_size(data), remaining)
    <<bytes::binary-size(take), rest::binary>> = data
    hash = :crypto.hash_update(state.hash, bytes)

    state = %{
      state
      | hash: hash,
        mode: if(take == remaining, do: :chunk_crlf, else: {:data, remaining - take})
    }

    consume(rest, state, [bytes | output])
  end

  defp consume(data, %{mode: mode} = state, output) when mode in [:chunk_crlf, :terminal_crlf] do
    bytes = state.buffer <> data

    case bytes do
      <<"\r\n", rest::binary>> ->
        next =
          if mode == :terminal_crlf,
            do: %{state | mode: :done, buffer: ""},
            else: %{verify_signature(state) | mode: :header, buffer: ""}

        consume(rest, next, output)

      _ when byte_size(bytes) < 2 ->
        {Enum.reverse(output), %{state | buffer: bytes}}

      _ ->
        throw({:error, :malformed_chunked})
    end
  end

  defp consume(data, %{mode: :trailers} = state, output) do
    line(data, state, output, fn
      "", rest, state, output ->
        consume(rest, %{state | mode: :done}, output)

      trailer, rest, state, output ->
        case String.split(trailer, ":", parts: 2) do
          [name, value] ->
            if not String.starts_with?(name, "x-amz-checksum-") or
                 Map.has_key?(state.trailers, name) or map_size(state.trailers) >= @max_trailers,
               do: throw({:error, :malformed_chunked})

            consume(
              rest,
              %{state | trailers: Map.put(state.trailers, name, String.trim(value))},
              output
            )

          _ ->
            throw({:error, :malformed_chunked})
        end
    end)
  end

  defp consume("", %{mode: :done} = state, output), do: {Enum.reverse(output), state}
  defp consume(_data, %{mode: :done}, _output), do: throw({:error, :malformed_chunked})

  defp line(data, state, output, next) do
    bytes = state.buffer <> data

    case :binary.match(bytes, "\r\n") do
      {size, 2} when size <= @max_line_bytes ->
        <<header::binary-size(size), "\r\n", rest::binary>> = bytes
        next.(header, rest, %{state | buffer: ""}, output)

      :nomatch when byte_size(bytes) <= @max_line_bytes ->
        {Enum.reverse(output), %{state | buffer: bytes}}

      _ ->
        throw({:error, :malformed_chunked})
    end
  end

  defp parse_header(header, :signed) do
    case Regex.run(~r/\A([0-9a-fA-F]+);chunk-signature=([0-9a-f]{64})\z/, header) do
      [_, hex, signature] -> {String.to_integer(hex, 16), signature}
      _ -> throw({:error, :malformed_chunked})
    end
  end

  defp parse_header(header, :unsigned_trailer) do
    if Regex.match?(~r/\A[0-9a-fA-F]+\z/, header),
      do: {String.to_integer(header, 16), nil},
      else: throw({:error, :malformed_chunked})
  end

  defp verify_signature(%{kind: :unsigned_trailer} = state), do: state

  defp verify_signature(%{context: context} = state) do
    hash = Base.encode16(:crypto.hash_final(state.hash), case: :lower)

    to_sign =
      Enum.join(
        [
          "AWS4-HMAC-SHA256-PAYLOAD",
          context.datetime,
          context.scope,
          context.signature,
          @empty_hash,
          hash
        ],
        "\n"
      )

    expected = SigV4.compute_signature(context.key, to_sign)

    if Plug.Crypto.secure_compare(expected, state.signature) do
      %{state | context: %{context | signature: state.signature}}
    else
      throw({:error, :chunk_signature_mismatch})
    end
  end
end
