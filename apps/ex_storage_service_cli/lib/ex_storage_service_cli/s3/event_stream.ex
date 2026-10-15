defmodule ExStorageServiceCli.S3.EventStream do
  @moduledoc "Bounded incremental AWS EventStream frames with prelude and message CRC validation."
  alias ExStorageServiceCli.Error

  defstruct parts: [],
            pending: "",
            size: 0,
            target: 12,
            prelude: nil,
            header_bytes: 0,
            max_frame_bytes: 16_908_304,
            max_header_bytes: 131_072

  def new(opts \\ []),
    do: %__MODULE__{
      max_frame_bytes: opts[:max_frame_bytes] || 16_908_304,
      max_header_bytes: opts[:max_header_bytes] || 131_072
    }

  @doc "Delivers each complete CRC-verified frame to reducer without buffering the event collection."
  def feed(%__MODULE__{} = state, "", acc, _reducer), do: {:ok, state, acc}

  def feed(%__MODULE__{} = state, chunk, acc, reducer) when is_binary(chunk) do
    take = min(state.target - state.size, byte_size(chunk))
    <<piece::binary-size(take), rest::binary>> = chunk
    state = append(%{state | size: state.size + take}, piece)

    cond do
      state.size < state.target ->
        {:ok, state, acc}

      state.prelude == nil ->
        prelude = assembled(state)
        with {:ok, state} <- prelude(state, prelude), do: feed(state, rest, acc, reducer)

      true ->
        body = assembled(state)

        with {:ok, event} <- frame(state, body),
             {:ok, acc} <- reducer.(event, acc) do
          state = %{
            state
            | parts: [],
              pending: "",
              size: 0,
              target: 12,
              prelude: nil,
              header_bytes: 0
          }

          if rest == "", do: {:ok, state, acc}, else: feed(state, rest, acc, reducer)
        end
    end
  end

  def finish(%__MODULE__{size: 0, prelude: nil}), do: :ok
  def finish(%__MODULE__{}), do: failure(:truncated_event_stream)

  defp prelude(state, <<total::32, headers::32, crc::32>> = prelude) do
    cond do
      :erlang.crc32(binary_part(prelude, 0, 8)) != crc ->
        failure(:event_prelude_crc)

      total < 16 or total > state.max_frame_bytes or headers > state.max_header_bytes or
          headers > total - 16 ->
        failure(:event_frame_limit)

      true ->
        {:ok,
         %{
           state
           | parts: [],
             pending: "",
             size: 0,
             target: total - 12,
             prelude: prelude,
             header_bytes: headers
         }}
    end
  end

  # Tiny transport fragments must not retain one list cell/binary per byte.
  defp append(state, piece) do
    needed = 16_384 - byte_size(state.pending)

    if byte_size(piece) < needed do
      %{state | pending: state.pending <> piece}
    else
      <<prefix::binary-size(needed), rest::binary>> = piece
      state = %{state | pending: "", parts: [state.pending <> prefix | state.parts]}
      append(state, rest)
    end
  end

  defp assembled(state), do: IO.iodata_to_binary([Enum.reverse(state.parts), state.pending])

  defp frame(state, body) do
    size = byte_size(body) - 4
    <<message::binary-size(size), crc::32>> = body
    <<fields::binary-size(state.header_bytes), payload::binary>> = message

    with true <- :erlang.crc32(:erlang.crc32(state.prelude), message) == crc,
         {:ok, headers} <- headers(fields, %{}) do
      {:ok, %{headers: headers, payload: payload}}
    else
      false -> failure(:event_message_crc)
      error -> error
    end
  end

  defp headers("", result), do: {:ok, result}

  defp headers(<<size, name::binary-size(size), type, rest::binary>>, result) when size > 0 do
    with true <- String.valid?(name) and not Map.has_key?(result, name),
         {:ok, value, rest} <- header_value(type, rest) do
      headers(rest, Map.put(result, name, value))
    else
      _ -> failure(:invalid_event_headers)
    end
  end

  defp headers(_fields, _result), do: failure(:invalid_event_headers)

  defp header_value(0, rest), do: {:ok, true, rest}
  defp header_value(1, rest), do: {:ok, false, rest}
  defp header_value(2, <<value::signed-8, rest::binary>>), do: {:ok, value, rest}
  defp header_value(3, <<value::signed-16, rest::binary>>), do: {:ok, value, rest}
  defp header_value(4, <<value::signed-32, rest::binary>>), do: {:ok, value, rest}

  defp header_value(type, <<value::signed-64, rest::binary>>) when type in [5, 8],
    do: {:ok, value, rest}

  defp header_value(type, <<size::16, value::binary-size(size), rest::binary>>)
       when type in [6, 7] do
    if type == 6 or String.valid?(value),
      do: {:ok, value, rest},
      else: failure(:invalid_event_headers)
  end

  defp header_value(9, <<value::binary-size(16), rest::binary>>), do: {:ok, value, rest}
  defp header_value(_type, _rest), do: failure(:invalid_event_headers)

  defp failure(code),
    do: {:error, Error.new(:protocol, "Invalid or truncated AWS EventStream", code: code)}
end
