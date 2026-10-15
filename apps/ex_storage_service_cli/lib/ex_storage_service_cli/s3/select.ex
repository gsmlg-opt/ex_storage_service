defmodule ExStorageServiceCli.S3.Select do
  @moduledoc "Backpressured SelectObjectContent events; only a validated End event confirms completion."
  alias ExStorageServiceCli.{Error, Transport.HTTPFetch}
  alias ExStorageServiceCli.S3.{Client, EventStream, Response, XML}

  def call(context, input, initial, reducer, opts \\ []) do
    # Frame assembly briefly retains both blocks and the contiguous frame.
    reserve = 2 * (opts[:max_frame_bytes] || 16_908_304) + 131_072

    client_opts =
      opts
      |> Keyword.put(:mode, :stream)
      |> Keyword.put(:buffer_bytes, max(opts[:buffer_bytes] || 0, reserve))

    with {:ok, %Response{} = response} <-
           Client.call(context, "SelectObjectContent", input, client_opts) do
      consume(response, initial, reducer, opts)
    end
  end

  def consume(%Response{} = response, initial, reducer, opts \\ []) do
    state = %{
      parser: EventStream.new(opts),
      result: initial,
      ended: false,
      stats: nil,
      progress: nil,
      record_bytes: 0,
      events: 0
    }

    event_reducer = fn event, state -> dispatch(event, state, reducer, opts) end

    chunk_reducer = fn chunk, state ->
      with {:ok, parser, state} <- EventStream.feed(state.parser, chunk, state, event_reducer),
           do: {:ok, %{state | parser: parser}}
    end

    with {:ok, state, metadata} <- HTTPFetch.consume(response, state, chunk_reducer, opts),
         :ok <- EventStream.finish(state.parser),
         true <- state.ended do
      {:ok, state.result,
       Map.merge(Map.take(state, [:stats, :progress, :record_bytes, :events]), metadata)}
    else
      false -> failure(:missing_select_end)
      error -> error
    end
  end

  defp dispatch(_event, %{ended: true}, _reducer, _opts), do: failure(:event_after_select_end)

  defp dispatch(%{headers: headers} = event, state, reducer, opts) do
    case headers[":message-type"] do
      "event" ->
        event(event, state, reducer, opts)

      type when type in ["error", "exception"] ->
        # Remote text may contain object data or terminal escapes; retain only a safe code.
        code = headers[":error-code"] || headers[":exception-type"]

        code =
          if is_binary(code) and Regex.match?(~r/\A[A-Za-z0-9_.-]{1,128}\z/, code),
            do: code,
            else: :select_service_error

        {:error,
         Error.new(:service, "S3 Select event stream reported an error",
           operation: "SelectObjectContent",
           code: code,
           http_status: 200
         )}

      _ ->
        failure(:invalid_select_message_type)
    end
  end

  defp event(%{headers: headers, payload: payload}, state, reducer, opts) do
    type = headers[":event-type"]

    with {:ok, details} <- details(type, payload, opts),
         {:ok, result} <-
           reducer.(%{type: type, payload: payload, details: details}, state.result) do
      state = %{state | result: result, ended: type == "End", events: state.events + 1}

      state =
        case type do
          "Records" -> %{state | record_bytes: state.record_bytes + byte_size(payload)}
          "Stats" -> %{state | stats: details}
          "Progress" -> %{state | progress: details}
          _ -> state
        end

      {:ok, state}
    end
  end

  defp details("Records", _payload, _opts), do: {:ok, nil}
  defp details(type, "", _opts) when type in ["End", "Cont"], do: {:ok, nil}

  defp details(type, payload, opts) when type in ["Stats", "Progress"] do
    with {:ok, node} <-
           XML.parse(payload, max_bytes: opts[:max_details_bytes] || 65_536, max_nodes: 64),
         true <- XML.local_name(node.name) == type,
         {:ok, fields} <-
           Enum.reduce_while(~w(BytesScanned BytesProcessed BytesReturned), {:ok, %{}}, fn name,
                                                                                           {:ok,
                                                                                            fields} ->
             case XML.children(node, name) do
               [child] ->
                 case Integer.parse(XML.text(child)) do
                   {value, ""} when value >= 0 -> {:cont, {:ok, Map.put(fields, name, value)}}
                   _ -> {:halt, failure(:invalid_select_details)}
                 end

               _ ->
                 {:halt, failure(:invalid_select_details)}
             end
           end) do
      {:ok, fields}
    else
      false -> failure(:invalid_select_details)
      error -> error
    end
  end

  defp details(_type, _payload, _opts), do: failure(:invalid_select_event)

  defp failure(code),
    do:
      {:error,
       Error.new(:protocol, "Invalid or incomplete S3 Select event stream",
         operation: "SelectObjectContent",
         code: code
       )}
end
