defmodule ExStorageServiceCli.S3.EventStreamTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, S3.EventStream}

  @fixtures __DIR__
            |> Path.join("../../fixtures/special_s3/eventstream-vectors.json")
            |> File.read!()
            |> JSON.decode!()

  for vector <- @fixtures["vectors"] do
    @vector vector
    test "pinned botocore EventStream vector #{@vector["name"]} survives arbitrary fragmentation" do
      vector = @vector
      bytes = Base.decode64!(vector["frame_base64"])
      expected = %{headers: vector["headers"], payload: Base.decode64!(vector["payload_base64"])}

      for size <- [1, 2, 3, 7, 11, 12, 13, byte_size(bytes)] do
        {state, events} =
          Enum.reduce(chunks(bytes, size), {EventStream.new(), []}, fn chunk, {state, events} ->
            assert {:ok, state, events} =
                     EventStream.feed(state, chunk, events, fn event, events ->
                       {:ok, [event | events]}
                     end)

            assert state.size <= state.max_frame_bytes
            {state, events}
          end)

        assert :ok = EventStream.finish(state)
        assert events == [expected]
      end
    end
  end

  test "CRC failures, forged sizes and truncated frames never dispatch payloads" do
    vector = hd(@fixtures["vectors"])
    bytes = Base.decode64!(vector["frame_base64"])

    reducer = fn event, state ->
      send(self(), {:dispatched, event})
      {:ok, state}
    end

    <<head, rest::binary>> = bytes

    assert {:error, %Error{code: :event_prelude_crc}} =
             EventStream.feed(EventStream.new(), <<Bitwise.bxor(head, 1)>> <> rest, nil, reducer)

    <<prefix::binary-size(byte_size(bytes) - 1), last>> = bytes

    assert {:error, %Error{code: :event_message_crc}} =
             EventStream.feed(
               EventStream.new(),
               prefix <> <<Bitwise.bxor(last, 1)>>,
               nil,
               reducer
             )

    prelude = <<1_000_000::32, 0::32>>

    assert {:error, %Error{code: :event_frame_limit}} =
             EventStream.feed(
               EventStream.new(max_frame_bytes: 1024),
               prelude <> <<:erlang.crc32(prelude)::32>>,
               nil,
               reducer
             )

    for size <- 1..(byte_size(bytes) - 1) do
      assert {:ok, state, nil} =
               EventStream.feed(EventStream.new(), binary_part(bytes, 0, size), nil, reducer)

      assert {:error, %Error{code: :truncated_event_stream}} = EventStream.finish(state)
    end

    refute_received {:dispatched, _}
  end

  test "coalesced frames honor reducer failures, reject duplicate headers and unknown types" do
    bytes = @fixtures["vectors"] |> hd() |> Map.fetch!("frame_base64") |> Base.decode64!()

    assert {:ok, state, 2} =
             EventStream.feed(EventStream.new(), bytes <> bytes, 0, fn _, count ->
               {:ok, count + 1}
             end)

    assert :ok = EventStream.finish(state)
    error = {:error, Error.new(:interrupted, "cancelled")}
    assert ^error = EventStream.feed(EventStream.new(), bytes <> bytes, nil, fn _, _ -> error end)

    for fields <- [
          <<1, "x", 0, 1, "x", 1>>,
          <<1, "x", 255>>,
          <<0, 0>>,
          <<1, "x", 7, 0, 2, 255, 255>>
        ] do
      frame = frame(fields, "")

      assert {:error, %Error{code: :invalid_event_headers}} =
               EventStream.feed(EventStream.new(), frame, nil, fn _, state -> {:ok, state} end)
    end
  end

  test "empty chunks do not accumulate parser state" do
    state = EventStream.new()

    assert {:ok, ^state, nil} =
             EventStream.feed(state, "", nil, fn _, _ -> flunk("empty event") end)
  end

  test "one-byte fragmentation coalesces retained buffers within the frame bound" do
    bytes = frame("", String.duplicate("x", 32_768))
    partial = binary_part(bytes, 0, byte_size(bytes) - 1)

    state =
      for <<byte <- partial>>, reduce: EventStream.new() do
        state ->
          {:ok, state, nil} =
            EventStream.feed(state, <<byte>>, nil, fn _, _ ->
              flunk("partial frame dispatched")
            end)

          state
      end

    assert length(state.parts) == 2
    assert byte_size(state.pending) == 3
    assert {:error, %Error{code: :truncated_event_stream}} = EventStream.finish(state)

    assert {:ok, _state, 32_768} =
             EventStream.feed(state, binary_part(bytes, byte_size(bytes) - 1, 1), nil, fn event,
                                                                                          _ ->
               {:ok, byte_size(event.payload)}
             end)
  end

  defp chunks("", _size), do: []

  defp chunks(bytes, size) do
    take = min(byte_size(bytes), size)
    <<chunk::binary-size(take), rest::binary>> = bytes
    [chunk | chunks(rest, size)]
  end

  defp frame(headers, payload) do
    prelude = <<byte_size(headers) + byte_size(payload) + 16::32, byte_size(headers)::32>>
    message = prelude <> <<:erlang.crc32(prelude)::32>> <> headers <> payload
    message <> <<:erlang.crc32(message)::32>>
  end
end
