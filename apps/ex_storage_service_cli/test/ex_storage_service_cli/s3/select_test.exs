Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.S3.SelectTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, S3.Response, S3.Select}
  alias ExStorageServiceCli.Test.WireServer

  setup_all do
    {:ok, _} = Application.ensure_all_started(:http_fetch)
    :ok
  end

  test "modeled Select uses raw event stream, preserves split records and requires End" do
    stats =
      "<Stats><BytesScanned>12</BytesScanned><BytesProcessed>12</BytesProcessed><BytesReturned>8</BytesReturned></Stats>"

    bytes =
      event("Records", "a\npar") <>
        event("Cont", "") <> event("Records", "t\n") <> event("Stats", stats) <> event("End", "")

    {endpoint, _server} =
      WireServer.fixture([{200, [{"Content-Type", "application/vnd.amazon.eventstream"}], bytes}])

    input = %{
      "Bucket" => "bucket",
      "Key" => "key% +",
      "Expression" => "SELECT * FROM s3object",
      "ExpressionType" => "SQL",
      "InputSerialization" => %{"CSV" => %{}},
      "OutputSerialization" => %{"CSV" => %{}}
    }

    reducer = fn
      %{type: "Records", payload: payload}, acc -> {:ok, acc <> payload}
      _, acc -> {:ok, acc}
    end

    assert {:ok, "a\npart\n",
            %{
              record_bytes: 7,
              events: 5,
              stats: %{"BytesScanned" => 12, "BytesProcessed" => 12, "BytesReturned" => 8}
            }} = Select.call(%Context{endpoint: endpoint, unsigned: true}, input, "", reducer)

    assert_receive {:wire_request, %{method: "POST", target: target, body: body}}
    assert target == "/bucket/key%25%20%2B?select=&select-type=2"
    assert body =~ "<Expression>SELECT * FROM s3object</Expression>"
  end

  test "HTTP-200 error/exception events are safe service failures" do
    for type <- ["error", "exception"] do
      headers = [
        {":message-type", type},
        {if(type == "error", do: ":error-code", else: ":exception-type"), "AccessDenied"},
        {":error-message", "secret token\e[31m"}
      ]

      response = %Response{status: 200, body: frame(headers, "secret payload")}

      assert {:error, %Error{category: :service, code: "AccessDenied", http_status: 200} = error} =
               Select.consume(response, nil, fn _, _ -> flunk("error dispatched") end)

      refute inspect(error) =~ "secret"
      refute inspect(error) =~ "payload"
    end
  end

  test "missing End, truncation, events after End and invalid stats cannot confirm success" do
    reducer = fn _, acc -> {:ok, acc} end

    assert {:error, %Error{code: :missing_select_end}} =
             Select.consume(%Response{body: event("Records", "partial")}, nil, reducer)

    bytes = event("Records", "partial") <> binary_part(event("End", ""), 0, 10)

    assert {:error, %Error{code: :truncated_event_stream}} =
             Select.consume(%Response{body: bytes}, nil, reducer)

    assert {:error, %Error{code: :event_after_select_end}} =
             Select.consume(
               %Response{body: event("End", "") <> event("Records", "late")},
               nil,
               reducer
             )

    assert {:error, %Error{code: :invalid_select_event}} =
             Select.consume(%Response{body: event("End", "not empty")}, nil, reducer)

    assert {:error, %Error{code: :invalid_select_details}} =
             Select.consume(
               %Response{body: event("Stats", "<Stats><BytesScanned>-1</BytesScanned></Stats>")},
               nil,
               reducer
             )

    assert {:error, %Error{code: :invalid_xml}} =
             Select.consume(
               %Response{body: event("Stats", "<!DOCTYPE Stats><Stats/>")},
               nil,
               reducer
             )
  end

  test "callback cancellation aborts the owned stream and closes its controller" do
    controller = HTTP.AbortController.new()
    parent = self()
    bytes = event("Records", "unpublished") <> event("End", "")

    stream =
      spawn(fn ->
        receive do
          {:read_chunk, caller, :ack} ->
            send(caller, {:stream_chunk, self(), bytes, make_ref()})
            send(parent, {:opened, self()})

            receive do
              _ -> Process.sleep(:infinity)
            end
        end
      end)

    assert {:error, %Error{category: :interrupted}} =
             Select.consume(%Response{body: stream, controller: controller}, nil, fn _, _ ->
               {:error, Error.new(:interrupted, "cancelled")}
             end)

    assert_received {:opened, ^stream}
    refute Process.alive?(stream)
    refute Process.alive?(controller)
  end

  defp event(type, payload),
    do: frame([{":message-type", "event"}, {":event-type", type}], payload)

  defp frame(headers, payload) do
    headers =
      Enum.map_join(headers, fn {key, value} ->
        <<byte_size(key), key::binary, 7, byte_size(value)::16, value::binary>>
      end)

    prelude = <<byte_size(headers) + byte_size(payload) + 16::32, byte_size(headers)::32>>
    bytes = prelude <> <<:erlang.crc32(prelude)::32>> <> headers <> payload
    bytes <> <<:erlang.crc32(bytes)::32>>
  end
end
