defmodule ExStorageServiceS3.Handlers.SharedTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias ExStorageService.BlobStore.Source
  alias ExStorageServiceS3.Handlers.Shared

  defmodule ClosingAdapter do
    def send_chunked(payload, _status, _headers), do: {:ok, "", payload}
    def chunk(_payload, _body), do: {:error, :closed}
  end

  describe "decode_aws_chunked/1" do
    test "decodes well-formed aws-chunked framing" do
      body =
        "5;chunk-signature=abc\r\nhello\r\n" <>
          "6;chunk-signature=def\r\n world\r\n" <>
          "0;chunk-signature=ghi\r\n\r\n"

      assert Shared.decode_aws_chunked(body) == "hello world"
    end

    test "fails closed when the declared chunk size exceeds the data" do
      body = "10;chunk-signature=abc\r\nshort\r\n"
      assert Shared.decode_aws_chunked(body) == {:error, :malformed_chunked}
    end

    test "fails closed on a non-hex chunk header" do
      body = "nothex\r\ndata\r\n"
      assert Shared.decode_aws_chunked(body) == {:error, :malformed_chunked}
    end

    test "fails closed when there is no CRLF terminator" do
      assert Shared.decode_aws_chunked("garbage") == {:error, :malformed_chunked}
    end

    test "decodes framing split across every input boundary" do
      body =
        "5;chunk-signature=abc\r\nhello\r\n" <>
          "6;chunk-signature=def\r\n world\r\n" <>
          "0;chunk-signature=ghi\r\n\r\n"

      chunks = for <<byte <- body>>, do: <<byte>>

      assert chunks
             |> Shared.decode_aws_chunked_stream(11)
             |> Enum.to_list()
             |> IO.iodata_to_binary() == "hello world"
    end

    test "enforces the decoded payload limit while streaming" do
      stream = Shared.decode_aws_chunked_stream(["5\r\nhello\r\n0\r\n\r\n"], 4)
      assert catch_throw(Enum.to_list(stream)) == {:error, :entity_too_large}
    end

    test "rejects a stream that ends before the terminal chunk" do
      stream = Shared.decode_aws_chunked_stream(["5\r\nhello\r\n"], 5)
      assert catch_throw(Enum.to_list(stream)) == {:error, :malformed_chunked}
    end
  end

  describe "xml_has_doctype?/1" do
    test "flags DOCTYPE declarations regardless of case" do
      assert Shared.xml_has_doctype?(~s(<?xml version="1.0"?><!DOCTYPE foo [ ]><Delete/>))
      assert Shared.xml_has_doctype?(~s(<!doctype x>))
    end

    test "flags custom ENTITY declarations" do
      assert Shared.xml_has_doctype?(~s(<!ENTITY xxe SYSTEM "file:///etc/passwd">))
    end

    test "accepts ordinary S3 XML bodies" do
      refute Shared.xml_has_doctype?(
               ~s(<?xml version="1.0"?><Delete><Object><Key>k</Key></Object></Delete>)
             )
    end
  end

  describe "storage_error_response/4" do
    test "maps cluster write availability failures to S3 ServiceUnavailable" do
      for reason <- [
            :blob_write_quorum_unavailable,
            :metadata_quorum_unavailable,
            :all_blob_replicas_unavailable,
            :insufficient_eligible_nodes,
            :cluster_data_plane_disabled,
            :no_leader,
            :cluster_not_ready,
            :timeout,
            :unknown,
            {:commit, :timeout}
          ] do
        response =
          :put
          |> conn("/bucket/key")
          |> Shared.storage_error_response(reason, "/bucket/key", "request-phase6")

        assert response.status == 503
        assert Plug.Conn.get_resp_header(response, "x-amz-request-id") == ["request-phase6"]
        assert response.resp_body =~ "<Code>ServiceUnavailable</Code>"
        assert response.resp_body =~ "<RequestId>request-phase6</RequestId>"
      end
    end

    test "keeps unrelated failures as InternalError" do
      response =
        :put
        |> conn("/bucket/key")
        |> Shared.storage_error_response(:eio, "/bucket/key", "request-phase6")

      assert response.status == 500
      assert response.resp_body =~ "<Code>InternalError</Code>"
    end
  end

  describe "send_blob_source/4" do
    test "threads Plug connection state through a stateful stream" do
      source =
        Source.stateful_stream(
          fn conn, reducer ->
            with {:cont, conn} <- reducer.("stateful ", conn),
                 {:cont, conn} <- reducer.("response", conn) do
              {:ok, conn}
            end
          end,
          17
        )

      response =
        :get
        |> conn("/bucket/key")
        |> Shared.send_blob_source(200, source, request_id: "request-phase7")

      assert response.status == 200
      assert response.state == :chunked
      assert response.resp_body == "stateful response"
      assert Plug.Conn.get_resp_header(response, "content-length") == ["17"]
    end

    test "serves an enumerable stream without buffering it in the handler" do
      source =
        ["enumerable ", "response"]
        |> Stream.map(& &1)
        |> Source.stream(19)

      response =
        :get
        |> conn("/bucket/key")
        |> Shared.send_blob_source(206, source, request_id: "request-phase7-range")

      assert response.status == 206
      assert response.state == :chunked
      assert response.resp_body == "enumerable response"
      assert Plug.Conn.get_resp_header(response, "content-length") == ["19"]
    end

    test "a closed client halts the stateful upstream producer immediately" do
      parent = self()

      source =
        Source.stateful_stream(
          fn conn, reducer ->
            send(parent, :producer_started)

            case reducer.("first", conn) do
              {:halt, reason, conn} ->
                send(parent, {:producer_cancelled, reason})
                {:error, reason, conn}

              {:cont, conn} ->
                send(parent, :producer_continued)
                {:ok, conn}
            end
          end,
          5
        )

      test_conn = conn(:get, "/bucket/key")
      test_conn = %{test_conn | adapter: {ClosingAdapter, %{}}}

      response =
        Shared.send_blob_source(test_conn, 200, source, request_id: "request-phase7-disconnect")

      assert response.status == 200
      assert response.state == :chunked
      assert_receive :producer_started
      assert_receive {:producer_cancelled, :closed}
      refute_receive :producer_continued
    end

    test "an upstream failure aborts instead of finalizing a short success response" do
      source =
        Source.stateful_stream(
          fn conn, _reducer -> {:error, :incomplete_response, conn} end,
          10
        )

      assert_raise RuntimeError,
                   "blob response stream terminated after response headers",
                   fn ->
                     :get
                     |> conn("/bucket/key")
                     |> Shared.send_blob_source(200, source,
                       request_id: "request-phase7-incomplete"
                     )
                   end
    end
  end
end
