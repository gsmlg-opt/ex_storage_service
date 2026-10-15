Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.S3.WriteResponseTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, S3.WriteResponse}
  alias ExStorageServiceCli.Test.WireServer

  setup_all do
    {:ok, _} = Application.ensure_all_started(:http_fetch)
    :ok
  end

  test "binary response preserves raw encoded bytes, forwarding headers and declared length" do
    body = :zlib.gzip(<<0, 255, "object bytes">>)
    {endpoint, _} = WireServer.fixture([{200, [], ""}])

    context = %Context{
      endpoint: String.replace(endpoint, "127.0.0.1", "0.0.1"),
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret",
      session_token: "fixture-token"
    }

    input = %{
      "RequestRoute" => "127",
      "RequestToken" => "request-token",
      "Body" => body,
      "ContentEncoding" => "gzip",
      "ContentType" => "application/octet-stream",
      "Metadata" => %{"name" => "value"}
    }

    assert {:ok, %{}} = WriteResponse.call(context, input)

    assert_receive {:wire_request,
                    %{
                      method: "POST",
                      target: "/WriteGetObjectResponse",
                      headers: headers,
                      body: ^body
                    }}

    assert headers["content-length"] == to_string(byte_size(body))
    assert headers["x-amz-fwd-header-content-encoding"] == "gzip"
    assert headers["x-amz-meta-name"] == "value"
    assert headers["x-amz-request-token"] == "request-token"
    assert headers["x-amz-security-token"] == "fixture-token"

    assert headers["x-amz-content-sha256"] ==
             Base.encode16(:crypto.hash(:sha256, body), case: :lower)

    assert headers["host"] ==
             URI.parse(endpoint).host <> ":" <> to_string(URI.parse(endpoint).port)
  end

  test "bounded source response retains Content-Length and never base64 encodes raw body" do
    {endpoint, _} = WireServer.fixture([{200, [], ""}])
    input = %{"RequestRoute" => "127", "RequestToken" => "request-token", "ContentLength" => 4}

    assert {:ok, %{}} =
             WriteResponse.call(
               %Context{endpoint: String.replace(endpoint, "127.0.0.1", "0.0.1"), unsigned: true},
               input,
               body_source: [<<0, 255>>, "ab"]
             )

    assert_receive {:wire_request, %{headers: headers, body: <<0, 255, "ab">>}}
    assert headers["content-length"] == "4"
  end

  test "malformed route and inconsistent body lengths fail before transport" do
    for route <- [
          "",
          "a.b",
          "https://bad",
          "bad\r\nhost",
          String.duplicate("a", 64),
          "-bad",
          "bad-",
          nil
        ] do
      assert {:error, %Error{code: :invalid_request_route}} = WriteResponse.route(route)
    end

    input = %{"RequestRoute" => "route", "RequestToken" => "token"}

    assert {:error, %Error{code: :stream_length_required}} =
             WriteResponse.call(%Context{}, input, body_source: ["bytes"])

    assert {:error, %Error{code: :conflicting_response_body}} =
             WriteResponse.call(%Context{}, Map.put(input, "Body", "bytes"),
               body_source: ["bytes"],
               content_length: 5
             )

    assert {:error, %Error{code: :response_length_mismatch}} =
             WriteResponse.call(
               %Context{},
               Map.merge(input, %{"Body" => "bytes", "ContentLength" => 6})
             )

    assert {:error, %Error{code: :response_length_mismatch}} =
             WriteResponse.call(%Context{}, Map.put(input, "ContentLength", 6),
               body_source: ["bytes"],
               content_length: 5
             )
  end

  test "signed HTTP streams need a replayable precomputed payload hash" do
    input = %{"RequestRoute" => "route", "RequestToken" => "token"}

    assert {:error, %Error{code: :payload_hash_required}} =
             WriteResponse.call(%Context{}, input, body_source: ["bytes"], content_length: 5)

    refute_receive {:wire_request, _}
  end
end
