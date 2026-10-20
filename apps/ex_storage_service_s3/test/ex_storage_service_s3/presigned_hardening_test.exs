defmodule ExStorageServiceS3.PresignedHardeningTest do
  use ExUnit.Case, async: true
  import Plug.Conn
  alias ExStorageServiceS3.Presigned

  @options [
    access_key_id: "test-access",
    secret_access_key: "test-secret",
    host: "localhost:9000"
  ]

  test "signed custom headers constrain the request" do
    url =
      Presigned.generate_url(
        "bucket",
        "key",
        Keyword.put(@options, :headers, %{"content-type" => "text/plain"})
      )

    good = request(url) |> put_req_header("content-type", "text/plain")
    assert {:ok, _} = Presigned.validate_presigned(good, fn _ -> "test-secret" end)

    assert {:error, "SignatureDoesNotMatch"} =
             Presigned.validate_presigned(
               put_req_header(good, "content-type", "application/json"),
               fn _ -> "test-secret" end
             )
  end

  test "malformed or duplicate authentication query values fail safely" do
    url = Presigned.generate_url("bucket", "key", @options)

    for query <- [
          String.replace(url, ~r/X-Amz-Expires=\d+/, "X-Amz-Expires=nope"),
          url <> "&X-Amz-Expires=10"
        ] do
      assert {:error, _} = Presigned.validate_presigned(request(query), fn _ -> "test-secret" end)
    end
  end

  test "raw special keys are safely encoded once and validate" do
    url = Presigned.generate_url("bucket", "a//./space + %2F/雪", @options)
    assert url =~ "/bucket/a//./space%20%2B%20%252F/%E9%9B%AA?"
    assert {:ok, _} = Presigned.validate_presigned(request(url), fn _ -> "test-secret" end)
  end

  defp request(url) do
    uri = URI.parse(url)

    Plug.Test.conn(:get, uri.path <> "?" <> uri.query)
    |> then(fn conn ->
      %{
        conn
        | host: uri.host,
          port: uri.port,
          req_headers: [{"host", uri.authority} | conn.req_headers]
      }
    end)
    |> fetch_query_params()
  end
end
