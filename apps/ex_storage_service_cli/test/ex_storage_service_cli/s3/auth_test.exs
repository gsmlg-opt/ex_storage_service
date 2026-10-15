defmodule ExStorageServiceCli.S3.AuthTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, S3.Auth, S3.Endpoint, S3.Encoder, S3.Request}

  test "directory sessions sign the S3 session header and omit the IAM token header" do
    context = %Context{
      access_key_id: "directory-access",
      secret_access_key: "directory-secret",
      session_token: "directory-token"
    }

    request = %Request{method: "GET", headers: [{"x-amz-security-token", "stale-iam-token"}]}
    # Request context creates the session header; IAM-only headers never enter this request.
    request = %{request | headers: []}

    assert {:ok, signed} =
             Auth.prepare(request, "https://bucket.s3express.test/key", context,
               service: "s3express",
               session_token_header: "x-amz-s3session-token",
               now: ~U[2026-10-15 00:00:00Z]
             )

    headers = Map.new(signed.headers)
    assert headers["x-amz-s3session-token"] == "directory-token"
    refute Map.has_key?(headers, "x-amz-security-token")
    assert headers["authorization"] =~ "/s3express/aws4_request"
    assert headers["authorization"] =~ "x-amz-s3session-token"
  end

  test "modeled multi-region access point selects SigV4a and preserves the object key" do
    context = %Context{
      frontend: :aws,
      addressing: :virtual,
      endpoint: "https://s3.us-east-1.amazonaws.com",
      access_key_id: "mrap-access",
      secret_access_key: "mrap-secret",
      session_token: "mrap-token"
    }

    input = %{
      "Bucket" => "arn:aws:s3::123456789012:accesspoint/mrap.alias",
      "Key" => "a//../%+ ?#雪"
    }

    assert {:ok, request} = Encoder.encode("GetObject", input)

    assert {:ok, url, %{auth: :sigv4a, signing_region_set: regions} = contract} =
             Endpoint.resolve(context, request, input)

    assert url ==
             "https://mrap.alias.accesspoint.s3-global.amazonaws.com/a//../%25%2B%20%3F%23%E9%9B%AA"

    assert {:ok, signed} =
             Auth.prepare(request, url, context,
               auth: contract.auth,
               signing_region_set: regions,
               service: contract.service,
               now: ~U[2026-10-15 00:00:00Z]
             )

    headers = Map.new(signed.headers)

    assert headers["authorization"] =~
             "AWS4-ECDSA-P256-SHA256 Credential=mrap-access/20261015/s3/aws4_request"

    assert headers["x-amz-region-set"] == "*"
    assert headers["x-amz-security-token"] == "mrap-token"
  end
end
