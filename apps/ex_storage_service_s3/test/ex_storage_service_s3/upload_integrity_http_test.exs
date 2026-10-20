defmodule ExStorageServiceS3.UploadIntegrityHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Versioning
  alias ExStorageServiceS3.Presigned
  @port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base "http://localhost:#{@port}"

  setup do
    old_auth = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    bucket = "integrity-http-#{suffix}"
    {:ok, user} = User.create_user("integrity-#{suffix}")
    {:ok, key} = AccessKey.create_access_key(user.id)

    {:ok, policy} =
      Policy.create_policy("integrity-#{suffix}", [
        %{effect: :allow, actions: ["s3:*"], resources: ["*"]}
      ])

    :ok = Policy.attach_policy(user.id, policy.id)
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      Application.put_env(:ex_storage_service, :s3_auth_enabled, old_auth)
      AccessKey.delete_key(key.access_key_id)
      Policy.delete_policy(policy.id)
      Concord.delete("user_policies:#{user.id}")
      User.delete_user(user.id)
      # Objects/versions belong to the disposable test root, never a live service.
    end)

    %{bucket: bucket, key: key, user: user}
  end

  test "a valid signature cannot authorize altered bytes or create a version", context do
    path = "/#{context.bucket}/object"
    :ok = Versioning.set_versioning(context.bucket, :enabled)
    assert %{status: 200} = request(context, :put, path, "original")
    {:ok, before} = Versioning.list_versions(context.bucket, "object")

    assert %{status: 400, body: body} =
             request(context, :put, path, "tampered", [], hash("intended"))

    assert body =~ "XAmzContentSHA256Mismatch"
    assert {:ok, ^before} = Versioning.list_versions(context.bucket, "object")
    assert %{status: 200, body: "original"} = request(context, :get, path)
  end

  test "HTTP checksum, precondition, unknown operation and returned 404 have truthful metrics",
       context do
    :ok = ExStorageService.Metrics.setup()
    :ok = ExStorageService.Metrics.reset()
    path = "/#{context.bucket}/metrics-object"
    assert %{status: 200} = request(context, :put, path, "original")
    assert %{status: 400} = request(context, :put, path, "tampered", [], hash("intended"))
    assert %{status: 412} = request(context, :put, path, "replacement", [{"if-none-match", "*"}])
    assert %{status: 200, body: "original"} = request(context, :get, path)
    assert %{status: 404} = request(context, :get, "/#{context.bucket}/missing")
    assert %{status: 501} = request(context, :get, path, "", [{"x-amz-acl", "private"}])
    output = ExStorageService.Metrics.format_metrics()
    assert output =~ ~s(s3_errors_total{category="integrity"} 1)
    assert output =~ ~s(s3_errors_total{category="precondition"} 1)
    assert output =~ ~s(s3_errors_total{category="unsupported"} 1)
    assert output =~ ~s(s3_requests_total{operation="get_object",status="error"} 1)
    assert output =~ ~s(s3_requests_total{operation="get_object",status="ok"} 1)
    refute output =~ context.bucket
    refute output =~ context.key.secret_access_key
  end

  test "all required wire algorithms and Content-MD5 validate on actual HTTP", context do
    vectors = [
      {"content-md5", Base.encode64(:crypto.hash(:md5, "123456789"))},
      {"x-amz-checksum-crc32", "y/Q5Jg=="},
      {"x-amz-checksum-crc32c", "4waSgw=="},
      {"x-amz-checksum-crc64nvme", "rosUhgp5mIg="},
      {"x-amz-checksum-sha1", "98O8HYCOBHMq32eZZczDTKeuNEE="},
      {"x-amz-checksum-sha256", "FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU="}
    ]

    for {header, value} <- vectors do
      path = "/#{context.bucket}/#{header}"
      assert %{status: 200} = request(context, :put, path, "123456789", [{header, value}])
      assert %{status: 400} = request(context, :put, path, "923456789", [{header, value}])
      assert %{status: 200, body: "123456789"} = request(context, :get, path)
    end

    assert %{status: 400} =
             request(context, :put, "/#{context.bucket}/unknown", "body", [
               {"x-amz-checksum-unknown", "AA=="}
             ])

    assert {:error, :not_found} = Metadata.get_object_meta(context.bucket, "unknown")
  end

  test "unsigned trailers reject corruption, truncation and post-terminal bytes", context do
    mode = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"

    headers = [
      {"content-encoding", "aws-chunked"},
      {"x-amz-decoded-content-length", "9"},
      {"x-amz-trailer", "x-amz-checksum-crc32"}
    ]

    encoded = "9\r\n123456789\r\n0\r\nx-amz-checksum-crc32: y/Q5Jg==\r\n\r\n"
    path = "/#{context.bucket}/trailers"
    assert %{status: 200} = request(context, :put, path, encoded, headers, mode)

    for body <- [
          String.replace(encoded, "123456789", "923456789"),
          String.trim_trailing(encoded),
          encoded <> "garbage"
        ] do
      assert %{status: 400} = request(context, :put, path, body, headers, mode)
      assert %{status: 200, body: "123456789"} = request(context, :get, path)
    end

    assert %{status: 400} =
             request(
               context,
               :put,
               path,
               encoded,
               Enum.reject(headers, fn {name, _} -> name == "x-amz-decoded-content-length" end),
               mode
             )
  end

  test "signed streaming chunks verify each signature and terminal signature", context do
    path = "/#{context.bucket}/signed-chunks"
    headers = [{"content-encoding", "aws-chunked"}, {"x-amz-decoded-content-length", "11"}]

    {signed_headers, signing} =
      sign(context, :put, path, headers, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")

    {chunks, previous} =
      Enum.map_reduce(["hello", " world", ""], signing.signature, fn bytes, previous ->
        signature =
          hmac_hex(
            signing.key,
            Enum.join(
              [
                "AWS4-HMAC-SHA256-PAYLOAD",
                signing.datetime,
                signing.scope,
                previous,
                hash(""),
                hash(bytes)
              ],
              "\n"
            )
          )

        {Integer.to_string(byte_size(bytes), 16) <>
           ";chunk-signature=" <> signature <> "\r\n" <> bytes <> "\r\n", signature}
      end)

    assert byte_size(previous) == 64
    encoded = IO.iodata_to_binary(chunks)
    assert %{status: 200} = raw(:put, path, encoded, signed_headers)

    for {bytes, status} <- [
          {String.replace(encoded, "hello", "jello"), 403},
          {String.replace(encoded, previous, String.duplicate("0", 64)), 403},
          {encoded <> "extra", 400}
        ] do
      assert %{status: ^status} = raw(:put, path, bytes, signed_headers)
      assert %{status: 200, body: "hello world"} = request(context, :get, path)
    end
  end

  test "presigned headers, signature parameters and key revocation remain enforced", context do
    path = "/#{context.bucket}/presigned"

    options = [
      method: "PUT",
      access_key_id: context.key.access_key_id,
      secret_access_key: context.key.secret_access_key,
      host: "localhost:#{@port}",
      headers: %{"content-type" => "text/plain"}
    ]

    url = Presigned.generate_url(context.bucket, "presigned", options)

    assert %{status: 200} =
             Req.put!(url,
               body: "presigned body",
               headers: [{"content-type", "text/plain"}],
               retry: false,
               decode_body: false
             )

    assert %{status: 403} =
             Req.put!(url,
               body: "bad",
               headers: [{"content-type", "application/json"}],
               retry: false,
               decode_body: false
             )

    assert %{status: 403} =
             Req.put!(url <> "&response-content-type=text%2Fhtml",
               body: "bad",
               headers: [{"content-type", "text/plain"}],
               retry: false,
               decode_body: false
             )

    :ok = AccessKey.delete_key(context.key.access_key_id)

    assert %{status: 403} =
             Req.put!(url,
               body: "bad",
               headers: [{"content-type", "text/plain"}],
               retry: false,
               decode_body: false
             )

    assert {:ok, %{size: 14}} = Metadata.get_object_meta(context.bucket, "presigned")
    assert String.ends_with?(path, "/presigned")
  end

  defp request(context, method, path, body \\ "", headers \\ [], payload \\ nil) do
    {headers, _} = sign(context, method, path, headers, payload || hash(body))
    raw(method, path, body, headers)
  end

  defp raw(method, path, body, headers),
    do:
      Req.request!(
        url: @base <> path,
        method: method,
        body: body,
        headers: headers,
        retry: false,
        decode_body: false,
        redirect: false
      )

  # Independent signer: no ESS canonicalization or signing helpers are used.
  defp sign(context, method, path, headers, payload) do
    datetime = Calendar.strftime(DateTime.utc_now(), "%Y%m%dT%H%M%SZ")
    date = binary_part(datetime, 0, 8)
    scope = date <> "/us-east-1/s3/aws4_request"

    all =
      [
        {"host", "localhost:#{@port}"},
        {"x-amz-content-sha256", payload},
        {"x-amz-date", datetime} | headers
      ]
      |> Enum.sort()

    names = Enum.map_join(all, ";", &elem(&1, 0))

    canonical_headers =
      Enum.map_join(all, "", fn {name, value} -> name <> ":" <> String.trim(value) <> "\n" end)

    canonical =
      Enum.join(
        [String.upcase(to_string(method)), path, "", canonical_headers, names, payload],
        "\n"
      )

    signing_key =
      Enum.reduce(
        [date, "us-east-1", "s3", "aws4_request"],
        "AWS4" <> context.key.secret_access_key,
        fn value, key -> :crypto.mac(:hmac, :sha256, key, value) end
      )

    signature =
      hmac_hex(
        signing_key,
        Enum.join(["AWS4-HMAC-SHA256", datetime, scope, hash(canonical)], "\n")
      )

    auth =
      "AWS4-HMAC-SHA256 Credential=#{context.key.access_key_id}/#{scope}, SignedHeaders=#{names}, Signature=#{signature}"

    {[{"authorization", auth} | Enum.reject(all, fn {name, _} -> name == "host" end)],
     %{key: signing_key, signature: signature, datetime: datetime, scope: scope}}
  end

  defp hash(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

  defp hmac_hex(key, bytes),
    do: Base.encode16(:crypto.mac(:hmac, :sha256, key, bytes), case: :lower)
end
