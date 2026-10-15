defmodule ExStorageServiceCli.S3.SigV4aTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.S3.{Encoding, SigV4a}

  @vectors __DIR__
           |> Path.join("../../fixtures/sigv4a/vectors.json")
           |> File.read!()
           |> JSON.decode!()

  @crt __DIR__
       |> Path.join("../../fixtures/sigv4a/crt-vectors.json")
       |> File.read!()
       |> JSON.decode!()

  for vector <- @crt["vectors"] do
    @vector vector
    test "pinned CRT S3 vector: #{vector["name"]}, presign=#{vector["presign"]}" do
      vector = @vector

      opts = [
        access_key_id: @crt["credentials"]["access_key_id"],
        secret_access_key: @crt["credentials"]["secret_access_key"],
        signing_region_set: String.split(vector["region_set"], ","),
        session_token: vector["token"],
        payload_hash: vector["payload_hash"],
        now: ~U[2024-01-15 12:00:00Z]
      ]

      public = Base.decode16!(vector["public_key"], case: :mixed)
      assert verify(vector["signature"], vector["string_to_sign"], public)
      headers = Enum.map(vector["headers"], &List.to_tuple/1)

      signature =
        if vector["presign"] do
          url =
            SigV4a.presign_url(
              vector["method"],
              vector["url"],
              Keyword.put(opts, :headers, headers)
            )

          uri = URI.parse(url)
          assert uri.path == URI.parse(vector["url"]).path
          query = uri.query |> Encoding.query_pairs()

          assert Enum.filter(query, fn {key, _} -> key in ["dup", "Param1"] end) ==
                   URI.parse(vector["url"]).query
                   |> Encoding.query_pairs()
                   |> Enum.filter(fn {key, _} -> key in ["dup", "Param1"] end)
                   |> Enum.sort()

          query |> Map.new() |> Map.fetch!("X-Amz-Signature")
        else
          stale = [
            {"Authorization", "stale"},
            {"X-Amz-Security-Token", "stale"},
            {"X-Amz-Region-Set", "wrong"},
            {"User-Agent", "mutable"}
          ]

          signed =
            SigV4a.sign_headers(vector["method"], vector["url"], headers ++ stale, :stream, opts)

          assert {"x-amz-region-set", vector["region_set"]} in signed
          assert {"x-amz-content-sha256", vector["payload_hash"]} in signed

          assert Enum.filter(signed, fn {key, _} -> key == "x-amz-security-token" end) ==
                   if(vector["token"], do: [{"x-amz-security-token", vector["token"]}], else: [])

          signed
          |> Map.new()
          |> Map.fetch!("authorization")
          |> String.split("Signature=", parts: 2)
          |> List.last()
        end

      assert verify(signature, vector["string_to_sign"], public)
      refute verify(signature, vector["string_to_sign"] <> "changed", public)
    end
  end

  for vector <- @vectors["vectors"] do
    @vector vector
    test "AWS c-auth header vector: #{vector["name"]}" do
      vector = @vector
      opts = options(vector)
      public = public(vector)
      assert SigV4a.public_key(opts) == public
      assert verify(vector["header_signature"], vector["header_string_to_sign"], public)

      signed =
        SigV4a.sign_headers(
          vector["method"],
          vector["url"],
          headers(vector),
          vector["body"],
          opts
        )

      authorization = Map.fetch!(Map.new(signed), "authorization")
      [_, signature] = String.split(authorization, "Signature=", parts: 2)
      assert verify(signature, vector["header_string_to_sign"], public)

      assert authorization =~
               "AWS4-ECDSA-P256-SHA256 Credential=AKIDEXAMPLE/20150830/service/aws4_request,"

      refute authorization =~ "/us-east-1/"
    end

    test "AWS c-auth presign vector: #{vector["name"]}" do
      vector = @vector

      opts =
        options(vector)
        |> Keyword.put(:headers, headers(vector))
        |> Keyword.put(
          :payload_hash,
          List.last(String.split(vector["query_canonical_request"], "\n"))
        )

      public = public(vector)
      assert verify(vector["query_signature"], vector["query_string_to_sign"], public)
      signed = SigV4a.presign_url(vector["method"], vector["url"], opts)
      query = signed |> URI.parse() |> Map.fetch!(:query) |> Encoding.query_pairs() |> Map.new()
      assert verify(query["X-Amz-Signature"], vector["query_string_to_sign"], public)
      assert query["X-Amz-Region-Set"] == vector["context"]["region"]
      assert query["X-Amz-Credential"] == "AKIDEXAMPLE/20150830/service/aws4_request"
    end
  end

  test "key derivation matches the independently coordinated C, Go and IAM fixed vector" do
    opts = [
      access_key_id: "AKISORANDOMAASORANDOM",
      secret_access_key: "q+jcrXGc+0zWN6uzclKVhvMmUsIfRPa4rlRandom"
    ]

    expected =
      "04" <>
        "15d242ceebf8d8169fd6a8b5a746c41140414c3b07579038da06af89190fffcb" <>
        "0515242cedd82e94799482e4c0514b505afccf2c0c98d6a553bf539f424c5ec0"

    assert Base.encode16(SigV4a.public_key(opts), case: :lower) == expected
  end

  test "region sets and streaming hashes are mandatory valid signing inputs" do
    opts = [access_key_id: "key", secret_access_key: "secret"]

    for region <- ["", "us-east-1\r\nheader: injected", ["us-east-1", ""], 42] do
      assert_raise ArgumentError, fn ->
        SigV4a.sign_headers(
          "GET",
          "https://example.com",
          [],
          "",
          Keyword.put(opts, :signing_region_set, region)
        )
      end
    end

    for expiry <- [0, -1, 604_801, "3600"] do
      assert_raise ArgumentError, fn ->
        SigV4a.presign_url("GET", "https://example.com", Keyword.put(opts, :expires, expiry))
      end
    end

    for hash <- [nil, "invalid", 42] do
      assert_raise ArgumentError, fn ->
        SigV4a.sign_headers(
          "PUT",
          "https://example.com",
          [],
          :stream,
          Keyword.put(opts, :payload_hash, hash)
        )
      end
    end
  end

  defp options(vector) do
    context = vector["context"]
    {:ok, now, 0} = DateTime.from_iso8601(context["timestamp"])

    [
      access_key_id: context["credentials"]["access_key_id"],
      secret_access_key: context["credentials"]["secret_access_key"],
      session_token: context["credentials"]["token"],
      signing_region_set: context["region"],
      service: context["service"],
      now: now,
      expires: context["expiration_in_seconds"],
      normalize_uri_path: context["normalize"],
      double_uri_encode: true,
      omit_session_token: context["omit_session_token"] || false,
      include_content_sha256: context["sign_body"]
    ]
  end

  defp headers(vector), do: Enum.map(vector["headers"], &List.to_tuple/1)

  defp public(vector),
    do:
      Base.decode16!("04" <> vector["public_key"]["X"] <> vector["public_key"]["Y"], case: :mixed)

  defp verify(signature, message, public),
    do:
      :crypto.verify(:ecdsa, :sha256, message, Base.decode16!(signature, case: :mixed), [
        public,
        :secp256r1
      ])
end
