defmodule ExStorageServiceCli.SigV4ReferenceTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.SigV4
  alias ExStorageServiceCli.S3.Encoding

  @fixture __DIR__
           |> Path.join("../fixtures/sigv4/vectors.json")
           |> File.read!()
           |> JSON.decode!()

  for vector <- @fixture["vectors"] do
    @vector vector
    test "matches independent botocore vector: #{vector["name"]}" do
      vector = @vector

      opts = [
        access_key_id: @fixture["credentials"]["access_key_id"],
        secret_access_key: @fixture["credentials"]["secret_access_key"],
        now: ~U[2024-01-15 12:00:00Z],
        session_token: vector["session_token"]
      ]

      if vector["presign"] do
        assert SigV4.presign_url(vector["method"], vector["url"], opts) == vector["expected"]
      else
        headers = Enum.map(vector["headers"], &List.to_tuple/1)
        body = Base.decode64!(vector["body_base64"])
        signed = SigV4.sign_headers(vector["method"], vector["url"], headers, body, opts)
        assert signed |> Map.new() |> Map.fetch!("authorization") == vector["expected"]

        if token = vector["session_token"] do
          assert {"x-amz-security-token", token} in signed
        end
      end
    end
  end

  test "RFC3986 encoding preserves raw key bytes, duplicates, and copy versions" do
    assert Encoding.path("a%+?# space/中文//./../end") ==
             "a%25%2B%3F%23%20space/%E4%B8%AD%E6%96%87//./../end"

    assert Encoding.query([{"same", "z"}, {"same", "a +"}, {"é", ""}]) ==
             "%C3%A9=&same=a%20%2B&same=z"

    assert Encoding.copy_source("bucket", "a%+?# /./../", "v +/=") ==
             "/bucket/a%25%2B%3F%23%20/./../?versionId=v%20%2B%2F%3D"
  end

  test "precomputed payload hash avoids touching streaming bodies" do
    opts = [access_key_id: "key", secret_access_key: "secret", now: ~U[2024-01-15 12:00:00Z]]
    hash = :crypto.hash(:sha256, <<0, 255>>) |> Base.encode16(case: :lower)
    expected = SigV4.sign_headers("PUT", "https://example.com/key", [], <<0, 255>>, opts)

    actual =
      SigV4.sign_headers(
        "PUT",
        "https://example.com/key",
        [],
        :stream,
        Keyword.put(opts, :payload_hash, hash)
      )

    assert actual == expected
  end

  test "invalid/incomplete percent escapes are encoded once without normalizing paths" do
    opts = [access_key_id: "key", secret_access_key: "secret", now: ~U[2024-01-15 12:00:00Z]]

    for {raw, encoded} <- [{"%", "%25"}, {"%2", "%252"}, {"%zz", "%25zz"}] do
      assert SigV4.sign_headers("GET", "https://example.com/a//./../#{raw}", [], "", opts) ==
               SigV4.sign_headers("GET", "https://example.com/a//./../#{encoded}", [], "", opts)
    end
  end

  test "generated headers replace stale signing headers and skip mutable transport headers" do
    opts = [
      access_key_id: "key",
      secret_access_key: "secret",
      now: ~U[2024-01-15 12:00:00Z],
      session_token: "new"
    ]

    stale = [
      {"Authorization", "old"},
      {"Host", "wrong"},
      {"X-Amz-Date", "old"},
      {"X-Amz-Content-Sha256", "old"},
      {"X-Amz-Security-Token", "old"},
      {"User-Agent", "changing"},
      {"Transfer-Encoding", "chunked"},
      {"Expect", "100-continue"}
    ]

    assert SigV4.sign_headers("GET", "https://example.com", stale, "", opts) ==
             SigV4.sign_headers("GET", "https://example.com/", [], "", opts)
  end

  test "expiry and payload overrides fail explicitly" do
    opts = [access_key_id: "key", secret_access_key: "secret"]

    for expiry <- [0, -1, 604_801, "3600", nil] do
      assert_raise ArgumentError, fn ->
        SigV4.presign_url("GET", "https://example.com/", Keyword.put(opts, :expires, expiry))
      end
    end

    for hash <- ["bad", nil, 123] do
      assert_raise ArgumentError, fn ->
        SigV4.sign_headers(
          "PUT",
          "https://example.com/",
          [],
          :stream,
          Keyword.put(opts, :payload_hash, hash)
        )
      end
    end
  end
end
