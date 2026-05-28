defmodule ExStorageServiceCli.SigV4Test do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.SigV4

  describe "sign_headers/5" do
    test "returns authorization, date, content-sha256, and host headers" do
      headers =
        SigV4.sign_headers("GET", "http://localhost:9000/test-bucket", [], "",
          access_key_id: "AKIAIOSFODNN7EXAMPLE",
          secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          region: "us-east-1",
          now: ~U[2024-01-15 12:00:00Z]
        )

      header_keys = Enum.map(headers, fn {k, _} -> k end) |> Enum.sort()
      assert "authorization" in header_keys
      assert "x-amz-date" in header_keys
      assert "x-amz-content-sha256" in header_keys
      assert "host" in header_keys
    end

    test "authorization header has correct format" do
      headers =
        SigV4.sign_headers("GET", "http://localhost:9000/", [], "",
          access_key_id: "AKIAIOSFODNN7EXAMPLE",
          secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          region: "us-east-1",
          now: ~U[2024-01-15 12:00:00Z]
        )

      {_, auth} = Enum.find(headers, fn {k, _} -> k == "authorization" end)
      assert String.starts_with?(auth, "AWS4-HMAC-SHA256")
      assert auth =~ "Credential=AKIAIOSFODNN7EXAMPLE/"
      assert auth =~ "SignedHeaders="
      assert auth =~ "Signature="
    end

    test "date header matches provided time" do
      headers =
        SigV4.sign_headers("PUT", "http://localhost:9000/bucket/key", [], "body",
          access_key_id: "AKIATEST",
          secret_access_key: "secret",
          region: "eu-west-1",
          now: ~U[2024-06-20 15:30:45Z]
        )

      {_, date} = Enum.find(headers, fn {k, _} -> k == "x-amz-date" end)
      assert date == "20240620T153045Z"
    end

    test "different bodies produce different signatures" do
      opts = [
        access_key_id: "AKIATEST",
        secret_access_key: "secret",
        region: "us-east-1",
        now: ~U[2024-01-15 12:00:00Z]
      ]

      headers1 = SigV4.sign_headers("PUT", "http://localhost:9000/b/k", [], "body1", opts)
      headers2 = SigV4.sign_headers("PUT", "http://localhost:9000/b/k", [], "body2", opts)

      {_, auth1} = Enum.find(headers1, fn {k, _} -> k == "authorization" end)
      {_, auth2} = Enum.find(headers2, fn {k, _} -> k == "authorization" end)

      refute auth1 == auth2
    end

    test "host includes port for non-standard ports" do
      headers =
        SigV4.sign_headers("GET", "http://localhost:9000/", [], "",
          access_key_id: "AKIATEST",
          secret_access_key: "secret",
          region: "us-east-1",
          now: ~U[2024-01-15 12:00:00Z]
        )

      {_, host} = Enum.find(headers, fn {k, _} -> k == "host" end)
      assert host == "localhost:9000"
    end
  end

  describe "presign_url/3" do
    test "generates a valid presigned URL" do
      url =
        SigV4.presign_url("GET", "http://localhost:9000/my-bucket/my-key.txt",
          access_key_id: "AKIAIOSFODNN7EXAMPLE",
          secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          region: "us-east-1",
          expires: 3600,
          now: ~U[2024-01-15 12:00:00Z]
        )

      assert String.starts_with?(url, "http://localhost:9000/my-bucket/my-key.txt?")
      assert url =~ "X-Amz-Algorithm=AWS4-HMAC-SHA256"
      assert url =~ "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE"
      assert url =~ "X-Amz-Date=20240115T120000Z"
      assert url =~ "X-Amz-Expires=3600"
      assert url =~ "X-Amz-SignedHeaders=host"
      assert url =~ "X-Amz-Signature="
    end

    test "different expiry produces different URL" do
      opts = [
        access_key_id: "AKIATEST",
        secret_access_key: "secret",
        region: "us-east-1",
        now: ~U[2024-01-15 12:00:00Z]
      ]

      url1 =
        SigV4.presign_url("GET", "http://localhost:9000/b/k", Keyword.put(opts, :expires, 3600))

      url2 =
        SigV4.presign_url("GET", "http://localhost:9000/b/k", Keyword.put(opts, :expires, 7200))

      refute url1 == url2
    end
  end
end
