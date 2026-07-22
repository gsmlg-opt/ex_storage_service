defmodule ExStorageServiceCluster.InternalAuthTest do
  use ExUnit.Case, async: true

  import Plug.Conn
  import Plug.Test

  alias ExStorageServiceCluster.InternalAuth

  @hash String.duplicate("a", 64)
  @secret "a sufficiently long shared test secret"
  @timestamp 1_752_000_000
  @request_id "request-id-00000001"
  @path "/internal/v1/blobs/#{@hash}"

  setup do
    table = :ets.new(__MODULE__, [:set, :public, write_concurrency: true])
    %{table: table}
  end

  test "sign emits deterministic headers for the documented canonical form" do
    headers =
      InternalAuth.sign(:put, String.upcase(@hash), 12, @secret,
        path: @path,
        timestamp: @timestamp,
        request_id: @request_id,
        range: "BYTES = 1 - 9"
      )

    canonical =
      Enum.join(
        [
          "ESS-HMAC-SHA256",
          "PUT",
          @path,
          Integer.to_string(@timestamp),
          @request_id,
          @hash,
          "12",
          "bytes=1-9"
        ],
        "\n"
      )

    signature =
      :crypto.mac(:hmac, :sha256, @secret, canonical)
      |> Base.encode16(case: :lower)

    assert [
             {"x-ess-timestamp", "1752000000"},
             {"x-ess-request-id", @request_id},
             {"x-ess-blob-sha256", @hash},
             {"x-ess-blob-size", "12"},
             {"authorization", "ESS-HMAC-SHA256 #{signature}"},
             {"range", "bytes=1-9"}
           ] == headers
  end

  test "verify accepts a valid signed range request and returns claims", %{table: table} do
    conn = signed_conn("GET", "-", range: "bytes=10-19")

    assert {:ok, %{request_id: @request_id, timestamp: @timestamp}} =
             InternalAuth.verify(conn, :get, @hash, "-", verify_opts(table))
  end

  test "verify rejects duplicate required and range headers", %{table: table} do
    conn = signed_conn("GET", "-")

    duplicate_timestamp = %{
      conn
      | req_headers: [{"x-ess-timestamp", "1752000000"} | conn.req_headers]
    }

    assert {:error, {:duplicate_header, "x-ess-timestamp"}} =
             InternalAuth.verify(duplicate_timestamp, "GET", @hash, "-", verify_opts(table))

    conn = signed_conn("GET", "-", range: "bytes=0-1")
    duplicate_range = %{conn | req_headers: [{"range", "bytes=2-3"} | conn.req_headers]}

    assert {:error, {:duplicate_header, "range"}} =
             InternalAuth.verify(duplicate_range, "GET", @hash, "-", verify_opts(table))
  end

  test "verify rejects missing and malformed authentication headers", %{table: table} do
    conn = signed_conn("HEAD", "-")

    missing = delete_req_header(conn, "x-ess-request-id")

    assert {:error, {:missing_header, "x-ess-request-id"}} =
             InternalAuth.verify(missing, "HEAD", @hash, "-", verify_opts(table))

    malformed_timestamp = put_req_header(conn, "x-ess-timestamp", "1752000000junk")

    assert {:error, :invalid_timestamp} =
             InternalAuth.verify(malformed_timestamp, "HEAD", @hash, "-", verify_opts(table))

    malformed_request_id = put_req_header(conn, "x-ess-request-id", "too-short")

    assert {:error, :invalid_request_id} =
             InternalAuth.verify(malformed_request_id, "HEAD", @hash, "-", verify_opts(table))

    malformed_authorization = put_req_header(conn, "authorization", "Bearer secret")

    assert {:error, :invalid_authorization} =
             InternalAuth.verify(malformed_authorization, "HEAD", @hash, "-", verify_opts(table))
  end

  test "verify binds method, path, hash, size, and range", %{table: table} do
    conn = signed_conn("PUT", 12)

    assert {:error, :method_mismatch} =
             InternalAuth.verify(conn, "GET", @hash, 12, verify_opts(table))

    assert {:error, :hash_mismatch} =
             InternalAuth.verify(conn, "PUT", String.duplicate("b", 64), 12, verify_opts(table))

    assert {:error, :size_mismatch} =
             InternalAuth.verify(conn, "PUT", @hash, 13, verify_opts(table))

    wrong_path = %{conn | request_path: "/internal/v1/blobs/different"}

    assert {:error, :invalid_signature} =
             InternalAuth.verify(wrong_path, "PUT", @hash, 12, verify_opts(table))

    ranged = signed_conn("GET", "-", range: "bytes=0-9")
    changed_range = put_req_header(ranged, "range", "bytes=1-9")

    assert {:error, :invalid_signature} =
             InternalAuth.verify(changed_range, "GET", @hash, "-", verify_opts(table))
  end

  test "verify rejects clock skew before claiming the request id", %{table: table} do
    conn = signed_conn("GET", "-")

    assert {:error, :clock_skew} =
             InternalAuth.verify(
               conn,
               "GET",
               @hash,
               "-",
               verify_opts(table, now_seconds: @timestamp + 61)
             )

    assert [] == :ets.lookup(table, @request_id)
  end

  test "verify uses constant-time signature validation before replay claim", %{table: table} do
    conn =
      signed_conn("GET", "-")
      |> put_req_header("authorization", "ESS-HMAC-SHA256 #{String.duplicate("0", 64)}")

    assert {:error, :invalid_signature} =
             InternalAuth.verify(conn, "GET", @hash, "-", verify_opts(table))

    assert [] == :ets.lookup(table, @request_id)
  end

  test "verify atomically rejects a replay", %{table: table} do
    conn = signed_conn("HEAD", "-")

    assert {:ok, %{request_id: @request_id}} =
             InternalAuth.verify(conn, "HEAD", @hash, "-", verify_opts(table))

    assert {:error, :replayed_request} =
             InternalAuth.verify(conn, "HEAD", @hash, "-", verify_opts(table))
  end

  test "future-dated requests remain claimed for their full validity window", %{table: table} do
    future_timestamp = @timestamp + 60
    conn = signed_conn("HEAD", "-", timestamp: future_timestamp)

    assert {:ok, %{request_id: @request_id}} =
             InternalAuth.verify(conn, "HEAD", @hash, "-", verify_opts(table))

    assert [{@request_id, 131_000}] = :ets.lookup(table, @request_id)
  end

  test "sign rejects malformed caller inputs" do
    assert_raise ArgumentError, fn ->
      InternalAuth.sign("GET", @hash, "-", @secret,
        path: @path,
        timestamp: @timestamp,
        request_id: "short"
      )
    end

    assert_raise ArgumentError, fn ->
      InternalAuth.sign("GET", @hash, "-", @secret,
        path: @path,
        timestamp: @timestamp,
        request_id: @request_id,
        range: "bytes=-"
      )
    end
  end

  defp signed_conn(method, size, opts \\ []) do
    range = Keyword.get(opts, :range)
    timestamp = Keyword.get(opts, :timestamp, @timestamp)

    headers =
      InternalAuth.sign(method, @hash, size, @secret,
        path: @path,
        timestamp: timestamp,
        request_id: @request_id,
        range: range
      )

    Enum.reduce(headers, conn(method, @path), fn {name, value}, conn ->
      put_req_header(conn, name, value)
    end)
  end

  defp verify_opts(table, overrides \\ []) do
    Keyword.merge(
      [
        secret: @secret,
        replay_table: table,
        skew_seconds: 60,
        now_seconds: @timestamp,
        monotonic_now_ms: 10_000
      ],
      overrides
    )
  end
end
