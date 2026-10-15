Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transport.HTTPFetchTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, S3.Request, S3.Response, Transport.HTTPFetch}
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transfer.Coordinator

  test "expired admission rejects I/O and releases the newly created controller" do
    {:ok, pool} = start_supervised({Coordinator, []})
    {:ok, lease} = Coordinator.acquire(pool, 65_536)
    Coordinator.release(lease)
    assert %{active: 0} = Coordinator.stats(pool)
    {endpoint, _} = WireServer.fixture([{200, [], "never requested"}])
    request = %Request{method: "GET", path: "/", lease: lease}
    assert {:error, %Error{code: :request_lease_expired}} = HTTPFetch.request(endpoint, request)
    refute_receive {:wire_request, _}
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
  end

  test "stream reservation stays held through consumption and normal cleanup" do
    {:ok, pool} = start_supervised({Coordinator, []})
    {:ok, lease} = Coordinator.acquire(pool, 65_536)
    body = String.duplicate("raw", 100_000)
    {endpoint, _} = WireServer.fixture([{200, [], body}])
    request = %Request{method: "GET", path: "/", lease: lease}
    assert {:ok, response} = HTTPFetch.request(endpoint, request, mode: :stream)
    assert %{active: 1, bytes: 65_536} = Coordinator.stats(pool)

    assert {:ok, total, %{bytes: 300_000}} =
             HTTPFetch.consume(response, 0, fn chunk, total -> {:ok, total + byte_size(chunk)} end)

    assert total == 300_000
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
    refute Process.alive?(response.controller)
  end

  test "zero-timeout compatibility reports the specific upstream blocker before I/O" do
    {endpoint, _} = WireServer.fixture([{200, [], "never requested"}])
    request = %Request{method: "GET", path: "/"}

    assert {:error, %Error{code: :upstream_timeout_blocked}} =
             HTTPFetch.request(endpoint, request, timeout: :infinity)

    refute_receive {:wire_request, _}
  end

  test "raw downloads are owned streams and keep the signed path bytes" do
    body = :zlib.gzip("raw object bytes")
    {endpoint, _} = WireServer.fixture([{200, [{"Content-Encoding", "gzip"}], body}])
    request = %Request{method: "GET", path: "/bucket/a//../%25%2B%20%3F%23"}

    assert {:ok, %Response{body: stream} = response} =
             HTTPFetch.request(endpoint <> request.path, request, mode: :stream)

    assert is_pid(stream)

    assert {:ok, chunks, %{bytes: size}} =
             HTTPFetch.consume(response, [], fn chunk, acc -> {:ok, [chunk | acc]} end)

    assert size == byte_size(body)
    assert IO.iodata_to_binary(Enum.reverse(chunks)) == body
    assert_receive {:wire_request, %{target: "/bucket/a//../%25%2B%20%3F%23"}}
    refute Process.alive?(response.controller)
  end

  test "bounded control reads fail before oversized bodies can accumulate" do
    {endpoint, _} = WireServer.fixture([{200, [], String.duplicate("x", 10_000)}])
    request = %Request{method: "GET", path: "/"}

    assert {:error, %Error{code: :control_limit}} =
             HTTPFetch.request(endpoint, request, max_control_bytes: 100)
  end

  test "signed redirects are returned without forwarding credentials" do
    {destination, _} = WireServer.fixture([{200, [], "wrong"}])
    {endpoint, _} = WireServer.fixture([{307, [{"Location", destination}], ""}])
    request = %Request{method: "GET", path: "/", headers: [{"Authorization", "private-fixture"}]}
    assert {:ok, %Response{status: 307}} = HTTPFetch.request(endpoint, request)
    assert_receive {:wire_request, %{headers: %{"authorization" => "private-fixture"}}}
    refute_receive {:wire_request, _}, 100
  end

  test "sink errors close the owned stream and controller" do
    {endpoint, _} = WireServer.fixture([{200, [], String.duplicate("x", 100_000)}])
    request = %Request{method: "GET", path: "/"}
    assert {:ok, response} = HTTPFetch.request(endpoint, request, mode: :stream)
    monitor = Process.monitor(response.body)
    failure = Error.new(:client, "Sink refused data")

    assert {:error, ^failure} =
             HTTPFetch.consume(response, nil, fn _chunk, _state -> {:error, failure} end)

    assert_receive {:DOWN, ^monitor, :process, _, _}, 1000
    refute Process.alive?(response.controller)
  end

  test "HEAD advertises object size without reading or applying the control body limit" do
    {endpoint, _} = WireServer.fixture([{200, [{"Content-Length", "10737418240"}], ""}])
    request = %Request{method: "HEAD", path: "/bucket/key"}

    assert {:ok, %Response{body: ""}} =
             HTTPFetch.request(endpoint <> request.path, request, max_control_bytes: 100)
  end
end
