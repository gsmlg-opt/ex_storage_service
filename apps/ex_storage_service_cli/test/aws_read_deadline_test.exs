Code.require_file("support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.AWSReadDeadlineTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, S3.Client, Transport.HTTPFetch, Test.WireServer}
  alias ExStorageServiceCli.CLI.Parser

  test "AWS socket read and connection limits are independent of the overall request" do
    assert %{timeout: :infinity, read_idle_timeout: 60_000, connect_timeout: 60_000} =
             context("http://localhost")

    assert %{timeout: :infinity, read_idle_timeout: nil, connect_timeout: :infinity} =
             context("http://localhost", ["--cli-read-timeout", "0", "--cli-connect-timeout", "0"])

    assert %{timeout: :infinity, read_idle_timeout: 1_000, connect_timeout: 3_000} =
             context("http://localhost", ["--cli-read-timeout", "1", "--cli-connect-timeout", "3"])
  end

  test "positive AWS read timeout resets on bytes while total transfer exceeds that limit" do
    {endpoint, _} =
      peer(fn socket ->
        :ok =
          :gen_tcp.send(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nx"
          )

        for byte <- ["y", "z", "w"] do
          Process.sleep(450)
          :ok = :gen_tcp.send(socket, byte)
        end
      end)

    started = System.monotonic_time(:millisecond)

    assert {:ok, response} =
             Client.call(
               context(endpoint, ["--cli-read-timeout", "1"]),
               "GetObject",
               %{"Bucket" => "b", "Key" => "k"},
               mode: :stream,
               max_attempts: 1
             )

    assert {:ok, "xyzw", %{bytes: 4}} =
             HTTPFetch.consume(response, "", fn bytes, acc -> {:ok, acc <> bytes} end)

    assert System.monotonic_time(:millisecond) - started >= 1_350
    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
  end

  test "positive AWS body read stalls report typed socket idle expiry and close ownership" do
    {endpoint, _} =
      peer(fn socket ->
        :ok =
          :gen_tcp.send(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nx"
          )

        assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_500)
      end)

    assert {:ok, response} =
             Client.call(
               context(endpoint, ["--cli-read-timeout", "1"]),
               "GetObject",
               %{"Bucket" => "b", "Key" => "k"},
               mode: :stream,
               max_attempts: 1
             )

    assert {:error, %Error{code: :read_idle_timeout}} =
             HTTPFetch.consume(response, "", fn bytes, acc -> {:ok, acc <> bytes} end)

    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
  end

  defp context(endpoint, flags \\ []) do
    assert {:ok, invocation} =
             Parser.parse(
               ["s3api", "list-buckets", "--endpoint-url", endpoint, "--no-sign-request"] ++ flags
             )

    assert {:ok, context} =
             Context.aws(invocation.options,
               env: %{},
               config_file: "/nonexistent",
               credentials_file: "/nonexistent"
             )

    context
  end

  defp peer(callback) do
    WireServer.start(fn listener, _owner ->
      {:ok, socket} = :gen_tcp.accept(listener, 5_000)
      WireServer.request(socket)
      callback.(socket)
      :gen_tcp.close(socket)
    end)
  end
end
