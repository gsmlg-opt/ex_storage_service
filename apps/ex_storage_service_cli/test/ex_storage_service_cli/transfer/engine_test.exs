Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transfer.EngineTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Transfer.Engine, Transfer.Sink}
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir

  test "file upload hashes and streams raw content with exact length", %{tmp_dir: root} do
    bytes = :zlib.gzip(:binary.copy(<<0, 255, 128, 1>>, 100_000))
    path = Path.join(root, "source.gz")
    File.write!(path, bytes)
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"uploaded\""}], ""}])
    context = %Context{endpoint: endpoint, access_key_id: "access", secret_access_key: "secret"}

    assert {:ok, %{bytes: size, etag: "uploaded"}} =
             Engine.upload_file(context, "bucket", "a% +", path,
               input: %{"ContentEncoding" => "gzip"}
             )

    assert size == byte_size(bytes)

    assert_receive {:wire_request,
                    %{body: ^bytes, target: "/bucket/a%25%20%2B", headers: headers}}

    assert headers["content-length"] == to_string(size)
    assert headers["content-encoding"] == "gzip"

    assert headers["x-amz-content-sha256"] ==
             Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    refute Map.has_key?(headers, "transfer-encoding")
  end

  test "download verifies raw checksum before replacing destination", %{tmp_dir: root} do
    bytes = :zlib.gzip(<<0, 255, 128, 1>>)
    checksum = Base.encode64(:crypto.hash(:sha256, bytes))

    {endpoint, _} =
      WireServer.fixture([
        {200, [{"Content-Encoding", "gzip"}, {"x-amz-checksum-sha256", checksum}], bytes}
      ])

    path = Path.join(root, "destination.gz")
    File.write!(path, "old")

    assert {:ok, %{bytes: size}} =
             Engine.download_file(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               path
             )

    assert size == byte_size(bytes)
    assert File.read!(path) == bytes
    assert Path.wildcard(Path.join(root, ".ess-download-*")) == []
  end

  test "checksum failure leaves existing destination intact", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([{200, [{"x-amz-checksum-sha256", Base.encode64(<<0::256>>)}], "new"}])

    path = Path.join(root, "destination")
    File.write!(path, "old")

    assert {:error, %Error{code: :checksum_mismatch}} =
             Engine.download_file(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               path
             )

    assert File.read!(path) == "old"
    assert Path.wildcard(Path.join(root, ".ess-download-*")) == []
  end

  test "truncated transfer never publishes a partial file", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.start(fn listener, _owner ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        WireServer.request(socket)

        :gen_tcp.send(
          socket,
          "HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\npartial"
        )

        :gen_tcp.close(socket)
      end)

    path = Path.join(root, "destination")
    File.write!(path, "old")

    assert {:error, %Error{}} =
             Engine.download_file(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               path
             )

    assert File.read!(path) == "old"
    assert Path.wildcard(Path.join(root, ".ess-download-*")) == []
  end

  test "unsafe local mappings and symlink parents fail before networking", %{tmp_dir: root} do
    for key <- [
          "../outside",
          "/absolute",
          "a/../../escape",
          "a/./b",
          "C:/drive",
          "a\\b",
          "a/CON",
          "a/x."
        ] do
      assert {:error, %Error{code: :unsafe_destination}} = Sink.map(root, key)
    end

    target = Path.join(root, "real")
    File.mkdir!(target)
    File.ln_s!(target, Path.join(root, "link"))

    assert {:error, %Error{code: :unsafe_destination}} =
             Engine.download_file(
               %Context{endpoint: "http://127.0.0.1:1", unsigned: true},
               "bucket",
               "key",
               Path.join(root, "link/file")
             )

    assert File.ls!(target) == []
  end
end
