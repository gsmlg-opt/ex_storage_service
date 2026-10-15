Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transfer.DownloadChecksumTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, S3.Checksum, S3.Response}
  alias ExStorageServiceCli.Transfer.{Coordinator, DownloadChecksum, Engine}
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir

  for algorithm <- ~w(CRC32 CRC32C CRC64NVME SHA1 SHA256) do
    @algorithm algorithm
    test "full-object and declared trailer verification #{@algorithm}" do
      bytes = <<0, 255, 128, 1, 13, 10>>
      {:ok, raw} = Checksum.digest(@algorithm, bytes)
      expected = Base.encode64(raw)
      name = Checksum.header(@algorithm)

      for {headers, trailers} <- [
            {[{name, expected}], []},
            {[{"Trailer", name}], [{name, expected}]}
          ] do
        response = %Response{headers: headers}
        assert {:ok, state} = DownloadChecksum.start(%Context{}, %{}, response, [])
        assert {:ok, state} = DownloadChecksum.update(state, binary_part(bytes, 0, 2))
        assert {:ok, state} = DownloadChecksum.update(state, binary_part(bytes, 2, 4))
        assert {:ok, :verified} = DownloadChecksum.finish(state, headers, trailers)

        assert {:error, %Error{code: :conflicting_checksums}} =
                 DownloadChecksum.finish(state, [{name, expected}], [{name, "invalid"}])
      end
    end
  end

  test "missing or unannounced checksum trailers fail" do
    response = %Response{headers: [{"Trailer", "x-amz-checksum-sha256"}]}
    {:ok, state} = DownloadChecksum.start(%Context{}, %{}, response, [])

    assert {:error, %Error{code: :missing_checksum_trailer}} =
             DownloadChecksum.finish(state, response.headers, [])

    {:ok, state} = DownloadChecksum.start(%Context{}, %{}, %Response{headers: []}, [])

    assert {:error, %Error{code: :undeclared_checksum_trailer}} =
             DownloadChecksum.finish(state, [], [{"x-amz-checksum-sha256", "invalid"}])
  end

  test "multipart digest is recomputed across actual part boundaries before publication", %{
    tmp_dir: root
  } do
    parts = ["first", "second-part", "last"]
    {headers, attributes} = composite(parts)
    {endpoint, _} = WireServer.fixture([{200, headers, Enum.join(parts)}, {200, [], attributes}])
    path = Path.join(root, "verified")

    assert {:ok, %{checksum: :verified}} =
             Engine.download_file(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               path
             )

    assert File.read!(path) == Enum.join(parts)
    assert_receive {:wire_request, %{method: "GET", target: "/bucket/key"}}
    assert_receive {:wire_request, %{method: "GET", target: target, headers: fields}}
    assert target == "/bucket/key?attributes="
    assert fields["x-amz-object-attributes"] == "ObjectParts,Checksum,ETag,ObjectSize"
  end

  test "a one-request budget releases data before metadata and reopens the same version", %{
    tmp_dir: root
  } do
    parts = ["one", "two"]
    {headers, attributes} = composite(parts)

    {endpoint, _} =
      WireServer.fixture([
        {200, headers, Enum.join(parts)},
        {200, [], attributes},
        {200, headers, Enum.join(parts)}
      ])

    {:ok, coordinator} = Coordinator.start_link(max_requests: 1)
    context = %Context{endpoint: endpoint, unsigned: true, coordinator: coordinator}

    assert {:ok, %{checksum: :verified}} =
             Engine.download_file(context, "bucket", "key", Path.join(root, "download"))

    assert Coordinator.stats(coordinator).high_requests == 1
    assert_receive {:wire_request, _}
    assert_receive {:wire_request, _}
    assert_receive {:wire_request, %{headers: fields}}
    assert fields["if-match"] == "\"identity\""
  end

  test "part or object identity mismatches preserve an existing file", %{tmp_dir: root} do
    parts = ["first", "last"]
    {headers, attributes} = composite(parts)

    for {body, manifest, code} <- [
          {"WRONGlast", attributes, :checksum_mismatch},
          {Enum.join(parts), String.replace(attributes, "identity", "changed"),
           :checksum_identity_changed}
        ] do
      {endpoint, _} = WireServer.fixture([{200, headers, body}, {200, [], manifest}])
      path = Path.join(root, Atom.to_string(code))
      File.write!(path, "old")

      assert {:error, %Error{code: ^code}} =
               Engine.download_file(
                 %Context{endpoint: endpoint, unsigned: true},
                 "bucket",
                 "key",
                 path
               )

      assert File.read!(path) == "old"
    end

    assert Path.wildcard(Path.join(root, ".ess-download-*")) == []
  end

  test "invalid sizes and missing part structures return typed errors instead of raising" do
    {headers, attributes} = composite(["a", "b"])

    for invalid <- [
          String.replace(attributes, "<Size>1</Size>", ""),
          "<GetObjectAttributesOutput><ETag>identity</ETag></GetObjectAttributesOutput>"
        ] do
      {endpoint, _} = WireServer.fixture([{200, [], invalid}])

      assert {:error, %Error{code: :invalid_checksum_parts}} =
               DownloadChecksum.start(
                 %Context{endpoint: endpoint, unsigned: true},
                 %{"Bucket" => "bucket", "Key" => "key"},
                 %Response{headers: headers},
                 []
               )
    end
  end

  test "invalid composite CRC64 and ranged composites fail before metadata networking" do
    context = %Context{endpoint: "http://127.0.0.1:1", unsigned: true}

    assert {:error, %Error{code: :invalid_composite_algorithm}} =
             DownloadChecksum.start(
               context,
               %{},
               %Response{
                 headers: [
                   {"x-amz-checksum-type", "COMPOSITE"},
                   {"x-amz-checksum-crc64nvme", "bad-2"}
                 ]
               },
               []
             )

    assert {:error, %Error{code: :composite_range_checksum}} =
             DownloadChecksum.start(
               context,
               %{"Range" => "bytes=0-1"},
               %Response{headers: [{"x-amz-checksum-sha256", "bad-2"}]},
               []
             )
  end

  defp composite(parts) do
    digests =
      Enum.map(parts, fn bytes ->
        {:ok, digest} = Checksum.digest("SHA256", bytes)
        Base.encode64(digest)
      end)

    {:ok, combined} = Checksum.composite("SHA256", digests)

    entries =
      Enum.zip(parts, digests)
      |> Enum.with_index(1)
      |> Enum.map_join(fn {{bytes, digest}, number} ->
        "<Part><PartNumber>#{number}</PartNumber><Size>#{byte_size(bytes)}</Size><ChecksumSHA256>#{digest}</ChecksumSHA256></Part>"
      end)

    body =
      "<GetObjectAttributesOutput><ETag>identity</ETag><ObjectParts><IsTruncated>false</IsTruncated>#{entries}</ObjectParts></GetObjectAttributesOutput>"

    {[
       {"ETag", "\"identity\""},
       {"Content-Length", to_string(Enum.reduce(parts, 0, &(byte_size(&1) + &2)))},
       {"x-amz-checksum-type", "COMPOSITE"},
       {"x-amz-checksum-sha256", combined}
     ], body}
  end
end
