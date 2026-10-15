Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transfer.UploadVerificationTest do
  # Journal mutex ports share the host namespace with concurrent wire fixtures.
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error}
  alias ExStorageServiceCli.Transfer.{Engine, Journal, Multipart, Planner, Source}
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  # CRC64/NVME check value from the published algorithm catalogue, not this signer.
  @crc "rosUhgp5mIg="
  @limits %{
    max_single_put_bytes: 5,
    min_nonfinal_part_bytes: 5,
    max_part_bytes: 5,
    max_parts: 10,
    max_object_bytes: 100
  }
  @create "<InitiateMultipartUploadResult><UploadId>ours</UploadId></InitiateMultipartUploadResult>"
  @complete "<CompleteMultipartUploadResult><ETag>&quot;done&quot;</ETag></CompleteMultipartUploadResult>"

  test "single PUT verifies provider digests and rejects a bad caller digest before I/O", %{
    tmp_dir: root
  } do
    path = Path.join(root, "source")
    File.write!(path, "123456789")

    {endpoint, _} =
      WireServer.fixture([{200, [{"ETag", "\"ok\""}, {"x-amz-checksum-crc64nvme", @crc}], ""}])

    context = %Context{endpoint: endpoint, unsigned: true}

    assert {:error, %Error{code: :upload_checksum_mismatch}} =
             Engine.upload_file(context, "bucket", "key", path,
               input: %{"ChecksumCRC64NVME" => Base.encode64(<<0::64>>)}
             )

    refute_receive {:wire_request, _}

    assert {:ok, %{checksum: :verified, bytes: 9}} =
             Engine.upload_file(context, "bucket", "key", path)

    assert_receive {:wire_request, %{body: "123456789", headers: headers}}
    assert headers["x-amz-checksum-crc64nvme"] == @crc
  end

  test "a successful PUT with a mismatched returned digest fails", %{tmp_dir: root} do
    path = Path.join(root, "source")
    File.write!(path, "123456789")

    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"ok\""}, {"x-amz-checksum-crc64nvme", Base.encode64(<<0::64>>)}], ""}
      ])

    assert {:error, %Error{code: :upload_checksum_mismatch}} =
             Engine.upload_file(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               path
             )
  end

  test "multipart retains absent part checksums and submits a full-object CRC", %{tmp_dir: root} do
    path = Path.join(root, "source")
    File.write!(path, "123456789")
    {:ok, source} = Source.file(path)
    {:ok, plan} = Planner.plan(9, @limits)

    {endpoint, _} =
      WireServer.fixture([
        {200, [], @create},
        {200, [{"ETag", "\"p1\""}], ""},
        {200, [{"ETag", "\"p2\""}], ""},
        {200, [], @complete},
        {200, [{"ETag", "\"done\""}, {"Content-Length", "9"}, {"x-amz-checksum-crc64nvme", @crc}],
         ""}
      ])

    state = Path.join(root, "state")

    assert {:ok, %{journal_id: id}} =
             Multipart.upload(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               source,
               plan,
               state_root: state
             )

    assert {:ok,
            %{
              "options" => %{
                "checksum_algorithm" => "CRC64NVME",
                "checksum_type" => "FULL_OBJECT"
              },
              "parts" => parts
            }} = Journal.get(state, id)

    assert Enum.all?(parts, &is_binary(&1["checksums"]["CRC64NVME"]))
    assert_receive {:wire_request, %{method: "POST", headers: create_headers}}
    assert create_headers["x-amz-checksum-algorithm"] == "CRC64NVME"
    assert create_headers["x-amz-checksum-type"] == "FULL_OBJECT"
    assert_receive {:wire_request, %{body: "12345"}}
    assert_receive {:wire_request, %{body: "6789"}}
    assert_receive {:wire_request, %{method: "POST", headers: headers, body: xml}}
    assert headers["x-amz-checksum-crc64nvme"] == @crc
    assert headers["x-amz-checksum-type"] == "FULL_OBJECT"
    assert headers["x-amz-mp-object-size"] == "9"
    assert xml =~ "<ChecksumCRC64NVME>"
  end

  test "conflicting initiation is aborted using only the acknowledged owned ID", %{tmp_dir: root} do
    path = Path.join(root, "source")
    File.write!(path, "123456789")
    {:ok, source} = Source.file(path)
    {:ok, plan} = Planner.plan(9, @limits)

    {endpoint, _} =
      WireServer.fixture([{200, [{"x-amz-checksum-type", "COMPOSITE"}], @create}, {204, [], ""}])

    assert {:error, %Error{code: :upload_checksum_type, details: %{cleanup: "aborted"}}} =
             Multipart.upload(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               source,
               plan,
               state_root: Path.join(root, "state")
             )

    assert_receive {:wire_request, %{method: "POST"}}
    assert_receive {:wire_request, %{method: "DELETE", target: "/bucket/key?uploadId=ours"}}
    refute_receive {:wire_request, %{method: "PUT"}}
  end

  test "a completed object with a bad checksum is retained as a failed verification", %{
    tmp_dir: root
  } do
    path = Path.join(root, "source")
    File.write!(path, "123456789")
    {:ok, source} = Source.file(path)
    {:ok, plan} = Planner.plan(9, @limits)

    {endpoint, _} =
      WireServer.fixture([
        {200, [], @create},
        {200, [{"ETag", "\"p1\""}], ""},
        {200, [{"ETag", "\"p2\""}], ""},
        {200, [],
         String.replace(
           @complete,
           "</CompleteMultipartUploadResult>",
           "<ChecksumCRC64NVME>AAAAAAAAAAA=</ChecksumCRC64NVME></CompleteMultipartUploadResult>"
         )}
      ])

    state = Path.join(root, "state")

    assert {:error, %Error{code: :upload_checksum_mismatch, details: %{journal_id: id}}} =
             Multipart.upload(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               source,
               plan,
               state_root: state
             )

    assert {:ok, %{"phase" => "failed", "completion" => %{"etag" => "\"done\""}}} =
             Journal.get(state, id)

    refute_receive {:wire_request, %{method: "DELETE"}}
  end
end
