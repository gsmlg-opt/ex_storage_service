Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.S3.RequestChecksumTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, S3.Client, S3.RequestChecksum}
  alias ExStorageServiceCli.S3.{Encoder, Request}
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transfer.Engine
  @moduletag :tmp_dir

  test "control default headers match independently captured AWS CLI 2.31.18 requests" do
    # Captured from the pinned AWS executable against a loopback HTTP receiver.
    for {operation, input, expected, hash} <- [
          {"PutBucketTagging",
           %{"Bucket" => "bucket", "Tagging" => %{"TagSet" => [%{"Key" => "x", "Value" => "y"}]}},
           "iKkbWqi623I=", "3e177339e33ed0aa9ef35f82b25434dd1424b25cdb377f8aea63caf559ee8a78"},
          {"DeleteObjects",
           %{"Bucket" => "bucket", "Delete" => %{"Objects" => [%{"Key" => "x"}]}}, "Bp47pgyANbM=",
           "63431156e1bc0af740f40e32294e50be777d9d3cf4d5b219279f53b83d11d5bf"}
        ] do
      {:ok, request} = Encoder.encode(operation, input)
      assert Base.encode16(:crypto.hash(:sha256, request.body), case: :lower) == hash
      assert {:ok, checksummed} = RequestChecksum.prepare(request, input, %Context{}, [])
      fields = Map.new(checksummed.headers)
      assert fields["x-amz-checksum-crc64nvme"] == expected
      assert fields["x-amz-sdk-checksum-algorithm"] == "CRC64NVME"
    end
  end

  test "required requests remain checksummed in when_required mode and supplied values are preserved" do
    input = %{"Bucket" => "bucket", "Delete" => %{"Objects" => [%{"Key" => "x"}]}}
    {:ok, request} = Encoder.encode("DeleteObjects", input)
    context = %Context{options: %{request_checksum_calculation: "when_required"}}
    assert {:ok, required} = RequestChecksum.prepare(request, input, context, [])
    assert Map.new(required.headers)["x-amz-checksum-crc64nvme"] != nil

    {:ok, optional} =
      Encoder.encode("PutObject", %{"Bucket" => "bucket", "Key" => "key", "Body" => ""},
        allow_streaming: true
      )

    assert {:ok, ^optional} = RequestChecksum.prepare(optional, %{}, context, [])
    explicit = %{request | headers: request.headers ++ [{"content-md5", "caller-value"}]}
    assert {:ok, ^explicit} = RequestChecksum.prepare(explicit, input, %Context{}, [])
  end

  test "streamed uploads calculate selected algorithms from the captured raw source", %{
    tmp_dir: root
  } do
    path = Path.join(root, "raw")
    File.write!(path, "123456789")
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"tag\""}], ""}])

    assert {:ok, _} =
             Engine.upload_file(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               path,
               input: %{"ChecksumAlgorithm" => "CRC32C"}
             )

    assert_receive {:wire_request, %{body: "123456789", headers: fields}}
    assert fields["x-amz-checksum-crc32c"] == "4waSgw=="
    assert fields["x-amz-sdk-checksum-algorithm"] == "CRC32C"
    refute Map.has_key?(fields, "x-amz-checksum-crc64nvme")
  end

  test "GetObject requests checksum validation by default and honors when_required" do
    input = %{"Bucket" => "bucket", "Key" => "key"}

    assert {:ok, %{"ChecksumMode" => "ENABLED"}} =
             RequestChecksum.input(%Context{}, "GetObject", input)

    assert {:ok, ^input} =
             RequestChecksum.input(
               %Context{options: %{response_checksum_validation: "when_required"}},
               "GetObject",
               input
             )
  end

  test "unreplayable input is rejected instead of consumed once before upload" do
    request = %Request{operation: "PutObject", body: "", headers: []}
    source = Stream.repeatedly(fn -> flunk("nonreplayable source was consumed") end)

    assert {:error, %Error{code: :checksum_source_required}} =
             RequestChecksum.prepare(request, %{}, %Context{}, body_source: source)
  end

  test "required checksum reaches the wire before request signing" do
    {endpoint, _} = WireServer.fixture([{200, [], ""}])
    input = %{"Bucket" => "bucket", "Tagging" => %{"TagSet" => [%{"Key" => "x", "Value" => "y"}]}}

    assert {:ok, %{}} =
             Client.call(
               %Context{
                 endpoint: endpoint,
                 access_key_id: "fixture",
                 secret_access_key: "fixture"
               },
               "PutBucketTagging",
               input
             )

    assert_receive {:wire_request, %{headers: headers}}
    assert headers["authorization"] =~ "x-amz-checksum-crc64nvme"
    assert headers["authorization"] =~ "x-amz-sdk-checksum-algorithm"
  end
end
