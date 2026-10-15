Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.S3.CustomizationsTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, S3.Client}
  alias ExStorageServiceCli.Test.WireServer

  test "raw API copy sources are quoted once while the explicit version suffix is preserved" do
    for operation <- ["CopyObject", "UploadPartCopy"] do
      {endpoint, _} =
        WireServer.fixture([
          {200, [], "<CopyObjectResult><ETag>fixture</ETag></CopyObjectResult>"}
        ])

      input = %{
        "Bucket" => "destination",
        "Key" => "key",
        "CopySource" => "source/old% +?#/雪"
      }

      input =
        if operation == "UploadPartCopy",
          do: Map.merge(input, %{"UploadId" => "upload", "PartNumber" => 1}),
          else: input

      assert {:ok, _} =
               Client.call(%Context{endpoint: endpoint, unsigned: true}, operation, input)

      assert_receive {:wire_request, %{headers: headers}}
      assert headers["x-amz-copy-source"] == "source/old%25%20%2B%3F%23/%E9%9B%AA"
    end

    {endpoint, _} =
      WireServer.fixture([{200, [], "<CopyObjectResult><ETag>fixture</ETag></CopyObjectResult>"}])

    assert {:ok, _} =
             Client.call(%Context{endpoint: endpoint, unsigned: true}, "CopyObject", %{
               "Bucket" => "destination",
               "Key" => "key",
               "CopySource" => "source/a%2Fb?versionId=v%2F%2B"
             })

    assert_receive {:wire_request, %{headers: headers}}
    assert headers["x-amz-copy-source"] == "source/a%252Fb?versionId=v%2F%2B"
  end

  test "native structured copy sources keep literal key syntax separate from selected versions" do
    for version <- [nil, "v/+%"] do
      {endpoint, _} =
        WireServer.fixture([
          {200, [], "<CopyObjectResult><ETag>fixture</ETag></CopyObjectResult>"}
        ])

      source = %{"Bucket" => "source", "Key" => "a+?versionId=literal"}
      source = if version, do: Map.put(source, "VersionId", version), else: source

      assert {:ok, _} =
               Client.call(%Context{endpoint: endpoint, unsigned: true}, "CopyObject", %{
                 "Bucket" => "destination",
                 "Key" => "key",
                 "CopySource" => source
               })

      assert_receive {:wire_request, %{headers: headers}}
      suffix = if version, do: "?versionId=v%2F%2B%25", else: ""
      assert headers["x-amz-copy-source"] == "/source/a%2B%3FversionId%3Dliteral" <> suffix
    end
  end

  test "automatic URL listing encoding decodes only reference-selected fields exactly once" do
    for operation <- ["ListObjects", "ListObjectsV2", "ListObjectVersions"],
        explicit <- [false, true] do
      body = """
      <ListBucketResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>
      <Prefix>prefix%252F%2B%2F%E9%9B%AA</Prefix><Delimiter>%2F</Delimiter>
      <Marker>marker%252F%2B</Marker><NextMarker>next%252F%2B</NextMarker>
      <StartAfter>start%252F%2B</StartAfter>
      <KeyMarker>marker%252F%2B</KeyMarker><NextKeyMarker>next%252F%2B</NextKeyMarker>
      <VersionIdMarker>opaque%2F%2B</VersionIdMarker><NextVersionIdMarker>opaque%2F%2B</NextVersionIdMarker>
      <ContinuationToken>opaque%2F%2B</ContinuationToken><NextContinuationToken>opaque%2F%2B</NextContinuationToken>
      <Contents><Key>key%252F%2B%20%E9%9B%AA</Key><Size>1</Size></Contents>
      <Version><Key>key%252F%2B%20%E9%9B%AA</Key><VersionId>opaque%2F%2B</VersionId></Version>
      <DeleteMarker><Key>key%252F%2B%20%E9%9B%AA</Key><VersionId>opaque%2F%2B</VersionId></DeleteMarker>
      <CommonPrefixes><Prefix>common%252F%2B%2F</Prefix></CommonPrefixes></ListBucketResult>
      """

      {endpoint, _} = WireServer.fixture([{200, [], body}])
      input = %{"Bucket" => "bucket"}
      input = if explicit, do: Map.put(input, "EncodingType", "url"), else: input

      assert {:ok, result} =
               Client.call(%Context{endpoint: endpoint, unsigned: true}, operation, input)

      assert_receive {:wire_request, %{target: target}}
      assert target =~ "encoding-type=url"
      encoded = "key%252F%2B%20%E9%9B%AA"
      decoded = "key%2F+ 雪"
      collection = if operation == "ListObjectVersions", do: "Versions", else: "Contents"
      assert hd(result[collection])["Key"] == if(explicit, do: encoded, else: decoded)
      assert result["Delimiter"] == if(explicit, do: "%2F", else: "/")

      assert result["Prefix"] ==
               if(explicit or operation == "ListObjects",
                 do: "prefix%252F%2B%2F%E9%9B%AA",
                 else: "prefix%2F+/雪"
               )

      assert hd(result["CommonPrefixes"])["Prefix"] ==
               if(explicit, do: "common%252F%2B%2F", else: "common%2F+/")

      for field <-
            ~w(VersionIdMarker NextVersionIdMarker ContinuationToken NextContinuationToken),
          Map.has_key?(result, field) do
        assert result[field] == "opaque%2F%2B"
      end

      for field <- ~w(Marker NextMarker StartAfter KeyMarker NextKeyMarker),
          value = result[field] do
        assert String.contains?(value, if(explicit, do: "%252F%2B", else: "%2F+"))
      end
    end
  end
end
