defmodule ExStorageServiceCli.S3.DecoderTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.S3.{Decoder, Response}

  test "flattened lists, modeled scalar types and empty text survive namespace XML" do
    body = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <IsTruncated>false</IsTruncated><Prefix></Prefix><KeyCount>2</KeyCount>
      <Contents><Key>one&amp;two</Key><Size>42</Size><LastModified>2026-10-15T04:00:00Z</LastModified></Contents>
      <Contents><Key>next</Key><Size>0</Size></Contents><FutureField>ignored</FutureField>
    </ListBucketResult>
    """

    assert {:ok, result} = Decoder.decode("ListObjectsV2", %Response{status: 200, body: body})

    assert result == %{
             "IsTruncated" => false,
             "Prefix" => "",
             "KeyCount" => 2,
             "Contents" => [
               %{"Key" => "one&two", "Size" => 42, "LastModified" => ~U[2026-10-15 04:00:00Z]},
               %{"Key" => "next", "Size" => 0}
             ]
           }

    refute Map.has_key?(result, "FutureField")
    refute Map.has_key?(result, "NextContinuationToken")
  end

  test "wrapped lists preserve explicit empty lists and absent lists" do
    assert {:ok, %{"TagSet" => []}} =
             Decoder.decode("GetBucketTagging", %Response{
               status: 200,
               body: "<Tagging><TagSet/></Tagging>"
             })

    assert {:ok, %{}} =
             Decoder.decode("GetBucketTagging", %Response{status: 200, body: "<Tagging/>"})

    assert {:ok, %{"TagSet" => [%{"Key" => "key", "Value" => ""}]}} =
             Decoder.decode("GetBucketTagging", %Response{
               status: 200,
               body: "<Tagging><TagSet><Tag><Key>key</Key><Value/></Tag></TagSet></Tagging>"
             })
  end

  test "XML attributes decode from namespaced attributes" do
    body =
      "<AccessControlPolicy><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>id</ID></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>"

    assert {:ok,
            %{
              "Grants" => [
                %{"Grantee" => %{"Type" => "CanonicalUser", "ID" => "id"}, "Permission" => "READ"}
              ]
            }} = Decoder.decode("GetBucketAcl", %Response{status: 200, body: body})
  end

  test "header scalar values and metadata maps decode case insensitively" do
    response = %Response{
      status: 200,
      body: "",
      headers: [
        {"Content-Length", "42"},
        {"X-Amz-Meta-Owner", "文"},
        {"Last-Modified", "Thu, 15 Oct 2026 04:00:00 GMT"}
      ]
    }

    assert {:ok,
            %{
              "ContentLength" => 42,
              "Metadata" => %{"owner" => "文"},
              "LastModified" => ~U[2026-10-15 04:00:00Z]
            }} = Decoder.decode("HeadObject", response)
  end

  test "scalar payloads and location root text are preserved" do
    policy = "{\"Statement\":[]}"

    assert {:ok, %{"Policy" => ^policy}} =
             Decoder.decode("GetBucketPolicy", %Response{status: 200, body: policy})

    assert {:ok, %{"LocationConstraint" => "eu-west-1"}} =
             Decoder.decode("GetBucketLocation", %Response{
               status: 200,
               body: "<LocationConstraint>eu-west-1</LocationConstraint>"
             })
  end

  test "Expires retains the pinned SDK raw string alongside a valid timestamp" do
    value = "Thu, 15 Oct 2026 04:00:00 GMT"
    response = %Response{status: 200, body: "", headers: [{"eXpIrEs", value}]}

    for result <- [
          Decoder.decode("HeadObject", response),
          Decoder.decode_headers("GetObject", response)
        ] do
      assert {:ok, %{"Expires" => ~U[2026-10-15 04:00:00Z], "ExpiresString" => ^value}} = result
    end
  end

  test "unparseable Expires remains available without failing an object download" do
    response = %Response{status: 200, body: "", headers: [{"Expires", "Invalid Date"}]}

    for result <- [
          Decoder.decode("HeadObject", response),
          Decoder.decode_headers("GetObject", response)
        ] do
      assert {:ok, decoded} = result
      assert decoded["ExpiresString"] == "Invalid Date"
      refute Map.has_key?(decoded, "Expires")
    end
  end

  test "success-status Error documents, malformed XML and invalid modeled scalars fail" do
    for {body, code} <- [
          {"<ListBucketResult><IsTruncated>maybe</IsTruncated></ListBucketResult>",
           :invalid_boolean},
          {"<ListBucketResult><KeyCount>1.5</KeyCount></ListBucketResult>", :invalid_number},
          {"<ListBucketResult><KeyCount>1</KeyCount><KeyCount>2</KeyCount></ListBucketResult>",
           :duplicate_xml_member},
          {"<Error><Code>SlowDown</Code></Error>", :service_error},
          {"<!DOCTYPE x><x/>", :invalid_xml}
        ] do
      assert {:error, %Error{code: ^code}} =
               Decoder.decode("ListObjectsV2", %Response{status: 200, body: body})
    end

    assert {:error, %Error{category: :service, code: :http_error, http_status: 403}} =
             Decoder.decode("ListObjectsV2", %Response{status: 403, body: "denied"})
  end

  test "streaming and event-stream outputs cannot be mistaken for control XML" do
    for operation <- ["GetObject", "GetObjectTorrent", "SelectObjectContent"] do
      assert {:error, %Error{code: :streaming_handler_required}} =
               Decoder.decode(operation, %Response{status: 200, body: "raw bytes"})
    end

    assert {:ok, %{}} = Decoder.decode("DeleteBucket", %Response{status: 204, body: ""})
  end

  test "dedicated handlers decode stream metadata without consuming the body" do
    response = %Response{
      status: 200,
      body: self(),
      headers: [{"Content-Length", "42"}, {"X-Amz-Meta-Owner", "user"}]
    }

    assert {:ok, %{"ContentLength" => 42, "Metadata" => %{"owner" => "user"}}} =
             Decoder.decode_headers("GetObject", response)

    assert {:error, %Error{code: :invalid_number}} =
             Decoder.decode_headers("GetObject", %{
               response
               | headers: [{"Content-Length", "42.5"}]
             })

    assert {:error, %Error{code: :http_error}} =
             Decoder.decode_headers("GetObject", %{response | status: 403})
  end
end
