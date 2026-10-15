defmodule ExStorageServiceCli.S3.EncoderTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.S3.{Encoder, Request, XML}

  test "URI labels retain key slash and dot segments and encode literal percent bytes" do
    assert {:ok, %Request{method: "GET", path: "/bucket/a/../%E6%96%87%20%252F", body: nil}} =
             Encoder.encode("GetObject", %{"Bucket" => "bucket", "Key" => "a/../文 %2F"})

    assert {:ok, %Request{path: "/bucket", query: query}} =
             Encoder.encode("ListObjectsV2", %{
               "Bucket" => "bucket",
               "Prefix" => "",
               "MaxKeys" => 0,
               "FetchOwner" => false
             })

    assert query == [
             {"list-type", "2"},
             {"fetch-owner", "false"},
             {"max-keys", "0"},
             {"prefix", ""}
           ]
  end

  test "nonflattened and flattened lists obey the locked XML model" do
    assert {:ok, %Request{body: body, query: [{"tagging", ""}]}} =
             Encoder.encode("PutBucketTagging", %{
               "Bucket" => "bucket",
               "Tagging" => %{"TagSet" => [%{"Key" => "<&", "Value" => ""}]}
             })

    assert body ==
             "<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet><Tag><Key>&lt;&amp;</Key><Value></Value></Tag></TagSet></Tagging>"

    assert {:ok, %Request{body: body}} =
             Encoder.encode("DeleteObjects", %{
               "Bucket" => "bucket",
               "Delete" => %{
                 "Objects" => [%{"Key" => "one"}, %{"Key" => "two"}],
                 "Quiet" => false
               }
             })

    assert body ==
             "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>one</Key></Object><Object><Key>two</Key></Object><Quiet>false</Quiet></Delete>"
  end

  test "XML attributes and namespaces remain attributes rather than child elements" do
    input = %{
      "Bucket" => "bucket",
      "AccessControlPolicy" => %{
        "Grants" => [
          %{"Grantee" => %{"Type" => "CanonicalUser", "ID" => "id"}, "Permission" => "READ"}
        ]
      }
    }

    assert {:ok, %Request{body: body}} = Encoder.encode("PutBucketAcl", input)

    assert body =~
             "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>id</ID></Grantee>"

    assert {:ok, _} = XML.parse(body)
  end

  test "scalar payloads remain raw and header timestamps use HTTP dates" do
    policy = "{\"Statement\":[]}"

    assert {:ok, %Request{body: ^policy}} =
             Encoder.encode("PutBucketPolicy", %{"Bucket" => "bucket", "Policy" => policy})

    assert {:ok, %Request{headers: [{"If-Modified-Since", "Thu, 15 Oct 2026 04:00:00 GMT"}]}} =
             Encoder.encode("HeadObject", %{
               "Bucket" => "bucket",
               "Key" => "key",
               "IfModifiedSince" => "2026-10-15T12:00:00+08:00"
             })
  end

  test "streaming input requires its dedicated handler instead of buffering a blob" do
    assert {:ok, %Request{operation: "PutObject", body: nil}} =
             Encoder.encode("PutObject", %{"Bucket" => "bucket", "Key" => "key"})

    assert {:error, %Error{code: :streaming_handler_required}} =
             Encoder.encode("PutObject", %{
               "Bucket" => "bucket",
               "Key" => "key",
               "Body" => "raw bytes"
             })

    assert {:error, %Error{code: :invalid_parameter}} =
             Encoder.encode("GetObject", %{"Bucket" => "bucket"})

    assert {:ok, %Request{method: "PUT", path: "/bucket/key", body: ""}} =
             Encoder.encode("PutObject", %{"Bucket" => "bucket", "Key" => "key", "Body" => ""},
               allow_streaming: true
             )

    assert {:error, %Error{code: :invalid_parameter}} =
             Encoder.encode(
               "PutObject",
               %{"Bucket" => "bucket", "Key" => "key", "Body" => self()},
               allow_streaming: true
             )
  end

  test "header ISO timestamps truncate fractional seconds like the pinned serializer" do
    assert {:ok,
            %Request{headers: [{"x-amz-object-lock-retain-until-date", "2026-10-15T12:00:00Z"}]}} =
             Encoder.encode("PutObject", %{
               "Bucket" => "bucket",
               "Key" => "key",
               "ObjectLockRetainUntilDate" => "2026-10-15T12:00:00.123Z"
             })
  end

  test "invalid XML controls and header injection return typed errors" do
    assert {:error, %Error{code: :invalid_xml_value}} =
             Encoder.encode("PutBucketTagging", %{
               "Bucket" => "bucket",
               "Tagging" => %{"TagSet" => [%{"Key" => "key", "Value" => <<0>>}]}
             })

    assert {:error, %Error{code: :invalid_header}} =
             Encoder.encode("HeadObject", %{
               "Bucket" => "bucket",
               "Key" => "key",
               "IfMatch" => "value\r\nInjected: secret"
             })
  end
end
