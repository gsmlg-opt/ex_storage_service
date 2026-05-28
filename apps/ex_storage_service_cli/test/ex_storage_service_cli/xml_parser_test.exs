defmodule ExStorageServiceCli.XmlParserTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.XmlParser

  describe "parse_list_buckets/1" do
    test "parses a ListAllMyBucketsResult" do
      xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Owner>
          <ID>owner</ID>
          <DisplayName>owner</DisplayName>
        </Owner>
        <Buckets>
          <Bucket>
            <Name>my-bucket</Name>
            <CreationDate>2024-01-15T12:00:00.000Z</CreationDate>
          </Bucket>
          <Bucket>
            <Name>another-bucket</Name>
            <CreationDate>2024-02-20T08:30:00.000Z</CreationDate>
          </Bucket>
        </Buckets>
      </ListAllMyBucketsResult>
      """

      result = XmlParser.parse_list_buckets(xml)
      assert length(result) == 2
      assert Enum.at(result, 0).name == "my-bucket"
      assert Enum.at(result, 0).creation_date == "2024-01-15T12:00:00.000Z"
      assert Enum.at(result, 1).name == "another-bucket"
    end

    test "handles empty bucket list" do
      xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Buckets/>
      </ListAllMyBucketsResult>
      """

      result = XmlParser.parse_list_buckets(xml)
      assert result == []
    end
  end

  describe "parse_list_objects/1" do
    test "parses a ListBucketResult V2" do
      xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Name>my-bucket</Name>
        <Prefix></Prefix>
        <KeyCount>2</KeyCount>
        <MaxKeys>1000</MaxKeys>
        <IsTruncated>false</IsTruncated>
        <Contents>
          <Key>file1.txt</Key>
          <LastModified>2024-01-15T12:00:00.000Z</LastModified>
          <ETag>"abc123"</ETag>
          <Size>1024</Size>
          <StorageClass>STANDARD</StorageClass>
        </Contents>
        <Contents>
          <Key>file2.txt</Key>
          <LastModified>2024-01-16T14:00:00.000Z</LastModified>
          <ETag>"def456"</ETag>
          <Size>2048</Size>
          <StorageClass>STANDARD</StorageClass>
        </Contents>
      </ListBucketResult>
      """

      result = XmlParser.parse_list_objects(xml)
      assert result.key_count == 2
      assert result.is_truncated == false
      assert length(result.contents) == 2
      assert Enum.at(result.contents, 0).key == "file1.txt"
      assert Enum.at(result.contents, 0).size == 1024
      assert Enum.at(result.contents, 0).etag == "abc123"
      assert Enum.at(result.contents, 1).key == "file2.txt"
    end

    test "parses common prefixes" do
      xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Name>my-bucket</Name>
        <Prefix></Prefix>
        <Delimiter>/</Delimiter>
        <KeyCount>0</KeyCount>
        <MaxKeys>1000</MaxKeys>
        <IsTruncated>false</IsTruncated>
        <CommonPrefixes>
          <Prefix>images/</Prefix>
        </CommonPrefixes>
        <CommonPrefixes>
          <Prefix>docs/</Prefix>
        </CommonPrefixes>
      </ListBucketResult>
      """

      result = XmlParser.parse_list_objects(xml)
      assert length(result.common_prefixes) == 2
      assert "images/" in result.common_prefixes
      assert "docs/" in result.common_prefixes
    end

    test "parses truncated result with continuation token" do
      xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Name>my-bucket</Name>
        <Prefix></Prefix>
        <KeyCount>1</KeyCount>
        <MaxKeys>1</MaxKeys>
        <IsTruncated>true</IsTruncated>
        <NextContinuationToken>abc123token</NextContinuationToken>
        <Contents>
          <Key>file1.txt</Key>
          <LastModified>2024-01-15T12:00:00.000Z</LastModified>
          <ETag>"abc"</ETag>
          <Size>100</Size>
          <StorageClass>STANDARD</StorageClass>
        </Contents>
      </ListBucketResult>
      """

      result = XmlParser.parse_list_objects(xml)
      assert result.is_truncated == true
      assert result.next_continuation_token == "abc123token"
    end
  end

  describe "parse_error/1" do
    test "parses an S3 error response" do
      xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <Error>
        <Code>NoSuchBucket</Code>
        <Message>The specified bucket does not exist</Message>
        <Resource>/my-bucket</Resource>
        <RequestId>ABC123</RequestId>
      </Error>
      """

      assert {:ok, error} = XmlParser.parse_error(xml)
      assert error.code == "NoSuchBucket"
      assert error.message == "The specified bucket does not exist"
    end

    test "returns :error for non-XML input" do
      assert :error = XmlParser.parse_error("not xml at all")
    end

    test "returns :error for XML without error elements" do
      xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <Something><Other>value</Other></Something>
      """

      assert :error = XmlParser.parse_error(xml)
    end
  end
end
