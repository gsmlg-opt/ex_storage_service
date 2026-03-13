defmodule ExStorageService.S3.XMLTest do
  use ExUnit.Case, async: true

  alias ExStorageService.S3.XML

  describe "error_response/4" do
    test "generates valid XML with all required elements" do
      result =
        XML.error_response(
          "NoSuchKey",
          "The specified key does not exist.",
          "/bucket/key",
          "REQ123"
        )

      assert result =~ ~s(<?xml version="1.0" encoding="UTF-8"?>)
      assert result =~ "<Error>"
      assert result =~ "<Code>NoSuchKey</Code>"
      assert result =~ "<Message>The specified key does not exist.</Message>"
      assert result =~ "<Resource>/bucket/key</Resource>"
      assert result =~ "<RequestId>REQ123</RequestId>"
      assert result =~ "</Error>"
    end
  end

  describe "error_status_code/1" do
    test "returns 404 for NoSuchBucket" do
      assert XML.error_status_code("NoSuchBucket") == 404
    end

    test "returns 404 for NoSuchKey" do
      assert XML.error_status_code("NoSuchKey") == 404
    end

    test "returns 409 for BucketAlreadyExists" do
      assert XML.error_status_code("BucketAlreadyExists") == 409
    end

    test "returns 409 for BucketNotEmpty" do
      assert XML.error_status_code("BucketNotEmpty") == 409
    end

    test "returns 403 for AccessDenied" do
      assert XML.error_status_code("AccessDenied") == 403
    end

    test "returns 400 for InvalidArgument" do
      assert XML.error_status_code("InvalidArgument") == 400
    end

    test "returns 500 for InternalError" do
      assert XML.error_status_code("InternalError") == 500
    end

    test "returns 500 for unknown error codes" do
      assert XML.error_status_code("SomethingUnexpected") == 500
    end
  end

  describe "list_buckets_response/1" do
    test "generates valid XML for a list of buckets" do
      buckets = [
        %{name: "bucket-one", creation_date: "2025-01-01T00:00:00Z"},
        %{name: "bucket-two", creation_date: "2025-06-15T12:00:00Z"}
      ]

      result = XML.list_buckets_response(buckets)

      assert result =~ ~s(<?xml version="1.0" encoding="UTF-8"?>)
      assert result =~ "<ListAllMyBucketsResult"
      assert result =~ "<Buckets>"
      assert result =~ "<Name>bucket-one</Name>"
      assert result =~ "<Name>bucket-two</Name>"
      assert result =~ "<CreationDate>2025-01-01T00:00:00Z</CreationDate>"
      assert result =~ "</ListAllMyBucketsResult>"
    end
  end

  describe "escape/1" do
    test "escapes ampersand" do
      assert XML.escape("a&b") == "a&amp;b"
    end

    test "escapes angle brackets" do
      assert XML.escape("<tag>") == "&lt;tag&gt;"
    end

    test "escapes double quotes" do
      assert XML.escape(~s(say "hello")) == "say &quot;hello&quot;"
    end

    test "escapes single quotes" do
      assert XML.escape("it's") == "it&apos;s"
    end

    test "returns empty string for nil" do
      assert XML.escape(nil) == ""
    end

    test "handles plain text without changes" do
      assert XML.escape("hello world") == "hello world"
    end
  end
end
