defmodule ExStorageServiceCli.BucketTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.Commands.Bucket

  describe "parse_bucket_name/1" do
    test "correctly parses bucket names with and without s3 prefix" do
      assert Bucket.parse_bucket_name("s3://my-bucket") == "my-bucket"
      assert Bucket.parse_bucket_name("s3://my-bucket/") == "my-bucket"
      assert Bucket.parse_bucket_name("my-bucket") == "my-bucket"
    end
  end
end
