defmodule ExStorageServiceCli.S3.InputTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.S3.Input

  test "absence, explicit empty values and booleans remain distinct" do
    input = %{"Bucket" => "bucket", "Prefix" => "", "FetchOwner" => false, "MaxKeys" => 0}
    assert {:ok, ^input} = Input.validate("list-objects-v2", input)

    assert {:ok, %{"Bucket" => "bucket"}} =
             Input.validate("ListObjectsV2", %{"Bucket" => "bucket"})

    for changes <- [
          %{"Bucket" => nil},
          %{"MaxKeys" => "1"},
          %{"MaxKeys" => 1.0},
          %{"FetchOwner" => "false"},
          %{"Unknown" => "secret"}
        ] do
      assert {:error, %Error{code: :invalid_parameter, message: message}} =
               Input.validate("ListObjectsV2", Map.merge(%{"Bucket" => "bucket"}, changes))

      refute message =~ "secret"
    end
  end

  test "nested required members, lists, enums and map keys are checked" do
    input = %{
      "Bucket" => "bucket",
      "Tagging" => %{"TagSet" => [%{"Key" => "key", "Value" => ""}]}
    }

    assert {:ok, ^input} = Input.validate("PutBucketTagging", input)

    assert {:error, %Error{message: message}} =
             Input.validate(
               "PutBucketTagging",
               put_in(input, ["Tagging", "TagSet"], [%{"Key" => "key"}])
             )

    assert message =~ "Tagging.TagSet.0.Value"

    assert {:error, %Error{}} =
             Input.validate("PutBucketTagging", Map.put(input, "ChecksumAlgorithm", "UNKNOWN"))

    assert {:error, %Error{}} =
             Input.validate("PutObject", %{
               "Bucket" => "bucket",
               "Key" => "key",
               "Metadata" => %{unexpected: "value"}
             })

    assert {:error, %Error{}} =
             Input.validate("DeleteObjects", %{
               "Bucket" => "bucket",
               "Delete" => %{"Objects" => "key"}
             })
  end

  test "timestamps support ISO8601 offsets, DateTime and numeric Unix seconds" do
    for timestamp <- [
          "2026-10-15T12:00:00Z",
          "2026-10-15T12:00:00+08:00",
          ~U[2026-10-15 12:00:00Z],
          1_760_000_000
        ] do
      input = %{"Bucket" => "bucket", "Key" => "key", "IfModifiedSince" => timestamp}
      assert {:ok, ^input} = Input.validate("GetObject", input)
    end

    assert {:error, %Error{}} =
             Input.validate("GetObject", %{
               "Bucket" => "bucket",
               "Key" => "key",
               "IfModifiedSince" => "yesterday"
             })
  end

  test "required top-level fields and exact integers are validated" do
    assert {:error, %Error{message: message}} =
             Input.validate("GetObject", %{"Bucket" => "bucket"})

    assert message =~ "Key"

    assert {:error, %Error{}} =
             Input.validate("ListObjectsV2", %{"Bucket" => "bucket", "MaxKeys" => 2_147_483_648})

    assert {:error, %Error{}} = Input.validate("GetObject", %{Bucket: "bucket", Key: "key"})
    assert {:error, %Error{code: :unknown_operation}} = Input.validate("missing", %{})
  end
end
