defmodule ExStorageService.BucketValidatorTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BucketValidator

  test "accepts normal S3 bucket names" do
    assert BucketValidator.valid_bucket_name?("my-bucket")
    assert :ok = BucketValidator.validate("my-bucket-123")
  end

  test "rejects the reserved cas name" do
    refute BucketValidator.valid_bucket_name?("cas")
    assert {:error, message} = BucketValidator.validate("cas")
    assert message =~ "reserved"
  end

  test "still accepts names merely containing cas" do
    assert BucketValidator.valid_bucket_name?("cascade")
    assert BucketValidator.valid_bucket_name?("my-cas-bucket")
  end
end
