defmodule ExStorageServiceCli.S3.CapabilitiesTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error}
  alias ExStorageServiceCli.S3.{Capabilities, Client, OperationCatalog}

  test "known ESS exclusions prevent destructive subresource fallthrough before I/O" do
    context = %Context{
      endpoint: "http://127.0.0.1:1",
      unsigned: true,
      capabilities: Capabilities.record("ess", "0.6.7")
    }

    for operation <-
          ~w(PutBucketPolicy DeleteBucketCors PutObjectTagging DeleteObjectTagging GetObjectAcl UploadPartCopy ListObjectVersions) do
      assert {:error, %Error{code: :provider_unsupported, operation: ^operation}} =
               Client.call(context, operation, %{"Bucket" => "bucket", "Key" => "key"})
    end

    assert {:error, %Error{code: :provider_unsupported}} =
             Capabilities.check(context, "PutObject", %{"SSECustomerAlgorithm" => "AES256"})
  end

  test "source-reviewed ESS operations exist in the locked catalog" do
    description =
      Capabilities.describe(%Context{capabilities: Capabilities.record("ess", "0.6.7")})

    for operation <- description["KnownOperations"],
        do: assert({:ok, _} = OperationCatalog.operation(operation))
  end

  test "AWS and generic capability declarations do not infer support from permissions" do
    for provider <- ~w(aws s3 unknown) do
      context = %Context{capabilities: Capabilities.record(provider)}
      assert :ok = Capabilities.check(context, "PutBucketPolicy")

      assert %{"UnknownCapabilities" => true, "KnownOperations" => []} =
               Capabilities.describe(context)
    end
  end
end
