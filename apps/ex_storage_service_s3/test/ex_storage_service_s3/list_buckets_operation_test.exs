defmodule ExStorageServiceS3.ListBucketsOperationTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceS3.Operation

  test "root listing accepts SDK metadata and preserves its query pairs" do
    for query <- ["", "x-id=ListBuckets", "x-id=OtherOperation", "x-id=", "x-id=List%42uckets"] do
      conn = Plug.Test.conn(:get, "/?#{query}")
      assert {:ok, %{name: :list_buckets, query_pairs: pairs}} = Operation.resolve(conn)
      assert pairs == Enum.to_list(URI.query_decoder(query))
      assert conn.query_string == query
    end
  end

  test "presigned authentication combines with SDK metadata without choosing an operation" do
    authentication =
      "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2Fscope&" <>
        "X-Amz-Date=20261020T000000Z&X-Amz-Expires=60&" <>
        "X-Amz-SignedHeaders=host&X-Amz-Signature=signature"

    for query <- [authentication, "x-id=ListBuckets&" <> authentication] do
      assert {:ok, %{name: :list_buckets, query_pairs: pairs}} =
               Operation.resolve(Plug.Test.conn(:get, "/?#{query}"))

      assert pairs == Enum.to_list(URI.query_decoder(query))
    end
  end

  test "root listing rejects unsupported features and operation selectors" do
    for query <- [
          "prefix=logs",
          "max-buckets=1",
          "continuation-token=next",
          "bucket-region=us-east-1",
          "unknown=value",
          "tagging",
          "acl",
          "retention",
          "versions",
          "uploads",
          "versionId=one",
          "x-id=ListBuckets&tagging",
          "tagging&uploads"
        ] do
      assert {:error, _, _} = Operation.resolve(Plug.Test.conn(:get, "/?#{query}"))
    end
  end

  test "root listing rejects duplicate query names including equivalent encoded names" do
    for query <- [
          "x-id=ListBuckets&x-id=ListBuckets",
          "x-id=ListBuckets&x-id=OtherOperation",
          "x-id=ListBuckets&x%2Did=ListBuckets",
          "X-Amz-Date=first&X-Amz-Date=second"
        ] do
      assert {:error, "InvalidArgument", _} =
               Operation.resolve(Plug.Test.conn(:get, "/?#{query}"))
    end
  end

  test "SDK metadata does not enable other root methods" do
    for method <- [:put, :post, :delete, :head] do
      assert {:error, _, _} = Operation.resolve(Plug.Test.conn(method, "/?x-id=ListBuckets"))
    end
  end
end
