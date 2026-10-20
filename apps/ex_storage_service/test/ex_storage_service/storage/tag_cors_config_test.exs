defmodule ExStorageService.Storage.TagCorsConfigTest do
  use ExUnit.Case, async: false
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.{Backend.Concord, BucketLifecycle}
  alias ExStorageService.Storage.{Tags, BucketCORS}
  @rules [%{allowed_origins: ["https://example.test"], allowed_methods: ["GET"]}]

  defmodule FailedBackend do
    def get(key, opts), do: Concord.get(key, opts)
    def transaction(_spec, _opts), do: {:error, :timeout}
  end

  setup do
    bucket = "tag-cors-config-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    {:ok, meta} = Metadata.get_bucket(bucket)
    on_exit(fn -> Metadata.delete_bucket(bucket) end)
    %{bucket: bucket, generation: BucketLifecycle.generation(meta)}
  end

  test "persisted tags and CORS survive rejected writes and expose transaction errors", %{
    bucket: bucket
  } do
    assert :ok = Tags.put_bucket(bucket, %{"retained" => "yes"})
    assert :ok = BucketCORS.put(bucket, @rules)
    assert {:ok, rules} = BucketCORS.get(bucket)
    opts = [backend: FailedBackend]
    assert {:error, :timeout} = Tags.put_bucket(bucket, %{"new" => "value"}, opts)
    assert {:error, :timeout} = Tags.delete_bucket(bucket, opts)
    assert {:error, :timeout} = BucketCORS.put(bucket, @rules, opts)
    assert {:error, :timeout} = BucketCORS.delete(bucket, opts)
    assert {:ok, %{"retained" => "yes"}} = Tags.get_bucket(bucket)
    assert {:ok, ^rules} = BucketCORS.get(bucket)

    assert {:error, :invalid_tag} =
             Tags.put_bucket(bucket, Map.new(1..51, &{"key#{&1}", "value"}))

    assert {:error, :invalid_cors} = BucketCORS.put(bucket, [])
    assert {:ok, %{"retained" => "yes"}} = Tags.get_bucket(bucket)
    assert {:ok, ^rules} = BucketCORS.get(bucket)
  end

  test "stale requests cannot recreate configuration after bucket deletion", ctx do
    assert :ok = Tags.put_bucket(ctx.bucket, %{"owner" => "old"})
    assert :ok = BucketCORS.put(ctx.bucket, @rules)
    assert :ok = Metadata.delete_bucket(ctx.bucket)
    assert :ok = Metadata.create_bucket(ctx.bucket)
    opts = [bucket_generation: ctx.generation]
    assert {:error, :bucket_generation_changed} = Tags.put_bucket(ctx.bucket, %{}, opts)
    assert {:error, :bucket_generation_changed} = Tags.delete_bucket(ctx.bucket, opts)
    assert {:error, :bucket_generation_changed} = BucketCORS.put(ctx.bucket, @rules, opts)
    assert {:error, :bucket_generation_changed} = BucketCORS.delete(ctx.bucket, opts)

    for prefix <- ~w(bucket_tagging bucket_cors),
        do: assert({:ok, nil} = Concord.get("#{prefix}:#{ctx.bucket}"))
  end

  test "missing buckets cannot acquire orphan tags or CORS" do
    bucket = "missing-tag-cors-#{System.unique_integer([:positive])}"
    assert {:error, :bucket_not_found} = Tags.put_bucket(bucket, %{})
    assert {:error, :bucket_not_found} = Tags.delete_bucket(bucket)
    assert {:error, :bucket_not_found} = BucketCORS.put(bucket, @rules)
    assert {:error, :bucket_not_found} = BucketCORS.delete(bucket)
  end
end
