defmodule ExStorageServiceS3.SdkInteropTest do
  @moduledoc """
  Qualifies the published ExStorageServiceSdk against this ESS server over real
  HTTP with SigV4 authentication and IAM authorization enabled.
  """
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.Versioning
  alias ExStorageServiceSdk.Error

  @endpoint "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"
  @keys [
    "plain.txt",
    "dir//repeated//sep",
    "space key.txt",
    "plus+a",
    "plus a",
    "100%done",
    "%2B literal",
    "unicode/雪/😀 + % ?#.bin"
  ]

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("sdk-interop-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    {:ok, policy} = Policy.create_policy("sdk-interop-#{suffix}", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "sdk-interop-#{suffix}"
    :ok = Metadata.create_bucket(bucket)

    {:ok, s3} =
      ExStorageServiceSdk.new(
        endpoint: @endpoint,
        region: "us-east-1",
        access_key_id: credentials.access_key_id,
        secret_access_key: credentials.secret_access_key
      )

    on_exit(fn ->
      {:ok, objects} = Metadata.list_objects(bucket)

      for %{key: key} <- objects do
        {:ok, versions} = Versioning.list_versions(bucket, key)
        Enum.each(versions, &ObjectService.delete(bucket, key, &1.version_id))
      end

      Metadata.delete_bucket(bucket)
      Policy.detach_policy(user.id, policy.id)
      Policy.delete_policy(policy.id)
      AccessKey.delete_key(credentials.access_key_id)
      User.delete_user(user.id)

      if is_nil(previous),
        do: Application.delete_env(:ex_storage_service, :s3_auth_enabled),
        else: Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)
    end)

    %{s3: s3, bucket: bucket}
  end

  test "exact keys and raw bytes round-trip through put, head, get and paginated listing",
       %{s3: s3, bucket: bucket} do
    payloads =
      Map.new(Enum.with_index(@keys), fn {key, index} ->
        {key, <<index, 0, 255>> <> ~s( {"json" :  "whitespace"}\n) <> :zlib.gzip(key)}
      end)

    for {key, bytes} <- payloads do
      assert {:ok, %{status: 200, etag: etag}} =
               ExStorageServiceSdk.put_object(s3, bucket, key, bytes, %{
                 "ContentType" => "application/octet-stream"
               })

      assert is_binary(etag) and etag != ""
      assert {:ok, %{status: 200, etag: ^etag}} = ExStorageServiceSdk.head_object(s3, bucket, key)
      assert {:ok, %{status: 200, body: ^bytes}} = ExStorageServiceSdk.get_object(s3, bucket, key)
    end

    listed =
      s3
      |> ExStorageServiceSdk.pages("ListObjectsV2", %{"Bucket" => bucket, "MaxKeys" => 2})
      |> Enum.flat_map(fn {:ok, page} -> Enum.map(page["Contents"] || [], & &1["Key"]) end)

    assert Enum.sort(listed) == Enum.sort(@keys)
  end

  test "empty objects, ranges and unsatisfiable ranges", %{s3: s3, bucket: bucket} do
    assert {:ok, _} = ExStorageServiceSdk.put_object(s3, bucket, "empty", "")
    assert {:ok, %{status: 200, body: ""}} = ExStorageServiceSdk.get_object(s3, bucket, "empty")

    assert {:ok, _} = ExStorageServiceSdk.put_object(s3, bucket, "digits", "0123456789")

    assert {:ok, %{status: 206, body: "234"}} =
             ExStorageServiceSdk.get_object(s3, bucket, "digits", %{"Range" => "bytes=2-4"})

    assert {:error, %Error{category: :service, http_status: 416}} =
             ExStorageServiceSdk.get_object(s3, bucket, "digits", %{"Range" => "bytes=50-60"})
  end

  test "missing objects are distinguishable 404s", %{s3: s3, bucket: bucket} do
    assert {:error, %Error{category: :service, http_status: 404, code: "NoSuchKey"}} =
             ExStorageServiceSdk.get_object(s3, bucket, "missing")

    assert {:error, %Error{category: :service, http_status: 404}} =
             ExStorageServiceSdk.head_object(s3, bucket, "missing")
  end

  test "conditional writes, reads, copies and deletes", %{s3: s3, bucket: bucket} do
    assert {:ok, %{etag: etag}} =
             ExStorageServiceSdk.put_object(s3, bucket, "cond", "v1", %{"IfNoneMatch" => "*"})

    assert {:error, %Error{http_status: 412}} =
             ExStorageServiceSdk.put_object(s3, bucket, "cond", "v2", %{"IfNoneMatch" => "*"})

    assert {:error, %Error{http_status: 412}} =
             ExStorageServiceSdk.put_object(s3, bucket, "cond", "v2", %{"IfMatch" => ~s("stale")})

    assert {:ok, %{body: "v1"}} =
             ExStorageServiceSdk.get_object(s3, bucket, "cond", %{"IfMatch" => etag})

    assert {:error, %Error{http_status: 412}} =
             ExStorageServiceSdk.get_object(s3, bucket, "cond", %{"IfMatch" => ~s("stale")})

    assert {:error, %Error{http_status: 412}} =
             ExStorageServiceSdk.copy_object(s3, bucket, "copy", %{
               "CopySource" => %{"Bucket" => bucket, "Key" => "cond"},
               "CopySourceIfMatch" => ~s("stale")
             })

    assert {:ok, %{status: 200}} =
             ExStorageServiceSdk.copy_object(s3, bucket, "unicode copy/雪", %{
               "CopySource" => %{"Bucket" => bucket, "Key" => "cond"},
               "CopySourceIfMatch" => etag,
               "MetadataDirective" => "REPLACE",
               "ContentType" => "text/plain",
               "Metadata" => %{"origin" => "sdk"}
             })

    assert {:ok, %{body: "v1", result: %{"Metadata" => %{"origin" => "sdk"}}}} =
             ExStorageServiceSdk.get_object(s3, bucket, "unicode copy/雪")

    assert {:ok, _} = ExStorageServiceSdk.delete_object(s3, bucket, "cond")

    assert {:error, %Error{http_status: 404}} =
             ExStorageServiceSdk.head_object(s3, bucket, "cond")
  end

  test "multipart uploads complete in part order and abort cleanly", %{s3: s3, bucket: bucket} do
    first = :binary.copy("a", 5 * 1024 * 1024)
    last = "tail"
    key = "multi/part 雪+.bin"

    assert {:ok, %{"UploadId" => id}} =
             ExStorageServiceSdk.create_multipart_upload(s3, bucket, key)

    assert {:ok, %{etag: etag2}} = ExStorageServiceSdk.upload_part(s3, bucket, key, id, 2, last)
    assert {:ok, %{etag: etag1}} = ExStorageServiceSdk.upload_part(s3, bucket, key, id, 1, first)

    assert {:ok, %{status: 200, result: %{"ETag" => final}}} =
             ExStorageServiceSdk.complete_multipart_upload(s3, bucket, key, id, [
               {2, etag2},
               {1, etag1}
             ])

    assert final =~ "-2"
    expected = first <> last

    assert {:ok, %{body: ^expected}} =
             ExStorageServiceSdk.get_object(s3, bucket, key, %{}, max_body_bytes: 6 * 1024 * 1024)

    assert {:ok, %{"UploadId" => aborted}} =
             ExStorageServiceSdk.create_multipart_upload(s3, bucket, "aborted")

    assert {:ok, _} = ExStorageServiceSdk.upload_part(s3, bucket, "aborted", aborted, 1, "x")
    assert {:ok, _} = ExStorageServiceSdk.abort_multipart_upload(s3, bucket, "aborted", aborted)

    assert {:error, %Error{category: :service}} =
             ExStorageServiceSdk.complete_multipart_upload(s3, bucket, "aborted", aborted, [
               {1, ~s("x")}
             ])

    assert {:error, %Error{http_status: 404}} =
             ExStorageServiceSdk.head_object(s3, bucket, "aborted")
  end

  test "completion with an unknown part ETag fails and the upload can still be aborted",
       %{s3: s3, bucket: bucket} do
    assert {:ok, %{"UploadId" => id}} =
             ExStorageServiceSdk.create_multipart_upload(s3, bucket, "bad-part")

    assert {:ok, _} = ExStorageServiceSdk.upload_part(s3, bucket, "bad-part", id, 1, "a")

    assert {:error, %Error{category: :service, http_status: 400}} =
             ExStorageServiceSdk.complete_multipart_upload(s3, bucket, "bad-part", id, [
               {1, ~s("00000000000000000000000000000000")}
             ])

    assert {:ok, _} = ExStorageServiceSdk.abort_multipart_upload(s3, bucket, "bad-part", id)

    assert {:error, %Error{http_status: 404}} =
             ExStorageServiceSdk.head_object(s3, bucket, "bad-part")
  end

  test "wrong credentials are rejected by SigV4", %{bucket: bucket} do
    {:ok, bad} =
      ExStorageServiceSdk.new(
        endpoint: @endpoint,
        access_key_id: "AKIANOTREAL",
        secret_access_key: "wrong"
      )

    assert {:error, %Error{category: :service, http_status: 403}} =
             ExStorageServiceSdk.head_object(bad, bucket, "anything")
  end
end
