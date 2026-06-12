defmodule ExStorageService.Storage.MultipartTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.Multipart

  setup do
    bucket = "multipart-core-#{:erlang.unique_integer([:positive])}"
    {:ok, upload_id} = Multipart.init_upload(bucket, "streamed.bin")

    on_exit(fn ->
      Multipart.abort_upload(bucket, upload_id)

      data_root =
        Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")

      File.rm_rf!(Path.join(data_root, bucket))
    end)

    %{bucket: bucket, upload_id: upload_id}
  end

  test "store_part writes enumerable data without requiring a buffered binary", %{
    bucket: bucket,
    upload_id: upload_id
  } do
    stream = Stream.map(["streamed", "-", "part"], & &1)
    expected_data = "streamed-part"
    expected_etag = :md5 |> :crypto.hash(expected_data) |> Base.encode16(case: :lower)

    assert {:ok, ^expected_etag} = Multipart.store_part(bucket, upload_id, 1, stream)

    assert {:ok, [%{part_number: 1, size: 13, etag: ^expected_etag}]} =
             Multipart.list_parts(bucket, upload_id)
  end
end
