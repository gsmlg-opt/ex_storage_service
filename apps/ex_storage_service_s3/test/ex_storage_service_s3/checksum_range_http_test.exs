defmodule ExStorageServiceS3.ChecksumRangeHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Versioning
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  test "hosted current and explicit-version ranges omit whole-object checksums" do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, false)
    on_exit(fn -> Application.put_env(:ex_storage_service, :s3_auth_enabled, previous) end)

    for enabled <- [false, true] do
      bucket = "checksum-range-#{System.unique_integer([:positive])}"
      :ok = Metadata.create_bucket(bucket)
      if enabled, do: assert(:ok = Versioning.set_versioning(bucket, :enabled))
      checksum = Base.encode64(:crypto.hash(:sha256, "0123456789"))
      url = @base <> "/#{bucket}/key"

      put =
        Req.put!(url,
          retry: false,
          body: "0123456789",
          headers: [{"x-amz-checksum-sha256", checksum}]
        )

      assert put.status == 200
      full = Req.get!(url, retry: false)
      assert full.body == "0123456789"
      assert Req.Response.get_header(full, "x-amz-checksum-sha256") == [checksum]

      targets =
        if enabled,
          do: [url, url <> "?versionId=" <> hd(Req.Response.get_header(put, "x-amz-version-id"))],
          else: [url]

      for target <- targets do
        partial = Req.get!(target, retry: false, headers: [{"range", "bytes=2-5"}])
        assert partial.status == 206
        assert partial.body == "2345"
        assert Req.Response.get_header(partial, "content-range") == ["bytes 2-5/10"]
        assert Req.Response.get_header(partial, "x-amz-checksum-sha256") == []
        assert Req.Response.get_header(partial, "x-amz-checksum-type") == []
      end
    end
  end
end
