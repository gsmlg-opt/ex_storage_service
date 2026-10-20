defmodule ExStorageServiceS3.MultipartProfileHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.{Multipart, Versioning}
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, false)
    on_exit(fn -> Application.put_env(:ex_storage_service, :s3_auth_enabled, previous) end)
    bucket = "multipart-profile-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    %{bucket: bucket}
  end

  test "negotiated composite checksum, tags and attributes survive completion retries", %{
    bucket: bucket
  } do
    :ok = Versioning.set_versioning(bucket, :enabled)

    response =
      Req.post!(@base <> "/#{bucket}/key?uploads",
        retry: false,
        body: "",
        headers: [
          {"content-type", "application/x-original"},
          {"content-encoding", "gzip"},
          {"content-language", "fr"},
          {"cache-control", "private"},
          {"content-disposition", "attachment"},
          {"expires", "Thu, 01 Jan 2037 00:00:00 GMT"},
          {"x-amz-meta-owner", "initial"},
          {"x-amz-tagging", "stage=initial"},
          {"x-amz-checksum-algorithm", "CRC32"},
          {"x-amz-checksum-type", "COMPOSITE"}
        ]
      )

    assert response.status == 200
    [_, upload] = Regex.run(~r/<UploadId>([^<]+)<\/UploadId>/, response.body)
    bytes = "raw compressed representation"
    crc = <<:erlang.crc32(bytes)::unsigned-big-32>>
    checksum = Base.encode64(crc)

    part =
      Req.put!(@base <> "/#{bucket}/key?uploadId=#{upload}&partNumber=1",
        retry: false,
        body: bytes,
        headers: [{"x-amz-checksum-crc32", checksum}, {"x-amz-sdk-checksum-algorithm", "CRC32"}]
      )

    assert part.status == 200
    assert Req.Response.get_header(part, "x-amz-checksum-crc32") == [checksum]
    [etag] = Req.Response.get_header(part, "etag")

    xml =
      "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>#{etag}</ETag><ChecksumCRC32>#{checksum}</ChecksumCRC32></Part></CompleteMultipartUpload>"

    composite = Base.encode64(<<:erlang.crc32(crc)::unsigned-big-32>>) <> "-1"
    path = @base <> "/#{bucket}/key?uploadId=#{upload}"

    completed =
      Req.post!(path,
        retry: false,
        body: xml,
        headers: [{"content-type", "application/xml"}, {"x-amz-checksum-crc32", composite}]
      )

    assert completed.status == 200
    assert completed.body =~ "<ChecksumCRC32>#{composite}</ChecksumCRC32>"
    assert Req.Response.get_header(completed, "x-amz-checksum-crc32") == [composite]
    [version] = Req.Response.get_header(completed, "x-amz-version-id")
    retry = Req.post!(path, retry: false, body: xml)
    assert retry.status == 200
    assert Req.Response.get_header(retry, "x-amz-version-id") == [version]
    assert {:ok, %{metadata: metadata}} = ObjectService.head(bucket, "key")
    assert metadata.content_type == "application/x-original"
    assert metadata.content_encoding == "gzip"
    assert metadata.content_language == "fr"
    assert metadata.cache_control == "private"
    assert metadata.content_disposition == "attachment"
    assert metadata.expires == "Thu, 01 Jan 2037 00:00:00 GMT"
    assert metadata.metadata == %{"owner" => "initial"}
    assert metadata.tags == %{"stage" => "initial"}
    assert metadata.checksums == %{"crc32" => composite}
    assert metadata.checksum_type == "COMPOSITE"

    for target <- ["key", "key?versionId=#{URI.encode_www_form(version)}"],
        method <- [:head, :get] do
      response =
        Req.request!(
          method: method,
          url: @base <> "/#{bucket}/#{target}",
          retry: false,
          raw: true,
          headers: [{"x-amz-checksum-mode", "ENABLED"}]
        )

      assert response.status == 200
      assert Req.Response.get_header(response, "x-amz-checksum-crc32") == [composite]
      assert Req.Response.get_header(response, "x-amz-checksum-type") == ["COMPOSITE"]
      if method == :get, do: assert(response.body == bytes)
    end

    copied =
      Req.put!(@base <> "/#{bucket}/copied",
        retry: false,
        body: "",
        headers: [{"x-amz-copy-source", "/#{bucket}/key"}]
      )

    assert copied.status == 200
    copied_etag = Base.encode16(:crypto.hash(:md5, bytes), case: :lower)
    assert copied.body =~ "<ETag>&quot;#{copied_etag}&quot;</ETag>"
    assert copied.body =~ "<ChecksumCRC32>#{checksum}</ChecksumCRC32>"
    assert copied.body =~ "<ChecksumType>FULL_OBJECT</ChecksumType>"
    assert {:ok, %{metadata: copied_metadata}} = ObjectService.head(bucket, "copied")
    assert copied_metadata.checksums == %{"crc32" => checksum}
    assert copied_metadata.checksum_type == "FULL_OBJECT"

    for method <- [:head, :get] do
      response =
        Req.request!(
          method: method,
          url: @base <> "/#{bucket}/copied",
          retry: false,
          raw: true,
          headers: [{"x-amz-checksum-mode", "ENABLED"}]
        )

      assert response.status == 200
      assert Req.Response.get_header(response, "x-amz-checksum-crc32") == [checksum]
      assert Req.Response.get_header(response, "x-amz-checksum-type") == ["FULL_OBJECT"]
      if method == :get, do: assert(response.body == bytes)
    end

    assert {:ok, versions} = Versioning.list_versions(bucket, "key")
    assert [%{version_id: ^version}] = versions
    assert {:ok, %{status: :completed}} = Multipart.get_upload(bucket, upload)
    assert Req.get!(path, retry: false).status == 404
    assert Req.delete!(path, retry: false).status == 404
    assert Req.put!(path <> "&partNumber=1", body: "late", retry: false).status == 404
  end

  test "bad negotiated completion checksum leaves no object and permits valid retry", %{
    bucket: bucket
  } do
    {:ok, upload} =
      Multipart.init_upload(bucket, "key", %{
        checksum_algorithm: "CRC32C",
        checksum_type: "FULL_OBJECT"
      })

    part =
      Req.put!(@base <> "/#{bucket}/key?uploadId=#{upload}&partNumber=1",
        body: "123456789",
        retry: false
      )

    assert part.status == 200
    [etag] = Req.Response.get_header(part, "etag")

    xml =
      "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>#{etag}</ETag><ChecksumCRC32C>4waSgw==</ChecksumCRC32C></Part></CompleteMultipartUpload>"

    path = @base <> "/#{bucket}/key?uploadId=#{upload}"

    assert Req.post!(path,
             body: xml,
             retry: false,
             headers: [{"x-amz-checksum-crc32c", "AAAAAA=="}]
           ).status == 400

    assert {:error, :object_not_found} = ObjectService.head(bucket, "key")
    assert {:ok, %{status: :initiated}} = Multipart.get_upload(bucket, upload)

    good =
      Req.post!(path, body: xml, retry: false, headers: [{"x-amz-checksum-crc32c", "4waSgw=="}])

    assert good.status == 200
    assert Req.Response.get_header(good, "x-amz-checksum-crc32c") == ["4waSgw=="]
  end

  test "all negotiated checksum algorithms round-trip through multipart HTTP", %{bucket: bucket} do
    alias ExStorageService.BlobStore.Source
    alias ExStorageServiceS3.MultipartChecksums

    profiles = [
      {"CRC32", "FULL_OBJECT"},
      {"CRC32", "COMPOSITE"},
      {"CRC32C", "FULL_OBJECT"},
      {"CRC32C", "COMPOSITE"},
      {"CRC64NVME", "FULL_OBJECT"},
      {"SHA1", "COMPOSITE"},
      {"SHA256", "COMPOSITE"}
    ]

    for {algorithm, type} <- profiles do
      key = algorithm <> "-" <> type

      init =
        Req.post!(@base <> "/#{bucket}/#{key}?uploads",
          retry: false,
          body: "",
          headers: [{"x-amz-checksum-algorithm", algorithm}, {"x-amz-checksum-type", type}]
        )

      assert init.status == 200
      [_, upload] = Regex.run(~r/<UploadId>([^<]+)<\/UploadId>/, init.body)
      name = String.downcase(algorithm)

      {:ok, %{^name => part_checksum}} =
        MultipartChecksums.checksum_source(Source.stream(["123456789"], 9), [name])

      part =
        Req.put!(@base <> "/#{bucket}/#{key}?uploadId=#{upload}&partNumber=1",
          retry: false,
          body: "123456789",
          headers: [{"x-amz-checksum-" <> name, part_checksum}]
        )

      assert part.status == 200
      [etag] = Req.Response.get_header(part, "etag")

      xml =
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>#{etag}</ETag><Checksum#{algorithm}>#{part_checksum}</Checksum#{algorithm}></Part></CompleteMultipartUpload>"

      result = Req.post!(@base <> "/#{bucket}/#{key}?uploadId=#{upload}", retry: false, body: xml)
      assert result.status == 200
      assert result.body =~ "<ChecksumType>#{type}</ChecksumType>"
      {:ok, %{metadata: metadata}} = ObjectService.head(bucket, key)
      [value] = Req.Response.get_header(result, "x-amz-checksum-" <> name)
      assert metadata.checksums[name] == value

      if type == "FULL_OBJECT",
        do: assert(value == part_checksum),
        else: assert(String.ends_with?(value, "-1"))
    end
  end

  test "part and upload pages expose bounded markers and reject invalid parameters", %{
    bucket: bucket
  } do
    {:ok, upload} = Multipart.init_upload(bucket, "key")

    for number <- [1, 3, 9],
        do: assert({:ok, _} = Multipart.store_part(bucket, upload, number, "part"))

    page = Req.get!(@base <> "/#{bucket}/key?uploadId=#{upload}&max-parts=2", retry: false)
    assert page.status == 200
    assert page.body =~ "<NextPartNumberMarker>3</NextPartNumberMarker>"
    assert page.body =~ "<IsTruncated>true</IsTruncated>"
    assert length(Regex.scan(~r/<Part>/, page.body)) == 2

    assert Req.get!(@base <> "/#{bucket}/key?uploadId=#{upload}&max-parts=-1", retry: false).status ==
             400

    {:ok, _} = Multipart.init_upload(bucket, "tree/a")
    {:ok, _} = Multipart.init_upload(bucket, "tree/b")
    uploads = Req.get!(@base <> "/#{bucket}?uploads&delimiter=/&max-uploads=1", retry: false)
    assert uploads.status == 200
    assert uploads.body =~ "<IsTruncated>true</IsTruncated>"
    assert uploads.body =~ "<NextKeyMarker>key</NextKeyMarker>"
    zero = Req.get!(@base <> "/#{bucket}?uploads&max-uploads=0", retry: false)
    assert zero.status == 200
    refute zero.body =~ "<Upload>"
    assert Req.get!(@base <> "/#{bucket}?uploads&max-uploads=1001", retry: false).status == 400
  end

  test "strict completion XML and part order errors preserve upload and object state", %{
    bucket: bucket
  } do
    {:ok, upload} = Multipart.init_upload(bucket, "key")
    {:ok, etag} = Multipart.store_part(bucket, upload, 1, "bytes")
    part = "<Part><PartNumber>1</PartNumber><ETag>#{etag}</ETag></Part>"

    invalid = [
      "<Other>#{part}</Other>",
      "<CompleteMultipartUpload>#{part}</CompleteMultipartUpload>junk",
      "<CompleteMultipartUpload><Wrapper>#{part}</Wrapper></CompleteMultipartUpload>",
      "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag></ETag></Part></CompleteMultipartUpload>"
    ]

    for body <- invalid do
      response = Req.post!(@base <> "/#{bucket}/key?uploadId=#{upload}", retry: false, body: body)
      assert response.status == 400
      assert response.body =~ "MalformedXML"
    end

    duplicate =
      Req.post!(@base <> "/#{bucket}/key?uploadId=#{upload}",
        retry: false,
        body: "<CompleteMultipartUpload>#{part}#{part}</CompleteMultipartUpload>"
      )

    assert duplicate.status == 400
    assert duplicate.body =~ "InvalidPartOrder"

    empty =
      Req.post!(@base <> "/#{bucket}/key?uploadId=#{upload}",
        retry: false,
        body: "<CompleteMultipartUpload/>"
      )

    assert empty.status == 400
    assert empty.body =~ "InvalidPart"
    assert {:ok, %{status: :initiated}} = Multipart.get_upload(bucket, upload)
    assert {:error, :object_not_found} = ObjectService.head(bucket, "key")
  end
end
