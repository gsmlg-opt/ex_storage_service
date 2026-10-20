defmodule ExStorageServiceS3.AuthorizationHTTPTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.Multipart

  @port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base "http://localhost:#{@port}"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    {:ok, user} = User.create_user("authorization-http-#{System.unique_integer([:positive])}")
    {:ok, key} = AccessKey.create_access_key(user.id)
    bucket = "authorization-http-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      if is_nil(previous),
        do: Application.delete_env(:ex_storage_service, :s3_auth_enabled),
        else: Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)

      case Policy.get_user_policies(user.id) do
        {:ok, policies} -> Enum.each(policies, &Policy.delete_policy(&1.id))
        _ -> :ok
      end

      Concord.delete("user_policies:#{user.id}")
      AccessKey.delete_key(key.access_key_id)
      User.delete_user(user.id)
    end)

    %{user: user, credentials: key, bucket: bucket}
  end

  test "independent raw signatures retain root SDK metadata and require listing permission", %{
    user: user,
    credentials: credentials,
    bucket: bucket
  } do
    path = "/?x-id=ListBuckets"
    headers = signed_headers(credentials, path)
    assert raw_get(path, headers) =~ "HTTP/1.1 403"
    grant(user.id, ["s3:ListAllMyBuckets"], ["*"])

    # The independent signer takes canonical values; the socket retains equivalent wire escapes.
    for {target, signing_target} <- [{"/", "/"}, {path, path}, {"/?x-id=List%42uckets", path}] do
      wire = raw_get(target, signed_headers(credentials, signing_target))
      assert wire =~ "HTTP/1.1 200"
      assert wire =~ "<ListAllMyBucketsResult"
      assert wire =~ "<Name>#{bucket}</Name>"
    end

    wire = raw_get("/?x-id=Changed", headers)
    assert wire =~ "HTTP/1.1 403"
    assert wire =~ "SignatureDoesNotMatch"

    for target <- ["/?x-id=ListBuckets&x-id=ListBuckets", "/?x-id=ListBuckets&prefix=logs"] do
      wire = raw_get(target, signed_headers(credentials, target))
      assert wire =~ "HTTP/1.1 400"
      refute wire =~ "<ListAllMyBucketsResult"
    end
  end

  test "independent presigned root listing keeps x-id signed", %{
    user: user,
    credentials: credentials,
    bucket: bucket
  } do
    url = presign(credentials, "/", query: [{"x-id", "ListBuckets"}])
    assert {:ok, %{status: 403}} = Req.get(url, retry: false)
    grant(user.id, ["s3:ListAllMyBuckets"], ["*"])

    for target <- [presign(credentials, "/"), url] do
      assert {:ok, %{status: 200, body: body}} = Req.get(target, retry: false)
      assert body =~ "<Name>#{bucket}</Name>"
    end

    mutated = String.replace(url, "x-id=ListBuckets", "x-id=Changed")
    assert {:ok, %{status: 403, body: body}} = Req.get(mutated, retry: false)
    assert body =~ "SignatureDoesNotMatch"
  end

  test "independently signed copy requires source read separately from destination write",
       context do
    %{user: user, credentials: credentials, bucket: bucket} = context
    {:ok, _} = ObjectService.put(bucket, "source", "protected bytes", "text/plain", %{})
    grant(user.id, ["s3:PutObject"], ["arn:aws:s3:::#{bucket}/destination"])
    headers = [{"x-amz-copy-source", "/#{bucket}/source"}]

    assert {:ok, %{status: 403}} =
             signed(credentials, :put, "/#{bucket}/destination", body: "", headers: headers)

    assert {:error, :object_not_found} = ObjectService.head(bucket, "destination")

    grant(user.id, ["s3:GetObject"], ["arn:aws:s3:::#{bucket}/source"])

    assert {:ok, %{status: 200}} =
             signed(credentials, :put, "/#{bucket}/destination", body: "", headers: headers)

    grant(user.id, ["s3:GetObject"], ["arn:aws:s3:::#{bucket}/destination"])

    assert {:ok, %{status: 200, body: "protected bytes"}} =
             signed(credentials, :get, "/#{bucket}/destination")
  end

  test "standard actions allow HEAD while an unmarked HEAD-only policy cannot GET", context do
    %{user: user, credentials: credentials, bucket: bucket} = context
    {:ok, _} = ObjectService.put(bucket, "object", "private", "text/plain", %{})
    head = grant(user.id, ["s3:HeadObject"], ["arn:ess:::#{bucket}/object"])
    :ok = Concord.put("policy:#{head.id}", Map.delete(head, :semantics))
    assert {:ok, %{status: 200, body: ""}} = signed(credentials, :head, "/#{bucket}/object")
    assert {:ok, %{status: 403}} = signed(credentials, :get, "/#{bucket}/object")

    grant(user.id, ["s3:ListBucket", "s3:GetObject"], [
      "arn:aws:s3:::#{bucket}",
      "arn:aws:s3:::#{bucket}/*"
    ])

    assert {:ok, %{status: 200}} = signed(credentials, :head, "/#{bucket}")
    assert {:ok, %{status: 200, body: "private"}} = signed(credentials, :get, "/#{bucket}/object")

    deny = %{effect: :deny, actions: ["s3:HeadObject"], resources: ["arn:ess:::#{bucket}/object"]}
    {:ok, _} = Policy.update_policy(head.id, %{statements: [deny]})
    assert {:ok, %{status: 403, body: ""}} = signed(credentials, :head, "/#{bucket}/object")
  end

  test "signed historical CopyObject needs version access and copies the selected bytes",
       context do
    %{user: user, credentials: credentials, bucket: bucket} = context
    :ok = ExStorageService.Storage.Versioning.set_versioning(bucket, :enabled)
    {:ok, %{version_id: old}} = ObjectService.put(bucket, "source", "old", "text/plain", %{})
    {:ok, _} = ObjectService.put(bucket, "source", "current", "text/plain", %{})
    grant(user.id, ["s3:PutObject", "s3:GetObject"], ["arn:aws:s3:::#{bucket}/*"])
    headers = [{"x-amz-copy-source", "/#{bucket}/source?versionId=#{old}"}]

    assert {:ok, %{status: 403}} =
             signed(credentials, :put, "/#{bucket}/copy", body: "", headers: headers)

    assert {:error, :object_not_found} = ObjectService.head(bucket, "copy")
    grant(user.id, ["s3:GetObjectVersion"], ["arn:aws:s3:::#{bucket}/source"])

    assert {:ok, %{status: 200}} =
             signed(credentials, :put, "/#{bucket}/copy", body: "", headers: headers)

    assert {:ok, %{status: 200, body: "old"}} = signed(credentials, :get, "/#{bucket}/copy")
    assert {:ok, %{status: 200, body: "current"}} = signed(credentials, :get, "/#{bucket}/source")
  end

  test "all signed multipart endpoints reject another key or bucket without changing parts",
       context do
    %{user: user, credentials: credentials, bucket: bucket} = context

    {:ok, upload_id} = Multipart.init_upload(bucket, "original")
    {:ok, etag} = Multipart.store_part(bucket, upload_id, 1, "original")

    completion =
      "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>#{etag}</ETag></Part></CompleteMultipartUpload>"

    for {method, query, body} <- [
          {:put, "partNumber=1&uploadId=#{upload_id}", "replacement"},
          {:get, "uploadId=#{upload_id}", ""},
          {:post, "uploadId=#{upload_id}", completion},
          {:delete, "uploadId=#{upload_id}", ""}
        ] do
      assert {:ok, %{status: 403}} =
               signed(credentials, method, "/#{bucket}/original?#{query}", body: body)

      assert {:ok, [%{etag: ^etag}]} = Multipart.list_parts(bucket, upload_id)
    end

    grant(user.id, ["s3:PutObject", "s3:ListMultipartUploadParts", "s3:AbortMultipartUpload"], [
      "*"
    ])

    other = "other-#{bucket}"
    :ok = Metadata.create_bucket(other)

    for {target_bucket, target_key} <- [{bucket, "different"}, {other, "original"}] do
      target = "/#{target_bucket}/#{target_key}"

      assert {:ok, %{status: 404}} =
               signed(credentials, :put, "#{target}?partNumber=1&uploadId=#{upload_id}",
                 body: "replacement"
               )

      assert {:ok, %{status: 404}} = signed(credentials, :get, "#{target}?uploadId=#{upload_id}")

      xml =
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>#{etag}</ETag></Part></CompleteMultipartUpload>"

      assert {:ok, %{status: 404}} =
               signed(credentials, :post, "#{target}?uploadId=#{upload_id}", body: xml)

      assert {:ok, %{status: 404}} =
               signed(credentials, :delete, "#{target}?uploadId=#{upload_id}")

      assert {:ok, [%{etag: ^etag}]} = Multipart.list_parts(bucket, upload_id)
    end

    assert {:ok, %{status: 200}} =
             signed(credentials, :get, "/#{bucket}/original?uploadId=#{upload_id}")

    :ok = Multipart.abort_upload(bucket, upload_id)
  end

  test "signed mixed batch deletion retains forbidden objects and quiet still returns errors",
       context do
    %{user: user, credentials: credentials, bucket: bucket} = context
    {:ok, _} = ObjectService.put(bucket, "allowed", "a", "text/plain", %{})
    {:ok, _} = ObjectService.put(bucket, "denied", "b", "text/plain", %{})
    {:ok, denied} = ObjectService.head(bucket, "denied")
    grant(user.id, ["s3:DeleteObject"], ["arn:aws:s3:::#{bucket}/allowed"])

    body =
      "<Delete><Quiet>true</Quiet><Object><Key>allowed</Key></Object><Object><Key>denied</Key></Object></Delete>"

    headers = [{"content-md5", :crypto.hash(:md5, body) |> Base.encode64()}]

    assert {:ok, %{status: 200, body: response}} =
             signed(credentials, :post, "/#{bucket}?delete", body: body, headers: headers)

    refute response =~ "<Deleted>"
    assert response =~ "<Error><Key>denied</Key><Code>AccessDenied</Code>"
    assert {:error, :object_not_found} = ObjectService.head(bucket, "allowed")
    assert {:ok, ^denied} = ObjectService.head(bucket, "denied")
    grant(user.id, ["s3:GetObject"], ["arn:aws:s3:::#{bucket}/denied"])
    assert {:ok, %{status: 200, body: "b"}} = signed(credentials, :get, "/#{bucket}/denied")
  end

  test "signed batches distinguish current deletion, historical deletion and forbidden versions",
       context do
    %{user: user, credentials: credentials, bucket: bucket} = context
    :ok = ExStorageService.Storage.Versioning.set_versioning(bucket, :enabled)

    versions =
      Map.new(["historical", "current-only"], fn key ->
        {:ok, %{version_id: old}} = ObjectService.put(bucket, key, "old", "text/plain", %{})
        {:ok, _} = ObjectService.put(bucket, key, "new", "text/plain", %{})
        {key, old}
      end)

    historical = versions["historical"]
    current_old = versions["current-only"]
    {:ok, %{version_id: latest}} = ObjectService.head(bucket, "historical")
    grant(user.id, ["s3:DeleteObject"], ["arn:aws:s3:::#{bucket}/current-only"])
    grant(user.id, ["s3:DeleteObjectVersion"], ["arn:aws:s3:::#{bucket}/historical"])

    body =
      "<Delete><Object><Key>historical</Key><VersionId>#{historical}</VersionId></Object>" <>
        "<Object><Key>current-only</Key><VersionId>#{current_old}</VersionId></Object>" <>
        "<Object><Key>current-only</Key></Object></Delete>"

    assert {:ok, %{status: 200, body: response}} =
             signed(credentials, :post, "/#{bucket}?delete",
               body: body,
               headers: [{"content-md5", :crypto.hash(:md5, body) |> Base.encode64()}]
             )

    assert response =~ "<Deleted><Key>historical</Key><VersionId>#{historical}</VersionId>"

    assert response =~
             "<Error><Key>current-only</Key><VersionId>#{current_old}</VersionId><Code>AccessDenied</Code>"

    assert response =~ "<Deleted><Key>current-only</Key>"
    assert {:error, :version_not_found} = ObjectService.head(bucket, "historical", historical, [])
    assert {:ok, %{version_id: ^latest}} = ObjectService.head(bucket, "historical")
    assert {:ok, _} = ObjectService.head(bucket, "current-only", current_old, [])
    assert {:ok, %{delete_marker: true}} = ObjectService.head(bucket, "current-only")
  end

  test "signed batch integrity and document limits reject without deleting retained versions",
       context do
    %{user: user, credentials: credentials, bucket: bucket} = context
    {:ok, _} = ObjectService.put(bucket, "retained", "keep", "text/plain", %{})
    grant(user.id, ["s3:DeleteObject"], ["arn:aws:s3:::#{bucket}/retained"])
    {:ok, before} = ExStorageService.Storage.Versioning.list_versions(bucket, "retained")
    one = "<Delete><Object><Key>retained</Key></Object></Delete>"

    for {body, digest, status} <- [
          {one, nil, 400},
          {one, Base.encode64(<<0::128>>), 400},
          {"<Delete>" <>
             String.duplicate("<Object><Key>retained</Key></Object>", 1001) <> "</Delete>",
           :valid, 400},
          {"<Delete><Object><Key>retained</Key><Key>other</Key></Object></Delete>", :valid, 400},
          {one <> String.duplicate(" ", 8 * 1024 * 1024), :valid, 413}
        ] do
      digest = if digest == :valid, do: :crypto.hash(:md5, body) |> Base.encode64(), else: digest
      headers = if digest, do: [{"content-md5", digest}], else: []

      assert {:ok, response} =
               signed(credentials, :post, "/#{bucket}?delete", body: body, headers: headers)

      assert response.status == status
      assert {:ok, %{delete_marker: false}} = ObjectService.head(bucket, "retained")

      assert {:ok, ^before} =
               ExStorageService.Storage.Versioning.list_versions(bucket, "retained")
    end
  end

  test "independent presigns enforce policy changes, key revocation, user suspension and expiry",
       context do
    %{user: user, credentials: credentials, bucket: bucket} = context
    {:ok, _} = ObjectService.put(bucket, "private", "secret", "text/plain", %{})
    policy = grant(user.id, ["s3:GetObject"], ["arn:aws:s3:::#{bucket}/private"])
    url = presign(credentials, "/#{bucket}/private")
    assert {:ok, %{status: 200, body: "secret"}} = Req.get(url, retry: false)

    {:ok, _} =
      Policy.update_policy(policy.id, %{
        statements: [%{effect: :deny, actions: ["s3:GetObject"], resources: ["*"]}]
      })

    assert {:ok, %{status: 403}} = Req.get(url, retry: false)
    {:ok, _} = Policy.update_policy(policy.id, %{statements: policy.statements})
    {:ok, _} = AccessKey.deactivate_key(credentials.access_key_id)
    assert {:ok, %{status: 403}} = Req.get(url, retry: false)
    {:ok, _} = AccessKey.activate_key(credentials.access_key_id)
    {:ok, _} = User.suspend_user(user.id)
    assert {:ok, %{status: 403}} = Req.get(url, retry: false)
    {:ok, _} = User.activate_user(user.id)
    assert {:ok, %{status: 200}} = Req.get(url, retry: false)

    expired =
      presign(credentials, "/#{bucket}/private",
        datetime: DateTime.add(DateTime.utc_now(), -3600),
        expires: 1
      )

    assert {:ok, %{status: 403}} = Req.get(expired, retry: false)
  end

  # Req signs independently of ESS; the socket sends the exact signed request target.
  defp signed_headers(credentials, path) do
    Req.Utils.aws_sigv4_headers(
      access_key_id: credentials.access_key_id,
      secret_access_key: credentials.secret_access_key,
      region: "us-east-1",
      service: :s3,
      datetime: DateTime.utc_now(),
      method: :get,
      url: @base <> path,
      headers: [],
      body: ""
    )
  end

  defp raw_get(path, headers) do
    {:ok, socket} = :gen_tcp.connect(~c"localhost", @port, [:binary, active: false], 2_000)

    try do
      :ok =
        :gen_tcp.send(socket, [
          "GET ",
          path,
          " HTTP/1.1\r\n",
          Enum.map(headers, fn {name, value} -> [name, ": ", value, "\r\n"] end),
          "Connection: close\r\n\r\n"
        ])

      receive_closed(socket, [])
    after
      :gen_tcp.close(socket)
    end
  end

  defp receive_closed(socket, chunks) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, chunk} -> receive_closed(socket, [chunk | chunks])
      {:error, :closed} -> chunks |> Enum.reverse() |> IO.iodata_to_binary()
      {:error, reason} -> flunk("signed HTTP connection failed: #{inspect(reason)}")
    end
  end

  defp signed(credentials, method, path, opts \\ []) do
    Req.request(
      Keyword.merge(
        [
          method: method,
          url: @base <> path,
          retry: false,
          aws_sigv4: [
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.secret_access_key,
            region: "us-east-1",
            service: :s3
          ]
        ],
        opts
      )
    )
  end

  defp presign(credentials, path, opts \\ []) do
    Req.Utils.aws_sigv4_url(
      Keyword.merge(
        [
          access_key_id: credentials.access_key_id,
          secret_access_key: credentials.secret_access_key,
          region: "us-east-1",
          service: :s3,
          datetime: DateTime.utc_now(),
          method: :get,
          url: @base <> path,
          expires: 3600
        ],
        opts
      )
    )
    |> URI.to_string()
  end

  defp grant(user_id, actions, resources) do
    {:ok, policy} =
      Policy.create_policy("http", [%{effect: :allow, actions: actions, resources: resources}])

    :ok = Policy.attach_policy(user_id, policy.id)
    policy
  end
end
