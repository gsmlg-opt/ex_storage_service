Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transfer.CopyTest do
  # Journal mutex ports share the host namespace with concurrent wire fixtures.
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.Transfer.{Copy, Journal}
  @moduletag :tmp_dir
  @fixture_attempts 4
  @copied "<CopyObjectResult><ETag>&quot;destination&quot;</ETag></CopyObjectResult>"
  @head [{"ETag", "\"source\""}, {"Content-Length", "10"}]
  @limits %{
    max_single_put_bytes: 5,
    min_nonfinal_part_bytes: 5,
    max_part_bytes: 10,
    max_parts: 10,
    max_object_bytes: 100
  }

  defp target(bucket, key), do: %Target{kind: :remote, bucket: bucket, key: key}

  test "same-service move verifies copy and deletes only the selected version" do
    {endpoint, _} =
      WireServer.fixture([
        {200, [{"x-amz-version-id", "v+ %"} | @head], ""},
        {200, [], @copied},
        {200, [{"ETag", "\"destination\""}, {"Content-Length", "10"}], ""},
        {200, [{"x-amz-version-id", "v+ %"} | @head], ""},
        {204, [], ""}
      ])

    context = %Context{endpoint: endpoint, unsigned: true}

    assert {:ok, %{source_deleted: true, bytes: 10}} =
             Copy.move(
               context,
               target("source", "a//../%+ ?#雪"),
               context,
               target("destination", "key")
             )

    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "PUT", headers: headers}}

    assert headers["x-amz-copy-source"] ==
             "/source/a//../%25%2B%20%3F%23%E9%9B%AA?versionId=v%2B%20%25"

    assert headers["x-amz-copy-source-if-match"] == "\"source\""
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "HEAD", target: source_head}}
    assert source_head =~ "versionId=v%2B%20%25"

    assert_receive {:wire_request,
                    %{
                      method: "DELETE",
                      target: "/source/a//../%25%2B%20%3F%23%E9%9B%AA?versionId=v%2B%20%25"
                    }}
  end

  test "unverified conditional deletion preserves source after a successful copy" do
    {endpoint, _} =
      WireServer.fixture([
        {200, @head, ""},
        {200, [], @copied},
        {200, [{"ETag", "\"destination\""}, {"Content-Length", "10"}], ""},
        {200, @head, ""}
      ])

    context = %Context{endpoint: endpoint, unsigned: true}

    assert {:error,
            %Error{
              code: :conditional_delete_unavailable,
              details: %{copy_completed: true, source_deleted: false}
            }} =
             Copy.move(context, target("source", "key"), context, target("destination", "key"))

    refute_receive {:wire_request, %{method: "DELETE"}}
  end

  test "copy errors, destination mismatch and same-resource moves perform no source deletion" do
    context = %Context{endpoint: "http://unused.test", unsigned: true}

    assert {:error, %Error{code: :same_resource}} =
             Copy.move(context, target("bucket", "key"), context, target("bucket", "key"))

    for response <- [
          {200, [], "<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>"},
          {200, [], @copied}
        ] do
      {endpoint, _} =
        WireServer.fixture([
          {200, @head, ""},
          response,
          {200, [{"ETag", "\"wrong\""}, {"Content-Length", "10"}], ""}
        ])

      context = %Context{endpoint: endpoint, unsigned: true}

      assert {:error, %Error{}} =
               Copy.move(context, target("source", "key"), context, target("destination", "key"))

      refute_receive {:wire_request, %{method: "DELETE"}}
    end
  end

  test "cross-endpoint bridge preserves raw gzip bytes and removes its staging file", %{
    tmp_dir: root
  } do
    body = :zlib.gzip("raw object bytes")

    source_head = [
      {"ETag", "\"source\""},
      {"Content-Length", to_string(byte_size(body))},
      {"Content-Encoding", "gzip"}
    ]

    {source, _} =
      WireServer.fixture([
        {200, source_head, ""},
        {200, source_head, ""},
        {200, [{"ETag", "\"source\""}, {"Content-Encoding", "gzip"}], body}
      ])

    {destination, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"destination\""}], ""},
        {200, [{"ETag", "\"destination\""}, {"Content-Length", to_string(byte_size(body))}], ""}
      ])

    assert {:ok, %{mode: :bridge, bytes: bytes}} =
             Copy.remote(
               %Context{endpoint: source, unsigned: true},
               target("source", "key"),
               %Context{endpoint: destination, unsigned: true},
               target("destination", "key"),
               copy_props: "none",
               input: %{"ContentEncoding" => "gzip"},
               state_root: root
             )

    assert bytes == byte_size(body)
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "GET", headers: %{"if-match" => "\"source\""}}}

    assert_receive {:wire_request,
                    %{method: "PUT", body: ^body, headers: %{"content-encoding" => "gzip"}}}

    assert_receive {:wire_request, %{method: "HEAD"}}
    assert File.ls!(Path.join(root, "staging")) == []
  end

  test "multipart server copy checkpoints source identity and sends exact owned ranges", %{
    tmp_dir: root
  } do
    create =
      "<InitiateMultipartUploadResult><UploadId>owned</UploadId></InitiateMultipartUploadResult>"

    part = fn tag -> "<CopyPartResult><ETag>&quot;#{tag}&quot;</ETag></CopyPartResult>" end

    complete =
      "<CompleteMultipartUploadResult><ETag>&quot;destination&quot;</ETag></CompleteMultipartUploadResult>"

    responses = [
      {200, @head, ""},
      {200, @head, ""},
      {200, [], create},
      {200, @head, ""},
      {200, [], part.("p1")},
      {200, @head, ""},
      {200, [], part.("p2")},
      {200, @head, ""},
      {200, [], complete},
      {200, [{"ETag", "\"destination\""}, {"Content-Length", "10"}], ""}
    ]

    {result, root} = multipart_copy(root, responses, copy_props: "none", limits: @limits)
    assert {:ok, %{journal_id: id, bytes: 10}} = result

    assert {:ok, %{"phase" => "completed", "source" => %{"kind" => "remote"}}} =
             Journal.get(root, id)

    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "POST", headers: create_headers}}
    refute Map.has_key?(create_headers, "x-amz-checksum-algorithm")
    assert_receive {:wire_request, %{method: "HEAD"}}

    assert_receive {:wire_request,
                    %{
                      method: "PUT",
                      body: "",
                      headers: %{"x-amz-copy-source-range" => "bytes=0-4"}
                    }}

    assert_receive {:wire_request, %{method: "HEAD"}}

    assert_receive {:wire_request,
                    %{
                      method: "PUT",
                      body: "",
                      headers: %{"x-amz-copy-source-range" => "bytes=5-9"}
                    }}

    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "POST", body: completion}}
    refute completion =~ "Checksum"
    refute_receive {:wire_request, %{method: "GET"}}
  end

  test "occupied initial mutex retries only HEAD setup using a fresh wire fixture", %{
    tmp_dir: tmp_dir
  } do
    {base, blocked_root, listener} =
      Enum.find_value(1..10, fn attempt ->
        base = Path.join(tmp_dir, "occupied-#{attempt}")
        blocked_root = Path.join(base, "journal-attempt-0")
        File.mkdir_p!(blocked_root)
        File.chmod!(blocked_root, 0o700)
        {:ok, port} = Journal.lock_port(blocked_root, "index")

        case :gen_tcp.listen(port, [:binary, active: false, ip: {127, 0, 0, 1}]) do
          {:ok, listener} -> {base, blocked_root, listener}
          {:error, :eaddrinuse} -> nil
        end
      end) || flunk("Could not reserve the initial copy fixture mutex")

    responses = [
      {200, @head, ""},
      {200, @head, ""},
      {200, [],
       "<InitiateMultipartUploadResult><UploadId>owned</UploadId></InitiateMultipartUploadResult>"},
      {200, @head, ""},
      {200, [], "<CopyPartResult><ETag>part1</ETag></CopyPartResult>"},
      {200, @head, ""},
      {200, [], "<CopyPartResult><ETag>part2</ETag></CopyPartResult>"},
      {200, @head, ""},
      {200, [],
       "<CompleteMultipartUploadResult><ETag>destination</ETag></CompleteMultipartUploadResult>"},
      {200, [{"ETag", "destination"}, {"Content-Length", "10"}], ""}
    ]

    try do
      {result, root} = multipart_copy(base, responses, copy_props: "none", limits: @limits)
      assert {:ok, %{journal_id: id}} = result
      assert root != blocked_root
      refute File.exists?(Path.join(blocked_root, "index.jsonl"))
      assert {:ok, %{"phase" => "completed"}} = Journal.get(root, id)

      assert Enum.frequencies_by(take_requests([]), & &1.method) == %{
               "HEAD" => 6,
               "POST" => 2,
               "PUT" => 2
             }

      assert {:ok, _} = :inet.sockname(listener)
    after
      :gen_tcp.close(listener)
    end
  end

  test "explicit multipart copy retains the selected returned checksums without source GET", %{
    tmp_dir: root
  } do
    sha = Base.encode64(:crypto.hash(:sha256, "selected part"))

    part =
      "<CopyPartResult><ETag>part</ETag><ChecksumSHA256>#{sha}</ChecksumSHA256><ChecksumCRC32>AAAAAA==</ChecksumCRC32></CopyPartResult>"

    responses = [
      {200, @head, ""},
      {200, @head, ""},
      {200, [],
       "<InitiateMultipartUploadResult><UploadId>owned</UploadId></InitiateMultipartUploadResult>"},
      {200, @head, ""},
      {200, [], part},
      {200, @head, ""},
      {200, [], part},
      {200, @head, ""},
      {200, [],
       "<CompleteMultipartUploadResult><ETag>destination</ETag></CompleteMultipartUploadResult>"},
      {200, [{"ETag", "destination"}, {"Content-Length", "10"}], ""}
    ]

    {result, root} =
      multipart_copy(root, responses,
        copy_props: "none",
        input: %{"ChecksumAlgorithm" => "SHA256"},
        limits: @limits
      )

    assert {:ok, %{journal_id: id}} = result

    assert {:ok, record} = Journal.get(root, id)
    assert Enum.all?(record["parts"], &(&1["checksums"] == %{"SHA256" => sha}))

    assert_receive {:wire_request,
                    %{method: "POST", headers: %{"x-amz-checksum-algorithm" => "SHA256"}}}

    assert_receive {:wire_request, %{method: "POST", body: completion}}
    assert length(Regex.scan(~r/<ChecksumSHA256>/, completion)) == 2
    refute completion =~ "ChecksumCRC32"
    refute_receive {:wire_request, %{method: "GET"}}
  end

  test "multipart copy aborts its owned upload when the selected part checksum is missing", %{
    tmp_dir: root
  } do
    responses = [
      {200, @head, ""},
      {200, @head, ""},
      {200, [],
       "<InitiateMultipartUploadResult><UploadId>owned</UploadId></InitiateMultipartUploadResult>"},
      {200, @head, ""},
      {200, [], "<CopyPartResult><ETag>part</ETag></CopyPartResult>"},
      {204, [], ""}
    ]

    {result, _selected_root} =
      multipart_copy(root, responses,
        copy_props: "none",
        input: %{"ChecksumAlgorithm" => "SHA256"},
        limits: @limits
      )

    assert {:error, %Error{code: :missing_copy_checksum, details: %{cleanup: "aborted"}}} = result

    assert_receive {:wire_request, %{method: "DELETE", target: "/destination/key?uploadId=owned"}}
    refute_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, %{method: "DELETE", target: "/source/key"}}
  end

  test "SSE-C server copy sends separate source and destination customer-key headers" do
    source_key = String.duplicate("s", 32)
    destination_key = String.duplicate("d", 32)

    {endpoint, _} =
      WireServer.fixture([
        {200, @head, ""},
        {200, @head, ""},
        {200, [], @copied},
        {200, [{"ETag", "\"destination\""}, {"Content-Length", "10"}], ""},
        {200, @head, ""}
      ])

    context = %Context{endpoint: endpoint, unsigned: true, options: %{debug: true}}
    owner = self()

    invocation = %ExStorageServiceCli.CLI.Invocation{
      frontend: :native,
      command: ["cp"],
      options: %{sse_c_key: destination_key, sse_c_copy_source_key: source_key, progress: false}
    }

    assert :ok =
             ExStorageServiceCli.CLI.HighLevel.run(
               invocation,
               [target("source", "key"), target("destination", "key")],
               [context, context],
               emit: fn _ -> :ok end,
               debug_write: &send(owner, {:debug_line, &1})
             )

    assert_receive {:wire_request, %{method: "HEAD", headers: source_headers}}

    assert source_headers["x-amz-server-side-encryption-customer-key"] ==
             Base.encode64(source_key)

    assert_receive {:wire_request, %{method: "HEAD", headers: second_source_headers}}

    assert second_source_headers["x-amz-server-side-encryption-customer-key"] ==
             Base.encode64(source_key)

    assert_receive {:wire_request, %{method: "PUT", headers: headers}}
    assert headers["x-amz-copy-source-server-side-encryption-customer-algorithm"] == "AES256"

    assert headers["x-amz-copy-source-server-side-encryption-customer-key"] ==
             Base.encode64(source_key)

    assert headers["x-amz-copy-source-server-side-encryption-customer-key-md5"] ==
             Base.encode64(:crypto.hash(:md5, source_key))

    assert headers["x-amz-server-side-encryption-customer-key"] == Base.encode64(destination_key)

    refute headers["x-amz-copy-source-server-side-encryption-customer-key"] ==
             headers["x-amz-server-side-encryption-customer-key"]

    debug = debug_output("")

    for key <- [source_key, destination_key] do
      refute debug =~ key
      refute debug =~ Base.encode64(key)
      refute debug =~ Base.encode64(:crypto.hash(:md5, key))
    end

    assert debug =~ "CopyObject"
  end

  test "multipart server copy sends source SSE-C on every part and keeps destination encryption separate",
       %{tmp_dir: root} do
    source_key = String.duplicate("s", 32)
    destination_key = String.duplicate("d", 32)

    customer = fn key ->
      %{
        "SSECustomerAlgorithm" => "AES256",
        "SSECustomerKey" => Base.encode64(key),
        "SSECustomerKeyMD5" => Base.encode64(:crypto.hash(:md5, key))
      }
    end

    responses = [
      {200, @head, ""},
      {200, @head, ""},
      {200, [],
       "<InitiateMultipartUploadResult><UploadId>owned</UploadId></InitiateMultipartUploadResult>"},
      {200, @head, ""},
      {200, [], "<CopyPartResult><ETag>p1</ETag></CopyPartResult>"},
      {200, @head, ""},
      {200, [], "<CopyPartResult><ETag>p2</ETag></CopyPartResult>"},
      {200, @head, ""},
      {200, [],
       "<CompleteMultipartUploadResult><ETag>destination</ETag></CompleteMultipartUploadResult>"},
      {200, [{"ETag", "destination"}, {"Content-Length", "10"}], ""}
    ]

    {result, root} =
      multipart_copy(root, responses,
        copy_props: "none",
        source_input: customer.(source_key),
        input: customer.(destination_key),
        limits: @limits
      )

    assert {:ok, %{journal_id: id}} = result

    assert {:ok, record} = Journal.get(root, id)
    serialized = JSON.encode!(record)

    for key <- [source_key, destination_key] do
      refute serialized =~ key
      refute serialized =~ Base.encode64(key)
      refute serialized =~ Base.encode64(:crypto.hash(:md5, key))
    end

    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "POST", headers: create}}
    assert create["x-amz-server-side-encryption-customer-key"] == Base.encode64(destination_key)
    refute Map.has_key?(create, "x-amz-copy-source-server-side-encryption-customer-key")

    for _ <- 1..2 do
      assert_receive {:wire_request, %{method: "HEAD"}}
      assert_receive {:wire_request, %{method: "PUT", headers: headers}}
      assert headers["x-amz-copy-source-server-side-encryption-customer-algorithm"] == "AES256"

      assert headers["x-amz-copy-source-server-side-encryption-customer-key"] ==
               Base.encode64(source_key)

      assert headers["x-amz-copy-source-server-side-encryption-customer-key-md5"] ==
               Base.encode64(:crypto.hash(:md5, source_key))

      assert headers["x-amz-server-side-encryption-customer-key"] ==
               Base.encode64(destination_key)
    end
  end

  defp debug_output(acc) do
    receive do
      {:debug_line, line} -> debug_output(acc <> line)
    after
      0 -> acc
    end
  end

  defp multipart_copy(base, responses, opts, attempt \\ 0) do
    {endpoint, server} =
      WireServer.start(fn listener, owner ->
        send(owner, {:copy_fixture_listener, self(), listener})

        Enum.each(responses, fn {status, headers, body} ->
          {:ok, socket} = :gen_tcp.accept(listener, 15_000)
          send(owner, {:wire_request, WireServer.request(socket)})
          WireServer.response(socket, status, headers, body)
          :gen_tcp.close(socket)
        end)
      end)

    assert_receive {:copy_fixture_listener, ^server, listener}
    context = %Context{endpoint: endpoint, unsigned: true}
    root = Path.join(base, "journal-attempt-#{attempt}")

    result =
      Copy.remote(
        context,
        target("source", "key"),
        context,
        target("destination", "key"),
        Keyword.put(opts, :state_root, root)
      )

    case result do
      {:error,
       %Error{
         code: :journal_busy,
         details: %{mutex_port: _, socket_reason: :eaddrinuse} = details
       }}
      when map_size(details) == 2 and attempt + 1 < @fixture_attempts ->
        # Copy captures and validates its source before acquiring the journal.
        # Retry only read-only setup, never a request that began an owned upload.
        requests = take_requests([])
        assert requests != []
        assert Enum.all?(requests, &(&1.method == "HEAD")), inspect(requests)
        monitor = Process.monitor(server)
        if Process.alive?(server), do: Process.exit(server, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^server, _}
        :gen_tcp.close(listener)
        multipart_copy(base, responses, opts, attempt + 1)

      _ ->
        {result, root}
    end
  end

  defp take_requests(requests) do
    receive do
      {:wire_request, request} -> take_requests([request | requests])
    after
      0 -> Enum.reverse(requests)
    end
  end
end
