Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativePipeTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, Parser, Runner}
  @moduletag :tmp_dir

  defp invocation(options),
    do: %Invocation{frontend: :native, command: ["pipe"], options: options}

  defp target, do: %Target{kind: :remote, bucket: "bucket", key: "raw.gz"}
  defp context(endpoint), do: %Context{endpoint: endpoint, unsigned: true}

  defp options(root, stdin),
    do: [
      stdin: stdin,
      tmp_dir: root,
      max_attempts: 1,
      emit: fn _ -> :ok end,
      progress_write: fn _ -> :ok end
    ]

  defp with_stdin(bytes, callback) do
    {:ok, stdin} = StringIO.open(bytes)

    try do
      callback.(stdin)
    after
      StringIO.close(stdin)
    end
  end

  defp counted_input do
    owner = self()
    input = spawn(fn -> input_loop(owner) end)
    on_exit(fn -> send(input, :stop) end)
    input
  end

  defp input_loop(owner) do
    receive do
      {:io_request, from, reply, request} ->
        send(owner, {:stdin_read, request})
        send(from, {:io_reply, reply, :eof})
        input_loop(owner)

      :stop ->
        :ok
    end
  end

  test "dryrun does not read stdin, create a spool, or send a request", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], ""}])
    input = counted_input()
    owner = self()
    tracer = spawn(fn -> trace_loop(owner) end)
    on_exit(fn -> send(tracer, :stop) end)
    :erlang.trace_pattern({File, :mkdir, 1}, true, [:local])
    :erlang.trace(self(), true, [:call, {:tracer, tracer}])

    try do
      assert :ok =
               HighLevel.run(
                 invocation(%{dryrun: true}),
                 [target()],
                 [context(endpoint)],
                 options(root, input)
               )
    after
      :erlang.trace(self(), false, [:call])
      :erlang.trace_pattern({File, :mkdir, 1}, false, [:local])
    end

    refute_receive {:stdin_read, _}, 50
    refute_receive {:spool_write, _}, 50
    refute_receive {:wire_request, _}, 50
    assert File.ls!(root) == []
    send(tracer, :stop)
  end

  defp trace_loop(owner) do
    receive do
      {:trace, _, :call, {File, :mkdir, args}} ->
        send(owner, {:spool_write, args})
        trace_loop(owner)

      :stop ->
        :ok

      _ ->
        trace_loop(owner)
    end
  end

  test "source-only options fail before stdin, provider calls, or key-file reads", %{
    tmp_dir: root
  } do
    input = counted_input()

    for option <- [
          :include,
          :exclude,
          :follow_symlinks,
          :guess_mime_type,
          :source_region,
          :page_size,
          :ignore_glacier_warnings,
          :force_glacier_transfer,
          :checksum_mode,
          :sse_c_copy_source,
          :sse_c_copy_source_key,
          :copy_props,
          :metadata_directive,
          :recursive,
          :version_id,
          :rewind
        ] do
      assert {:error, %Error{code: :pipe_option}} =
               HighLevel.run(
                 invocation(%{option => "fileb://" <> Path.join(root, "must-not-read")}),
                 [target()],
                 [context("http://127.0.0.1:1")],
                 options(root, input)
               )
    end

    refute_receive {:stdin_read, _}, 50
    assert File.ls!(root) == []
  end

  test "Runner rejects copy-source flags before resolving an AWS credential_process", %{
    tmp_dir: root
  } do
    marker = Path.join(root, "credential-process-ran")
    helper = Path.join(root, "credential-helper")
    File.write!(helper, "#!/bin/sh\ntouch '" <> marker <> "'\nexit 1\n")
    File.chmod!(helper, 0o700)
    aws = Path.join(root, "aws-config")

    File.write!(
      aws,
      "[profile pipe-fixture]\ncredential_process = " <> helper <> "\nregion = us-east-1\n"
    )

    File.write!(
      Path.join(root, "config.toml"),
      "[aliases.fixture]\nendpoint = 'http://127.0.0.1:1'\nprovider = 'aws-profile'\nprofile = 'pipe-fixture'\n"
    )

    previous = System.get_env("AWS_CONFIG_FILE")
    System.put_env("AWS_CONFIG_FILE", aws)

    on_exit(fn ->
      if previous,
        do: System.put_env("AWS_CONFIG_FILE", previous),
        else: System.delete_env("AWS_CONFIG_FILE")
    end)

    before = File.ls!(root) |> Enum.sort()

    assert {:error, %Error{category: :syntax}} =
             Runner.run([
               "pipe",
               "fixture/bucket/key",
               "--config-dir",
               root,
               "--sse-c-copy-source-key",
               "ignored-key"
             ])

    refute File.exists?(marker)
    assert Enum.sort(File.ls!(root)) == before

    assert {:ok, _} =
             Parser.parse([
               "cp",
               "s3://bucket/key",
               "./local",
               "--sse-c-copy-source-key",
               "source-key"
             ])
  end

  test "retained upload fields and explicit checksum reach PutObject with raw bytes", %{
    tmp_dir: root
  } do
    bytes = <<0, 255, 31, 139, 8>>
    digest = Base.encode64(:crypto.hash(:sha256, bytes))

    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"fixture\""}, {"x-amz-checksum-sha256", digest}], ""}
      ])

    with_stdin(bytes, fn stdin ->
      assert :ok =
               HighLevel.run(
                 invocation(%{
                   metadata: "team=ops",
                   content_type: "application/gzip",
                   content_encoding: "gzip",
                   content_language: "en",
                   cache_control: "max-age=60",
                   content_disposition: "attachment",
                   storage_class: "STANDARD_IA",
                   acl: "private",
                   grants: ["read=id=fixture-owner"],
                   website_redirect: "/other",
                   request_payer: "requester",
                   checksum_algorithm: "SHA256",
                   expected_size: "5",
                   progress: false
                 }),
                 [target()],
                 [context(endpoint)],
                 options(root, stdin)
               )

      assert_receive {:wire_request, request}
      assert request.method == "PUT" and request.target == "/bucket/raw.gz"
      assert request.body == bytes

      expected = %{
        "content-length" => "5",
        "content-type" => "application/gzip",
        "content-encoding" => "gzip",
        "content-language" => "en",
        "cache-control" => "max-age=60",
        "content-disposition" => "attachment",
        "x-amz-storage-class" => "STANDARD_IA",
        "x-amz-acl" => "private",
        "x-amz-grant-read" => "id=fixture-owner",
        "x-amz-website-redirect-location" => "/other",
        "x-amz-request-payer" => "requester",
        "x-amz-meta-team" => "ops",
        "x-amz-checksum-sha256" => digest
      }

      assert Map.take(request.headers, Map.keys(expected)) == expected
      assert File.ls!(root) == []
    end)
  end

  test "retained SSE forms reach their destination headers", %{tmp_dir: root} do
    for {fields, expected} <- [
          {%{sse: "AES256"}, %{"x-amz-server-side-encryption" => "AES256"}},
          {%{sse: "aws:kms", sse_kms_key_id: "fixture-key"},
           %{
             "x-amz-server-side-encryption" => "aws:kms",
             "x-amz-server-side-encryption-aws-kms-key-id" => "fixture-key"
           }},
          {%{sse_c: "AES256", sse_c_key: String.duplicate("k", 32)},
           %{
             "x-amz-server-side-encryption-customer-algorithm" => "AES256",
             "x-amz-server-side-encryption-customer-key" =>
               Base.encode64(String.duplicate("k", 32)),
             "x-amz-server-side-encryption-customer-key-md5" =>
               Base.encode64(:crypto.hash(:md5, String.duplicate("k", 32)))
           }}
        ] do
      {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"fixture\""}], ""}])

      with_stdin("x", fn stdin ->
        assert :ok =
                 HighLevel.run(
                   invocation(Map.put(fields, :progress, false)),
                   [target()],
                   [context(endpoint)],
                   options(root, stdin)
                 )

        assert_receive {:wire_request, request}
        assert Map.take(request.headers, Map.keys(expected)) == expected
        assert File.ls!(root) == []
      end)
    end
  end

  test "progress settings have effects and quiet suppresses progress", %{tmp_dir: root} do
    owner = self()

    for quiet <- [false, true] do
      {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"fixture\""}], ""}])

      with_stdin("abc", fn stdin ->
        opts = Keyword.put(options(root, stdin), :progress_write, &send(owner, {:progress, &1}))

        assert :ok =
                 HighLevel.run(
                   invocation(%{progress_frequency: 1, progress_multiline: true, quiet: quiet}),
                   [target()],
                   [context(endpoint)],
                   opts
                 )

        assert_receive {:wire_request, _}

        if quiet,
          do: refute_receive({:progress, _}, 50),
          else: assert_receive({:progress, "Completed 3 Bytes\n"})
      end)
    end
  end

  test "invalid expected size fails before reads; size mismatch prevents upload and cleans spool",
       %{tmp_dir: root} do
    for value <- ["-1", "invalid", -1] do
      assert {:error, %Error{code: :pipe_option}} =
               HighLevel.run(
                 invocation(%{expected_size: value}),
                 [target()],
                 [context("http://127.0.0.1:1")],
                 options(root, counted_input())
               )
    end

    refute_receive {:stdin_read, _}, 50
    {endpoint, _} = WireServer.fixture([{200, [], ""}])

    with_stdin("abc", fn stdin ->
      assert {:error, %Error{code: :stdin_size}} =
               HighLevel.run(
                 invocation(%{expected_size: "4"}),
                 [target()],
                 [context(endpoint)],
                 options(root, stdin)
               )

      refute_receive {:wire_request, _}, 50
      assert File.ls!(root) == []
    end)
  end
end
