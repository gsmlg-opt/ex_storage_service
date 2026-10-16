Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeConditionalPutTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, Parser}
  alias ExStorageServiceCli.Transfer.{Journal, Multipart, Planner, Source}
  @moduletag :tmp_dir
  @limits %{
    max_single_put_bytes: 5,
    min_nonfinal_part_bytes: 5,
    max_part_bytes: 10,
    max_parts: 10,
    max_object_bytes: 100
  }
  @create "<InitiateMultipartUploadResult><UploadId>owned</UploadId></InitiateMultipartUploadResult>"
  @complete "<CompleteMultipartUploadResult><ETag>&quot;created&quot;</ETag></CompleteMultipartUploadResult>"
  @precondition "<Error><Code>PreconditionFailed</Code><Message>Object already exists</Message></Error>"

  defp invocation(command \\ "put"),
    do: %Invocation{
      frontend: :native,
      command: [command],
      options: %{if_not_exists: true, progress: false}
    }

  defp remote, do: %Target{kind: :remote, bucket: "bucket", key: "key"}

  defp context(endpoint),
    do: %Context{
      endpoint: endpoint,
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret"
    }

  defp options(root),
    do: [
      state_root: Path.join(root, "journals"),
      tmp_dir: root,
      max_attempts: 1,
      emit: fn _ -> :ok end
    ]

  defp put(root, endpoint, data, opts \\ []) do
    path = Path.join(root, "source")
    File.write!(path, data)

    HighLevel.run(
      invocation(),
      [%Target{kind: :local, path: path}, remote()],
      [nil, context(endpoint)],
      Keyword.merge(options(root), opts)
    )
  end

  test "only native put accepts the conditional creation flag" do
    assert {:ok, %{options: %{if_not_exists: true}}} =
             Parser.parse(["put", "source", "s3://bucket/key", "--if-not-exists"])

    for command <- ~w(cp mv get mirror sync pipe) do
      assert {:error, %Error{category: :syntax}} =
               Parser.parse([command, "source", "s3://bucket/key", "--if-not-exists"])
    end

    assert {:error, %Error{category: :syntax}} =
             Parser.parse(["s3", "cp", "source", "s3://bucket/key", "--if-not-exists"])
  end

  test "conditional upload rejects download, copy and injected command flags before requests", %{
    tmp_dir: root
  } do
    for {command, targets} <- [
          {"put", [remote(), %Target{kind: :local, path: Path.join(root, "destination")}]},
          {"put", [remote(), %{remote() | key: "other"}]},
          {"cp", [%Target{kind: :local, path: Path.join(root, "missing")}, remote()]}
        ] do
      {endpoint, _} = WireServer.fixture([{403, [], "<Error><Code>AccessDenied</Code></Error>"}])

      assert {:error, %Error{code: :conditional_put_direction}} =
               HighLevel.run(
                 invocation(command),
                 targets,
                 [context(endpoint), context(endpoint)],
                 options(root)
               )

      refute_receive {:wire_request, _}
    end
  end

  test "single upload signs If-None-Match without a preflight existence read", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"created\""}], ""}])
    assert :ok = put(root, endpoint, "raw")
    assert_receive {:wire_request, %{method: "PUT", body: "raw", headers: headers}}
    assert headers["if-none-match"] == "*"
    assert headers["authorization"] =~ "if-none-match"
    refute_receive {:wire_request, _}
  end

  test "single conditional rejection preserves the service failure and never overwrites", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.start(fn listener, owner ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        request = WireServer.request(socket)
        send(owner, {:wire_request, request})

        if request.headers["if-none-match"] == "*" do
          WireServer.response(socket, 412, [], @precondition)
        else
          send(owner, :overwritten)
          WireServer.response(socket, 200, [{"ETag", "\"overwritten\""}], "")
        end

        :gen_tcp.close(socket)
      end)

    assert {:error, %Error{category: :service, code: "PreconditionFailed", http_status: 412}} =
             put(root, endpoint, "raw")

    assert_receive {:wire_request, %{method: "PUT"}}
    refute_received :overwritten
    refute_receive {:wire_request, _}
  end

  test "multipart conditions completion only and verifies the completed version", %{tmp_dir: root} do
    {endpoint, _} =
      multipart_peer([
        {200, [{"x-amz-version-id", "created-version"}], @complete},
        {200,
         [
           {"ETag", "\"created\""},
           {"Content-Length", "10"},
           {"x-amz-version-id", "created-version"}
         ], ""}
      ])

    assert :ok = put(root, endpoint, "abcdefghij", limits: @limits)

    for method <- ["POST", "PUT", "PUT"] do
      assert_receive {:wire_request, %{method: ^method, headers: headers}}
      refute Map.has_key?(headers, "if-none-match")
    end

    assert_receive {:wire_request,
                    %{method: "POST", target: "/bucket/key?uploadId=owned", headers: headers}}

    assert headers["if-none-match"] == "*"
    assert headers["authorization"] =~ "if-none-match"

    assert_receive {:wire_request,
                    %{
                      method: "HEAD",
                      target: "/bucket/key?versionId=created-version",
                      headers: head
                    }}

    refute Map.has_key?(head, "if-none-match")
    assert {:ok, %{entries: entries}} = journal_inventory(root)
    assert Enum.any?(entries, &(&1["phase"] == "completed"))
  end

  for {identity, returned_version} <- [{"missing", nil}, {"mismatched", "other-version"}] do
    test "multipart completion rejects a #{identity} returned HEAD version", %{tmp_dir: root} do
      headers = [{"ETag", "\"created\""}, {"Content-Length", "10"}]

      headers =
        if unquote(returned_version),
          do: [{"x-amz-version-id", unquote(returned_version)} | headers],
          else: headers

      {endpoint, _} =
        multipart_peer([
          {200, [{"x-amz-version-id", "created-version"}], @complete},
          {200, headers, ""}
        ])

      assert {:error,
              %Error{
                category: :protocol,
                code: :completion_verification_failed,
                details: %{journal_id: id}
              }} = put(root, endpoint, "abcdefghij", limits: @limits)

      for _ <- 1..4, do: assert_receive({:wire_request, _})

      assert_receive {:wire_request,
                      %{method: "HEAD", target: "/bucket/key?versionId=created-version"}}

      refute_receive {:wire_request, _}

      assert {:ok, %{"phase" => "failed", "completion" => completion}} =
               Journal.get(Path.join(root, "journals"), id)

      assert completion["version_id"] == "created-version"
    end
  end

  test "multipart 412 aborts only the owned upload and preserves the rejection", %{tmp_dir: root} do
    {endpoint, _} = multipart_peer([{412, [], @precondition}, {204, [], ""}])

    assert {:error,
            %Error{
              category: :service,
              code: "PreconditionFailed",
              http_status: 412,
              details: %{journal_id: id, cleanup: "aborted"}
            }} =
             put(root, endpoint, "abcdefghij", limits: @limits)

    for _ <- 1..3, do: assert_receive({:wire_request, _})
    assert_receive {:wire_request, %{method: "POST", headers: headers}}
    assert headers["if-none-match"] == "*"
    assert_receive {:wire_request, %{method: "DELETE", target: "/bucket/key?uploadId=owned"}}
    refute_receive {:wire_request, _}

    assert {:ok, %{"cleanup" => "aborted", "phase" => "failed"}} =
             Journal.get(Path.join(root, "journals"), id)
  end

  test "retained multipart journal rejects a resume that drops its write condition", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], @create},
        {403, [], "<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>"}
      ])

    path = Path.join(root, "source")
    File.write!(path, "abcdefghij")
    {:ok, source} = Source.file(path)
    {:ok, plan} = Planner.plan(10, @limits)
    opts = Keyword.merge(options(root), keep_incomplete: true, input: %{"IfNoneMatch" => "*"})

    assert {:error, %Error{details: %{journal_id: id, cleanup: "retained"}}} =
             Multipart.upload(context(endpoint), "bucket", "key", source, plan, opts)

    for _ <- 1..2, do: assert_receive({:wire_request, _})

    assert {:error, %Error{code: :resume_conflict}} =
             Multipart.resume(context(endpoint), id, Keyword.put(opts, :input, %{}))

    refute_receive {:wire_request, _}

    assert {:ok, %{"cleanup" => "retained", "options" => recorded}} =
             Journal.get(Path.join(root, "journals"), id)

    assert recorded["input_digest"] ==
             Base.encode16(
               :crypto.hash(
                 :sha256,
                 :erlang.term_to_binary(%{"IfNoneMatch" => "*"}, [:deterministic])
               ),
               case: :lower
             )
  end

  test "multipart verification does not inherit the conditional write predicate", %{tmp_dir: root} do
    {endpoint, _} =
      multipart_peer([
        {200, [], @complete},
        {200, [{"ETag", "\"created\""}, {"Content-Length", "10"}], ""}
      ])

    path = Path.join(root, "source")
    File.write!(path, "abcdefghij")
    {:ok, source} = Source.file(path)
    {:ok, plan} = Planner.plan(10, @limits)

    assert {:ok, _} =
             Multipart.upload(
               context(endpoint),
               "bucket",
               "key",
               source,
               plan,
               Keyword.put(options(root), :input, %{"IfNoneMatch" => "*"})
             )

    for _ <- 1..4, do: assert_receive({:wire_request, _})
    assert_receive {:wire_request, %{method: "HEAD", headers: headers}}
    refute Map.has_key?(headers, "if-none-match")
  end

  defp multipart_peer(tail) do
    WireServer.fixture([
      {200, [], @create},
      {200, [{"ETag", "\"first\""}], ""},
      {200, [{"ETag", "\"second\""}], ""}
      | tail
    ])
  end

  defp journal_inventory(root), do: Journal.list(Path.join(root, "journals"))
end
