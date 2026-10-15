Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeBucketAliasesTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, Management, Parser, Runner}
  @moduletag :tmp_dir

  test "native bucket flags are scoped away from AWS mb" do
    for flag <- ~w(ignore-existing with-lock with-versioning) do
      assert {:ok, _} = Parser.parse(["mb", "s3://bucket", "--" <> flag])
      assert {:error, %Error{}} = Parser.parse(["s3", "mb", "s3://bucket", "--" <> flag])
    end
  end

  test "native bucket creation sends lock and region then enables versioning" do
    {endpoint, _} = WireServer.fixture([{200, [], ""}, {200, [], ""}])

    assert {:ok, result} =
             bucket(endpoint, %{with_lock: true, with_versioning: true}, "us-west-2")

    assert result["Created"]
    assert result["ObjectLockEnabledOnCreate"]
    assert result["VersioningEnabled"]
    assert_receive {:wire_request, create}
    assert create.method == "PUT"
    assert create.target == "/bucket"
    assert create.headers["x-amz-bucket-object-lock-enabled"] == "true"
    assert create.body =~ "<LocationConstraint>us-west-2</LocationConstraint>"
    assert_receive {:wire_request, version}
    assert version.method == "PUT"
    assert version.target == "/bucket?versioning="
    assert version.body =~ "<Status>Enabled</Status>"
  end

  test "unflagged native and AWS bucket creation retain their single-request contract" do
    for frontend <- [:native, :s3] do
      {endpoint, _} = WireServer.fixture([{200, [], ""}])
      invocation = %Invocation{frontend: frontend, command: ["mb"]}

      assert :ok =
               HighLevel.run(
                 invocation,
                 [target()],
                 [%Context{endpoint: endpoint, unsigned: true}],
                 emit: fn _ -> :ok end
               )

      assert_receive {:wire_request, request}
      assert request.target == "/bucket"
      refute Map.has_key?(request.headers, "x-amz-bucket-object-lock-enabled")
      refute_receive {:wire_request, _}, 50
    end
  end

  test "lock creation enables versioning without an unnecessary versioning mutation" do
    {endpoint, _} = WireServer.fixture([{200, [], ""}])
    assert {:ok, result} = bucket(endpoint, %{with_lock: true})
    assert result["ObjectLockEnabledOnCreate"]
    assert result["VersioningEnabled"]
    assert_receive {:wire_request, %{target: "/bucket"}}
    refute_receive {:wire_request, _}, 50
  end

  test "ignore-existing only accepts the two modeled creation conflicts" do
    for code <- ~w(BucketAlreadyOwnedByYou BucketAlreadyExists) do
      {endpoint, _} = WireServer.fixture([{409, [], error_xml(code)}])
      assert {:ok, result} = bucket(endpoint, %{ignore_existing: true, with_lock: true})
      refute result["Created"]
      assert result["AlreadyExists"]
      refute result["ObjectLockEnabledOnCreate"]
      assert_receive {:wire_request, %{method: "PUT"}}
    end

    for {code, ignore} <- [
          {"AccessDenied", true},
          {"OperationAborted", true},
          {"BucketAlreadyOwnedByYou", false}
        ] do
      {endpoint, _} = WireServer.fixture([{409, [], error_xml(code)}])
      assert {:error, %Error{code: ^code}} = bucket(endpoint, %{ignore_existing: ignore})
      assert_receive {:wire_request, %{method: "PUT"}}
    end
  end

  test "ignored existing bucket still receives requested versioning" do
    {endpoint, _} =
      WireServer.fixture([{409, [], error_xml("BucketAlreadyOwnedByYou")}, {200, [], ""}])

    assert {:ok, result} = bucket(endpoint, %{ignore_existing: true, with_versioning: true})
    assert result["AlreadyExists"]
    assert result["VersioningEnabled"]
    assert_receive {:wire_request, %{target: "/bucket"}}
    assert_receive {:wire_request, %{target: "/bucket?versioning="}}
  end

  test "versioning failure reports the completed creation without rollback" do
    {endpoint, _} = WireServer.fixture([{200, [], ""}, {403, [], error_xml("AccessDenied")}])

    assert {:error, %Error{code: "AccessDenied", operation: "PutBucketVersioning"} = error} =
             bucket(endpoint, %{with_versioning: true})

    assert error.details.bucket_created
    refute error.details.bucket_already_exists
    assert error.message =~ "Bucket created"
    assert_receive {:wire_request, %{method: "PUT", target: "/bucket"}}
    assert_receive {:wire_request, %{method: "PUT", target: "/bucket?versioning="}}
    refute_receive {:wire_request, _}, 50
  end

  test "declared ESS lock exclusion fails before transport" do
    invocation = %Invocation{command: ["mb"], options: %{with_lock: true}}

    context = %Context{
      endpoint: "http://127.0.0.1:1",
      unsigned: true,
      capabilities: %{provider: "ess"}
    }

    assert {:error, %Error{code: :provider_unsupported}} =
             HighLevel.run(invocation, [target()], [context])
  end

  test "runner applies native mb flags without an explicit output selector", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], ""}, {200, [], ""}])

    output =
      capture_io(fn ->
        assert :ok =
                 Runner.run([
                   "mb",
                   "s3://bucket",
                   "--with-versioning",
                   "--no-sign-request",
                   "--endpoint",
                   endpoint,
                   "--config-dir",
                   root
                 ])
      end)

    assert output =~ "bucket"
    assert_receive {:wire_request, %{target: "/bucket"}}
    assert_receive {:wire_request, %{target: "/bucket?versioning="}}
  end

  test "deprecated lifecycle and encryption spellings share exact handler specs" do
    commands = Management.commands()

    for {alias_name, action} <- [
          {"add", "add"},
          {"edit", "edit"},
          {"ls", "list"},
          {"rm", "remove"},
          {"export", "export"},
          {"import", "import"}
        ] do
      assert commands[["ilm", alias_name]] == commands[["ilm", "rule", action]]

      assert {:ok, %{command: ["ilm", ^alias_name]}} =
               Parser.parse(["ilm", alias_name, "s3://bucket"])
    end

    assert commands[["encrypt", "clear"]] == commands[["encrypt", "remove"]]
    assert commands[["encrypt", "info"]] == commands[["encrypt", "get"]]
  end

  test "deprecated lifecycle aliases preserve selected rule edits and removal guards" do
    original = %{
      "Rules" => [%{"ID" => "selected", "Status" => "Enabled", "Expiration" => %{"Days" => 20}}]
    }

    owner = self()

    call = fn
      "GetBucketLifecycleConfiguration", _ ->
        {:ok, original}

      operation, input ->
        send(owner, {:effect, operation, input})
        {:ok, %{}}
    end

    assert {:ok, _} = manage(["ilm", "add"], %{expire_days: 30}, call)
    assert_receive {:effect, "PutBucketLifecycleConfiguration", added}
    assert length(added["LifecycleConfiguration"]["Rules"]) == 2
    assert hd(added["LifecycleConfiguration"]["Rules"]) == hd(original["Rules"])
    assert {:ok, _} = manage(["ilm", "edit"], %{id: "selected", expire_days: 40}, call)
    assert_receive {:effect, "PutBucketLifecycleConfiguration", edited}
    assert hd(edited["LifecycleConfiguration"]["Rules"])["Expiration"] == %{"Days" => 40}
    assert {:ok, ^original} = manage(["ilm", "ls"], %{}, call)
    assert {:ok, ^original} = manage(["ilm", "export"], %{}, call)
    assert {:error, %Error{}} = manage(["ilm", "rm"], %{all: true}, call)
    refute_receive {:effect, _, _}, 50
    assert {:ok, _} = manage(["ilm", "rm"], %{all: true, force: true}, call)
    assert_receive {:effect, "DeleteBucketLifecycle", %{"Bucket" => "bucket"}}
  end

  test "lifecycle import alias consumes bounded stdin and encryption aliases preserve modeled effects" do
    owner = self()

    document = %{
      "Rules" => [
        %{
          "ID" => "imported",
          "Status" => "Enabled",
          "Filter" => %{"Prefix" => ""},
          "Expiration" => %{"Days" => 10}
        }
      ]
    }

    {:ok, input} = StringIO.open(JSON.encode!(document))

    call = fn operation, payload ->
      send(owner, {:effect, operation, payload})
      {:ok, %{}}
    end

    invocation = %Invocation{command: ["ilm", "import"], args: ["s3://bucket"]}

    try do
      assert {:ok, _} = Management.run(invocation, target(), %Context{}, call: call, stdin: input)
    after
      StringIO.close(input)
    end

    assert_receive {:effect, "PutBucketLifecycleConfiguration",
                    %{"LifecycleConfiguration" => ^document}}

    assert {:ok, _} = manage(["encrypt", "clear"], %{}, call)
    assert_receive {:effect, "DeleteBucketEncryption", %{"Bucket" => "bucket"}}
    assert {:ok, _} = manage(["encrypt", "info"], %{}, call)
    assert_receive {:effect, "GetBucketEncryption", %{"Bucket" => "bucket"}}
    assert {:ok, _} = manage(["encrypt", "clear"], %{dry_run: true}, call)
    refute_receive {:effect, _, _}, 50
  end

  defp target, do: %Target{kind: :remote, bucket: "bucket", key: ""}
  defp error_xml(code), do: "<Error><Code>#{code}</Code><Message>fixture</Message></Error>"

  defp bucket(endpoint, options, region \\ "us-east-1") do
    HighLevel.run(
      %Invocation{command: ["mb"], options: options},
      [target()],
      [%Context{endpoint: endpoint, unsigned: true, region: region}],
      max_attempts: 1,
      emit: fn _ -> :ok end
    )
  end

  defp manage(command, options, call),
    do:
      Management.run(
        %Invocation{command: command, args: ["s3://bucket"], options: options},
        target(),
        %Context{},
        call: call
      )
end
