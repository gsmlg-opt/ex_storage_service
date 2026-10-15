Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutableBaselineTest do
  use ExUnit.Case, async: false

  alias ExStorageServiceCli.Test.WireServer

  @moduletag :tmp_dir
  @commands ~w(configure mb rb ls tree cp rm mv presign info version)
  @buckets "<ListAllMyBucketsResult><Buckets><Bucket><Name>sample</Name><CreationDate>2026-01-01T00:00:00Z</CreationDate></Bucket></Buckets></ListAllMyBucketsResult>"
  @objects "<ListBucketResult><Name>sample</Name><Prefix>docs/</Prefix><KeyCount>1</KeyCount><IsTruncated>false</IsTruncated><Contents><Key>docs/file.txt</Key><Size>3</Size><ETag>&quot;abc&quot;</ETag><LastModified>2026-01-01T00:00:00Z</LastModified><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"

  setup_all do
    directory =
      Path.join(System.tmp_dir!(), "ess_executable_#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    executable = Path.join(directory, "ess")
    root = Path.expand("../..", __DIR__)

    {output, status} =
      System.cmd(
        "mix",
        ["escript.build", "--no-compile"],
        cd: root,
        env: [{"MIX_ENV", "test"}, {"PAGER", "cat"}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    File.cp!(Path.join(root, "ess"), executable)
    on_exit(fn -> File.rm_rf!(directory) end)
    %{executable: executable}
  end

  test "every existing command has executable help without server access", context do
    for command <- @commands do
      {output, 0} = run(context, ["--help", command])
      assert output =~ "ess #{command}"
      refute output =~ "fixture-secret"
    end

    {output, 0} = run(context, ["--json", "version"])
    assert JSON.decode!(output) == %{"version" => ExStorageServiceCli.version()}
  end

  test "legacy bucket and removal JSON outputs and return codes", context do
    signed_fixture(context)

    for {command, target, method, path, expected} <- [
          {"mb", "sample", "PUT", "/sample", %{"status" => "ok", "bucket" => "sample"}},
          {"rb", "s3://sample/", "DELETE", "/sample", %{"status" => "ok", "bucket" => "sample"}},
          {"rm", "s3://sample/docs/file.txt", "DELETE", "/sample/docs/file.txt",
           %{"status" => "ok", "bucket" => "sample", "key" => "docs/file.txt"}}
        ] do
      {endpoint, _server} = WireServer.fixture([{204, [], ""}])
      {output, 0} = run(context, ["--endpoint", endpoint, "--json", command, target])
      assert JSON.decode!(output) == expected
      assert_receive {:wire_request, %{method: ^method, target: ^path, body: ""}}
    end
  end

  test "listing tree and info preserve structured results", context do
    signed_fixture(context)
    {endpoint, _server} = WireServer.fixture([{200, [], @buckets}])
    {output, 0} = run(context, ["--endpoint", endpoint, "--json", "ls"])

    assert [%{"name" => "sample", "creation_date" => "2026-01-01T00:00:00Z"}] =
             JSON.decode!(output)

    for command <- ["ls", "tree"] do
      {endpoint, _server} = WireServer.fixture([{200, [], @objects}])
      {output, 0} = run(context, ["--endpoint", endpoint, "--json", command, "sample/docs/"])
      result = JSON.decode!(output)
      objects = result["contents"] || result["objects"]
      assert [%{"key" => "docs/file.txt", "size" => 3, "etag" => "abc"}] = objects

      if command == "tree" do
        assert result["bucket"] == "sample"
        assert result["prefix"] == "docs/"

        assert result["tree"] == [
                 "s3://sample/docs/",
                 "└── file.txt",
                 "",
                 "0 directories, 1 file"
               ]
      else
        assert result["is_truncated"] == false
        assert result["common_prefixes"] == []
      end
    end

    {endpoint, _server} = WireServer.fixture([{200, [], ~s({"status":"ok","nodes":1})}])
    {output, 0} = run(context, ["--endpoint", endpoint, "--json", "info"])
    assert JSON.decode!(output) == %{"status" => "ok", "nodes" => 1}
  end

  test "native version selection and complete anonymous JSON use public executable dispatch",
       context do
    signed_fixture(context)
    version = "selected+ %"

    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "selected"}, {"Content-Length", "3"}, {"x-amz-version-id", version}], ""},
        {200, [{"ETag", "selected"}, {"x-amz-version-id", version}], "raw"}
      ])

    path = Path.join(context.tmp_dir, "selected-version")

    {_output, 0} =
      run(context, [
        "--endpoint",
        endpoint,
        "get",
        "s3://bucket/key",
        path,
        "--version-id",
        version
      ])

    assert File.read!(path) == "raw"

    for method <- ["HEAD", "GET"] do
      assert_receive {:wire_request,
                      %{method: ^method, target: "/bucket/key?versionId=selected%2B%20%25"}}
    end

    policy = %{
      "Version" => "2012-10-17",
      "Statement" => [],
      "FutureField" => %{"retained" => true}
    }

    {endpoint, _} = WireServer.fixture([{200, [], JSON.encode!(policy)}])

    {output, 0} =
      run(context, [
        "--endpoint",
        endpoint,
        "--service",
        "s3",
        "anonymous",
        "get-json",
        "s3://bucket"
      ])

    assert JSON.decode!(output) == policy
    assert_receive {:wire_request, %{method: "GET", target: "/bucket?policy="}}

    input = Path.join(context.tmp_dir, "policy.json")
    File.write!(input, JSON.encode!(policy))

    {output, 0} =
      run(context, [
        "--endpoint",
        endpoint,
        "--service",
        "s3",
        "--json",
        "anonymous",
        "set-json",
        input,
        "s3://bucket",
        "--replace",
        "--dry-run"
      ])

    assert %{
             "DryRun" => true,
             "Input" => %{"Bucket" => "bucket"},
             "Operation" => "PutBucketPolicy"
           } = JSON.decode!(output)

    refute_receive {:wire_request, _}
  end

  test "upload download server copy and move preserve legacy JSON", context do
    signed_fixture(context)
    file = Path.join(context.tmp_dir, "input.txt")
    File.write!(file, "abc")
    {endpoint, _server} = WireServer.fixture([{200, [{"ETag", "\"abc\""}], ""}])

    {output, 0} =
      run(context, ["--endpoint", endpoint, "--json", "cp", file, "s3://sample/docs/"])

    assert JSON.decode!(output) == %{
             "status" => "ok",
             "operation" => "upload",
             "source" => file,
             "destination" => "s3://sample/docs/",
             "size" => 3,
             "etag" => "abc"
           }

    assert_receive {:wire_request,
                    %{method: "PUT", target: "/sample/docs/input.txt", body: "abc"}}

    download = Path.join(context.tmp_dir, "download.txt")

    source_head =
      {200, [{"Content-Length", "3"}, {"ETag", "\"abc\""}, {"x-amz-version-id", "source-v1"}], ""}

    {endpoint, _server} =
      WireServer.fixture([
        source_head,
        {200, [{"ETag", "\"abc\""}, {"x-amz-version-id", "source-v1"}], "abc"}
      ])

    {output, 0} =
      run(context, ["--endpoint", endpoint, "--json", "cp", "s3://sample/docs/file.txt", download])

    assert JSON.decode!(output) == %{
             "status" => "ok",
             "operation" => "download",
             "source" => "s3://sample/docs/file.txt",
             "destination" => download,
             "size" => 3
           }

    assert File.read!(download) == "abc"
    assert_receive {:wire_request, %{method: "HEAD", target: "/sample/docs/file.txt"}}

    assert_receive {:wire_request,
                    %{
                      method: "GET",
                      target: "/sample/docs/file.txt?versionId=source-v1",
                      headers: download_headers
                    }}

    assert download_headers["if-match"] == "\"abc\""

    for {command, operation} <- [{"cp", "copy"}, {"mv", "move"}] do
      responses =
        [
          source_head,
          source_head,
          {200, [{"x-amz-version-id", "destination-v1"}],
           "<CopyObjectResult><ETag>&quot;copied&quot;</ETag></CopyObjectResult>"},
          {200,
           [
             {"Content-Length", "3"},
             {"ETag", "\"copied\""},
             {"x-amz-version-id", "destination-v1"}
           ], ""},
          source_head
        ] ++ if(command == "mv", do: [{204, [], ""}], else: [])

      {endpoint, _server} = WireServer.fixture(responses)

      {output, 0} =
        run(context, [
          "--endpoint",
          endpoint,
          "--json",
          command,
          "s3://sample/docs/file.txt",
          "s3://other/folder/"
        ])

      assert JSON.decode!(output) == %{
               "status" => "ok",
               "operation" => operation,
               "source" => "s3://sample/docs/file.txt",
               "destination" => "s3://other/folder/file.txt"
             }

      assert_receive {:wire_request, %{method: "HEAD", target: "/sample/docs/file.txt"}}

      assert_receive {:wire_request,
                      %{
                        method: "HEAD",
                        target: "/sample/docs/file.txt?versionId=source-v1",
                        headers: selected_headers
                      }}

      assert selected_headers["if-match"] == "\"abc\""

      assert_receive {:wire_request,
                      %{method: "PUT", target: "/other/folder/file.txt", headers: copy_headers}}

      assert copy_headers["x-amz-copy-source"] == "/sample/docs/file.txt?versionId=source-v1"
      assert copy_headers["x-amz-copy-source-if-match"] == "\"abc\""

      assert_receive {:wire_request,
                      %{method: "HEAD", target: "/other/folder/file.txt?versionId=destination-v1"}}

      assert_receive {:wire_request,
                      %{method: "HEAD", target: "/sample/docs/file.txt?versionId=source-v1"}}

      if command == "mv",
        do:
          assert_receive(
            {:wire_request,
             %{method: "DELETE", target: "/sample/docs/file.txt?versionId=source-v1"}}
          )
    end
  end

  test "named profile selection and explicit overrides preserve presign JSON", context do
    write_config(context, """
    [default]
    endpoint = "http://unused.example"
    [profiles.fixture]
    endpoint = "http://fixture.example:9000"
    access_key_id = "fixture-access"
    secret_access_key = "fixture-secret"
    region = "eu-west-1"
    """)

    {output, 0} =
      run(context, [
        "--profile",
        "fixture",
        "--json",
        "presign",
        "s3://sample/file",
        "--expires",
        "60",
        "--method",
        "HEAD"
      ])

    assert %{
             "method" => "HEAD",
             "expires" => 60,
             "bucket" => "sample",
             "key" => "file",
             "url" => url
           } = JSON.decode!(output)

    assert URI.parse(url).host == "fixture.example"
    assert URI.decode_query(URI.parse(url).query)["X-Amz-Credential"] =~ "fixture-access/"

    assert URI.decode_query(URI.parse(url).query)["X-Amz-Credential"] =~
             "/eu-west-1/s3/aws4_request"

    refute output =~ "fixture-secret"

    {output, 0} =
      run(context, [
        "--profile",
        "fixture",
        "--endpoint",
        "http://override.example",
        "--json",
        "presign",
        "s3://sample/file"
      ])

    assert URI.parse(JSON.decode!(output)["url"]).host == "override.example"
  end

  test "configure selects its command profile and does not print entered secret", context do
    {output, 0} =
      run(
        context,
        ["configure", "--profile", "fixture"],
        "http://fixture.example\nfixture-access\nfixture-secret\neu-west-1\n"
      )

    assert output =~ "Profile 'fixture' saved"
    refute output =~ "fixture-secret"
    {:ok, config} = Toml.decode(File.read!(Path.join(context.tmp_dir, ".config/ess/config.toml")))
    assert config["profiles"]["fixture"]["secret_access_key"] == "fixture-secret"
  end

  test "argument and provider failures exit nonzero and keep secrets out of errors", context do
    for args <- [
          ["unknown-command"],
          ["mb"],
          ["rm", "s3://sample"],
          ["cp", "local", "other"],
          ["presign", "s3://sample/file"]
        ] do
      {output, 1} = run(context, args)
      assert output =~ "Error:"
      refute output =~ "fixture-secret"
    end

    {endpoint, _server} =
      WireServer.fixture([
        {403, [], "<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>"}
      ])

    {output, 1} =
      run(context, [
        "--endpoint",
        endpoint,
        "--access-key",
        "fixture-access",
        "--secret-key",
        "fixture-secret",
        "ls"
      ])

    assert output =~ "Denied"
    refute output =~ "fixture-secret"
  end

  defp write_config(context, content) do
    path = Path.join(context.tmp_dir, ".config/ess/config.toml")
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
  end

  defp signed_fixture(context) do
    write_config(
      context,
      "[default]\naccess_key_id = \"fixture-access\"\nsecret_access_key = \"fixture-secret\"\n"
    )
  end

  test "configuration directory overrides are isolated and preserve precedence", context do
    xdg = Path.join(context.tmp_dir, "xdg")
    explicit = Path.join(context.tmp_dir, "explicit")
    input = "http://fixture.example\nfixture-access\nfixture-secret\nus-east-1\n"

    xdg_context =
      Map.put(context, :environment, %{~c"XDG_CONFIG_HOME" => String.to_charlist(xdg)})

    {_output, 0} = run(xdg_context, ["configure"], input)
    assert File.exists?(Path.join(xdg, "ess/config.toml"))

    explicit_context =
      Map.put(context, :environment, %{
        ~c"XDG_CONFIG_HOME" => String.to_charlist(xdg),
        ~c"ESS_CONFIG_DIR" => String.to_charlist(explicit)
      })

    {_output, 0} = run(explicit_context, ["configure", "--profile", "override"], input)
    {:ok, config} = Toml.decode(File.read!(Path.join(explicit, "config.toml")))
    assert config["profiles"]["override"]["access_key_id"] == "fixture-access"
    refute File.exists?(Path.join(context.tmp_dir, ".config/ess/config.toml"))
  end

  defp run(context, args, input \\ nil) do
    environment = %{
      ~c"HOME" => String.to_charlist(context.tmp_dir),
      ~c"ERL_FLAGS" => ~c"+S 2:2",
      ~c"ESS_CONFIG_DIR" => false,
      ~c"XDG_CONFIG_HOME" => false
    }

    port =
      Port.open(
        {:spawn_executable, String.to_charlist(System.find_executable("escript"))},
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: [context.executable | args],
          env: Map.to_list(Map.merge(environment, Map.get(context, :environment, %{})))
        ]
      )

    if input, do: Port.command(port, input)
    collect(port, "")
  end

  defp collect(port, output) do
    receive do
      {^port, {:data, data}} -> collect(port, output <> data)
      {^port, {:exit_status, status}} -> {strip_ansi(output), status}
    after
      15_000 ->
        Port.close(port)
        flunk("CLI subprocess exceeded 15 second deadline")
    end
  end

  defp strip_ansi(output), do: Regex.replace(~r/\e\[[0-9;]*m/, output, "")
end
