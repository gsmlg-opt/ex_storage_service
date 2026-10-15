Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutableDifferentialTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @fixture Path.expand("../fixtures/differential/reference-wire.json", __DIR__)

  setup_all do
    directory =
      Path.join(System.tmp_dir!(), "ess-differential-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    root = Path.expand("../..", __DIR__)

    {output, status} =
      System.cmd("mix", ["escript.build", "--no-compile"],
        cd: root,
        env: [{"MIX_ENV", "test"}, {"PAGER", "cat"}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    executable = Path.join(directory, "ess")
    File.cp!(Path.join(root, "ess"), executable)
    on_exit(fn -> File.rm_rf!(directory) end)

    cases =
      @fixture
      |> File.read!()
      |> JSON.decode!()
      |> Map.fetch!("cases")
      |> Map.new(&{&1["name"], &1})

    %{executable: executable, cases: cases}
  end

  test "complex notification output matches the pinned AWS executable", context do
    golden = context.cases["notification-get"]
    {endpoint, _} = peer(golden)
    assert {output, "", 0} = run(context, aws(endpoint, golden["args"]))
    assert JSON.decode!(output) == JSON.decode!(golden["results"]["aws"]["stdout"])
    assert_receive {:wire_request, %{method: "GET", target: "/bucket?notification="}}
  end

  test "multi-delete matches literal AWS XML, checksum and partial-error JSON", context do
    golden = context.cases["delete-objects"]
    {endpoint, _} = peer(golden)
    assert {output, "", 0} = run(context, aws(endpoint, golden["args"]))
    assert JSON.decode!(output) == JSON.decode!(golden["results"]["aws"]["stdout"])

    assert_receive {:wire_request,
                    %{method: "POST", target: "/bucket?delete=", body: body, headers: headers}}

    reference = hd(golden["results"]["aws"]["requests"])
    assert body == reference["body"]

    for name <-
          ~w(content-length x-amz-content-sha256 x-amz-checksum-crc32c x-amz-sdk-checksum-algorithm x-amz-bypass-governance-retention x-amz-request-payer) do
      assert headers[name] == reference["headers"][name]
    end
  end

  test "raw encoded upload matches the pinned AWS checksum and metadata headers", context do
    golden = context.cases["put-checksum"]
    File.write!(Path.join(context.tmp_dir, "source.gz"), "abc")
    args = Enum.map(golden["args"], &String.replace(&1, "@HOME@", context.tmp_dir))
    {endpoint, _} = peer(golden)
    assert {output, "", 0} = run(context, aws(endpoint, args))
    assert JSON.decode!(output) == JSON.decode!(golden["results"]["aws"]["stdout"])

    assert_receive {:wire_request,
                    %{method: "PUT", target: target, body: "abc", headers: headers}}

    reference = hd(golden["results"]["aws"]["requests"])
    assert target == reference["target"]

    for name <-
          ~w(content-length content-encoding x-amz-content-sha256 x-amz-checksum-sha256 x-amz-sdk-checksum-algorithm x-amz-meta-owner x-amz-meta-note x-amz-server-side-encryption) do
      assert headers[name] == reference["headers"][name]
    end
  end

  test "AWS sync filters and dry-run delete produce the same selected actions", context do
    golden = context.cases["sync-filter-delete-dryrun"]
    local = Path.join(context.tmp_dir, "local")
    File.mkdir_p!(Path.join(local, "secret"))
    File.write!(Path.join(local, "a.txt"), "abc")
    File.write!(Path.join(local, "ignored.bin"), "ignore")
    File.write!(Path.join(local, "secret/private.txt"), "private")
    args = Enum.map(golden["args"], &String.replace(&1, "@HOME@", context.tmp_dir))
    {endpoint, _} = peer(golden)
    assert {output, "", 0} = run(context, aws(endpoint, args))
    relative = Path.relative_to(context.tmp_dir, File.cwd!(), force: true)
    expected = String.replace(golden["results"]["aws"]["stdout"], "@RELATIVE_HOME@", relative)
    assert output == expected
    assert_receive {:wire_request, %{method: "GET", body: ""}}
    refute_receive {:wire_request, _}, 50
    assert File.read!(Path.join(local, "ignored.bin")) == "ignore"
    assert File.read!(Path.join(local, "secret/private.txt")) == "private"
  end

  test "a high-level denied download has the pinned transfer status and plain stderr", context do
    golden = context.cases["cp-access-denied"]
    args = Enum.map(golden["args"], &String.replace(&1, "@HOME@", context.tmp_dir))
    {endpoint, _} = peer(golden)
    assert {"", error, 1} = run(context, aws(endpoint, args))
    assert error == golden["results"]["aws"]["stderr"]
    refute error =~ "\e["
    refute File.exists?(Path.join(context.tmp_dir, "denied"))
  end

  test "native inspection agrees with pinned mc object selection and byte totals", context do
    golden = context.cases["mc-list"]
    {endpoint, _} = peer(golden)
    assert {output, "", 0} = run(context, native(endpoint, ["ls", "s3://bucket/p/"]))
    entries = JSON.decode!(output)["Entries"]

    mc =
      golden["results"]["mc"]["stdout"]
      |> String.split("\n", trim: true)
      |> Enum.map(&JSON.decode!/1)

    assert Enum.map(entries, &{Path.basename(&1["Key"]), &1["Size"]}) ==
             Enum.map(mc, &{&1["key"], &1["size"]})

    golden = context.cases["mc-du"]
    {endpoint, _} = peer(golden)
    assert {output, "", 0} = run(context, native(endpoint, ["du", "s3://bucket/p/"]))
    result = JSON.decode!(output)
    mc = JSON.decode!(golden["results"]["mc"]["stdout"])
    assert result["Objects"] == mc["objects"]
    assert result["Bytes"] == mc["size"]
    assert result["Partial"] == false
  end

  test "native find supports the pinned mc name filter", context do
    golden = context.cases["mc-find"]
    {endpoint, _} = peer(golden)

    assert {output, "", 0} =
             run(context, native(endpoint, ["find", "s3://bucket/p/", "--name", "*.txt"]))

    expected =
      golden["results"]["mc"]["stdout"] |> JSON.decode!() |> Map.fetch!("key") |> Path.basename()

    assert JSON.decode!(output)["Entries"] == [expected]
  end

  defp peer(golden) do
    [status, headers, body] = golden["response"]
    WireServer.fixture([{status, Map.to_list(headers), body}])
  end

  defp aws(endpoint, args),
    do: ["--endpoint-url", endpoint, "--region", "us-east-1", "--no-cli-pager" | args]

  defp native(endpoint, args),
    do: [
      "--endpoint",
      endpoint,
      "--access-key",
      "fixture-access",
      "--secret-key",
      "fixture-secret",
      "--output",
      "json" | args
    ]

  defp run(context, args) do
    stderr = Path.join(context.tmp_dir, "stderr")

    env = [
      {"AWS_CONFIG_FILE", "/dev/null"},
      {"AWS_SHARED_CREDENTIALS_FILE", "/dev/null"},
      {"AWS_ACCESS_KEY_ID", "fixture-access"},
      {"AWS_SECRET_ACCESS_KEY", "fixture-secret"},
      {"AWS_EC2_METADATA_DISABLED", "true"},
      {"AWS_MAX_ATTEMPTS", "1"},
      {"AWS_CLI_AUTO_PROMPT", "off"},
      {"AWS_PAGER", ""},
      {"ERL_FLAGS", "+S 2:2"},
      {"ESS_CONFIG_DIR", context.tmp_dir},
      {"ESS_STATE_DIR", Path.join(context.tmp_dir, "state")}
    ]

    {output, status} =
      System.cmd(
        "sh",
        [
          "-c",
          "ess_stderr=$1; shift; exec \"$@\" 2>\"$ess_stderr\"",
          "ess-test",
          stderr,
          System.find_executable("escript"),
          context.executable | args
        ],
        env: env
      )

    error = File.read!(stderr)
    refute output =~ "fixture-secret"
    refute error =~ "fixture-secret"
    {output, error, status}
  end
end
