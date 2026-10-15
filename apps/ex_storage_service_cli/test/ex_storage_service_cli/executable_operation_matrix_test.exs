Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutableOperationMatrixTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.S3.XML
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @fixture Path.expand("../fixtures/differential/operation-wire.json", __DIR__)
  @oracle JSON.decode!(File.read!(@fixture))

  setup_all do
    root = Path.expand("../..", __DIR__)

    directory =
      Path.join(System.tmp_dir!(), "ess-operations-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)

    {output, status} =
      System.cmd("mix", ["escript.build", "--no-compile"],
        cd: root,
        env: [{"MIX_ENV", "test"}, {"PAGER", "cat"}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    executable = Path.join(directory, "ess")
    File.cp!(Path.join(root, "ess"), executable)
    %{executable: executable}
  end

  test "independent capture accounts for all 108 operations and names its exclusion" do
    assert Base.encode16(:crypto.hash(:sha256, File.read!(@fixture)), case: :lower) ==
             "6593519c84aa5e4847aa7d83dc1fd5598d79f511f9a8e252f22c193f967708d4"

    assert @oracle["aws_sha256"] ==
             "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"

    captured = @oracle["cases"] |> Enum.map(& &1["operation"]) |> Enum.uniq()

    assert [%{"operation" => "WriteGetObjectResponse", "reason" => reason}] =
             @oracle["exclusions"]

    assert reason =~ "controlled DNS/TLS"
    assert length(captured) == 107

    model =
      Path.expand("../../priv/compatibility/aws/service-2.json", __DIR__)
      |> File.read!()

    assert Base.encode16(:crypto.hash(:sha256, model), case: :lower) == @oracle["model_sha256"]
    modeled = model |> JSON.decode!() |> Map.fetch!("operations") |> Map.keys() |> Enum.sort()
    assert Enum.sort(captured ++ ["WriteGetObjectResponse"]) == modeled
  end

  for golden <- @oracle["cases"] do
    @golden golden
    test "pinned AWS #{@golden["operation"]} #{@golden["scenario"]}", context do
      golden = @golden
      response = golden["response"]
      body = Base.decode64!(response["body_base64"])

      {endpoint, _server} =
        WireServer.fixture([{response["status"], Map.to_list(response["headers"]), body}])

      File.write!(Path.join(context.tmp_dir, "body.bin"), <<"fixture", 0, 255>>)

      args =
        [
          "--endpoint-url",
          endpoint,
          "--region",
          "us-east-1",
          "--no-cli-pager",
          "--cli-connect-timeout",
          "1",
          "--cli-read-timeout",
          "2",
          "--no-paginate"
        ] ++
          Enum.map(golden["args"], &String.replace(&1, "@TMP@", context.tmp_dir))

      {stdout, stderr, status} = run(context, args)
      oracle = golden["aws"]
      assert status == oracle["status"], stderr
      assert String.trim(stderr) == String.trim(oracle["stderr"])
      assert decode_output(stdout) == decode_output(oracle["stdout"])
      outfile = Path.join(context.tmp_dir, "result.bin")

      if expected = oracle["outfile_base64"] do
        assert File.read!(outfile) == Base.decode64!(expected)
      else
        refute File.exists?(outfile)
      end

      assert [expected] = oracle["requests"]
      assert_receive {:wire_request, request}
      assert request.method == expected["method"]
      assert target(request.target) == target(expected["target"])
      assert_body(request.body, expected)
      assert_headers(request.headers, expected["headers"], request.body)
      refute_receive {:wire_request, _}, 5
    end
  end

  defp decode_output(""), do: nil
  defp decode_output(output), do: JSON.decode!(output)

  defp target(value) do
    uri = URI.parse(value)

    query =
      (uri.query || "")
      |> String.split("&", trim: true)
      |> Enum.map(fn item ->
        [key | values] = String.split(item, "=", parts: 2)
        {URI.decode_www_form(key), URI.decode_www_form(List.first(values) || "")}
      end)
      |> Enum.sort()

    {uri.path, query}
  end

  defp assert_body(body, %{"xml_semantics" => expected}) do
    assert {:ok, node} = XML.parse(body)
    assert xml_semantics(node) == expected
  end

  defp assert_body(body, expected), do: assert(body == Base.decode64!(expected["body_base64"]))

  defp xml_semantics(%XML.Node{} = node) do
    children =
      node.content
      |> Enum.filter(&match?(%XML.Node{}, &1))
      |> Enum.group_by(&XML.local_name(&1.name))
      |> Map.new(fn {key, values} -> {key, Enum.map(values, &xml_semantics/1)} end)

    attributes =
      node.attributes
      |> Enum.reject(fn {key, _} -> String.starts_with?(key, "xmlns") end)
      |> Map.new(fn {key, value} -> {XML.local_name(key), value} end)

    %{
      "name" => XML.local_name(node.name),
      "attributes" => attributes,
      "text" => node.content |> Enum.filter(&is_binary/1) |> Enum.join(),
      "children" => children
    }
  end

  defp assert_headers(headers, expected, body) do
    for {key, value} <- expected,
        key not in ~w(expect content-length x-amz-content-sha256),
        not String.starts_with?(key, "x-amz-checksum-") do
      assert headers[key] == value, "request header #{key}"
    end

    if length = headers["content-length"],
      do: assert(String.to_integer(length) == byte_size(body))

    if hash = headers["x-amz-content-sha256"] do
      assert hash == Base.encode16(:crypto.hash(:sha256, body), case: :lower)
    end

    # XML sibling order and empty-tag spelling can change entity bytes. The
    # checksum algorithm/header presence must agree; checksum vector tests cover
    # each algorithm independently, while the raw AWS values remain in fixtures.
    for {key, _value} <- expected, String.starts_with?(key, "x-amz-checksum-") do
      assert is_binary(headers[key]), "missing request checksum #{key}"
    end
  end

  defp run(context, args) do
    stderr = Path.join(context.tmp_dir, "stderr")

    cleared =
      System.get_env()
      |> Enum.filter(fn {key, _} -> String.starts_with?(key, ["AWS_", "ESS_", "MC_"]) end)
      |> Map.new(fn {key, _} -> {key, nil} end)

    env =
      Map.merge(cleared, %{
        "AWS_CONFIG_FILE" => "/dev/null",
        "AWS_SHARED_CREDENTIALS_FILE" => "/dev/null",
        "AWS_ACCESS_KEY_ID" => "fixture-access",
        "AWS_SECRET_ACCESS_KEY" => "fixture-secret",
        "AWS_EC2_METADATA_DISABLED" => "true",
        "AWS_MAX_ATTEMPTS" => "1",
        "AWS_PAGER" => "",
        "AWS_CLI_AUTO_PROMPT" => "off",
        "AWS_RESPONSE_CHECKSUM_VALIDATION" => "when_required",
        "ESS_CONFIG_DIR" => Path.join(context.tmp_dir, "config"),
        "ESS_STATE_DIR" => Path.join(context.tmp_dir, "state"),
        "ERL_FLAGS" => "+S 2:2",
        "NO_PROXY" => "*",
        "PAGER" => "cat"
      })
      |> Map.to_list()

    {output, status} =
      System.cmd(
        "sh",
        [
          "-c",
          "ess_stderr=$1; shift; exec \"$@\" 2>\"$ess_stderr\"",
          "ess-matrix",
          stderr,
          System.find_executable("escript"),
          context.executable | args
        ],
        env: env
      )

    {output, File.read!(stderr), status}
  end
end
