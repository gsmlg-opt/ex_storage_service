Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutablePaginatorMatrixTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @fixture Path.expand("../fixtures/differential/paginator-wire.json", __DIR__)
  @oracle JSON.decode!(File.read!(@fixture))

  setup_all do
    root = Path.expand("../..", __DIR__)

    directory =
      Path.join(System.tmp_dir!(), "ess-paginators-#{System.unique_integer([:positive])}")

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

  test "independent capture covers all seven pinned paginators and six scenarios" do
    assert digest(File.read!(@fixture)) ==
             "43cc6abc45c0669b852aed35eb37d720b17f9569b6bd85c31f36b574eb7d6dc8"

    assert @oracle["aws_sha256"] ==
             "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"

    for {filename, field} <- [
          {"service-2.json", "model_sha256"},
          {"paginators-1.json", "paginators_sha256"}
        ] do
      source = File.read!(Path.expand("../../priv/compatibility/aws/#{filename}", __DIR__))
      assert digest(source) == @oracle[field]
    end

    paginators =
      Path.expand("../../priv/compatibility/aws/paginators-1.json", __DIR__)
      |> File.read!()
      |> JSON.decode!()
      |> Map.fetch!("pagination")
      |> Map.keys()
      |> Enum.sort()

    captured = Enum.group_by(@oracle["cases"], & &1["operation"])
    assert length(paginators) == 7
    assert Enum.sort(Map.keys(captured)) == paginators
    assert length(@oracle["cases"]) == 42

    for {_operation, cases} <- captured do
      assert Enum.sort(Enum.map(cases, & &1["scenario"])) ==
               ~w(aggregate no-paginate resume-boundary resume-within truncate-boundary truncate-within)

      for kind <- ~w(within boundary) do
        truncated = Enum.find(cases, &(&1["scenario"] == "truncate-#{kind}"))
        resumed = Enum.find(cases, &(&1["scenario"] == "resume-#{kind}"))
        token = JSON.decode!(truncated["aws"]["stdout"])["NextToken"]
        assert ["--starting-token", ^token] = Enum.take(resumed["args"], -2)
      end
    end
  end

  for golden <- @oracle["cases"] do
    @golden golden
    test "pinned AWS paginator #{@golden["operation"]} #{@golden["scenario"]}", context do
      golden = @golden

      responses =
        Enum.map(golden["responses"], fn response ->
          {response["status"], Map.to_list(response["headers"]),
           Base.decode64!(response["body_base64"])}
        end)

      {endpoint, _server} = WireServer.fixture(responses)

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
          "2"
        ] ++ golden["args"]

      {stdout, stderr, status} = run(context, args)
      oracle = golden["aws"]
      assert status == oracle["status"], stderr
      assert String.trim(stderr) == String.trim(oracle["stderr"])
      assert normalized_output(stdout) == normalized_output(oracle["stdout"])
      assert length(responses) == length(oracle["requests"])

      for expected <- oracle["requests"] do
        assert_receive {:wire_request, request}
        assert request.method == expected["method"]
        assert target(request.target) == target(expected["target"])
        assert request.body == Base.decode64!(expected["body_base64"])

        for {key, value} <- expected["headers"] do
          assert request.headers[key] == value, "request header #{key}"
        end
      end

      refute_receive {:wire_request, _}, 5
      refute_receive {:wire_error, _, _, _}, 5
    end
  end

  # AWS and ESS may serialize the opaque token's JSON with different whitespace
  # or map order. Compare its actual decoded fields; resume uses the literal AWS
  # token above, so token interoperability is independently exercised as well.
  defp normalized_output(output) do
    decoded = JSON.decode!(output)

    if Map.has_key?(decoded, "NextToken") do
      Map.update!(decoded, "NextToken", fn token ->
        token |> Base.decode64!() |> JSON.decode!()
      end)
    else
      decoded
    end
  end

  defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

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
          "ess-paginators",
          stderr,
          System.find_executable("escript"),
          context.executable | args
        ],
        env: env
      )

    {output, File.read!(stderr), status}
  end
end
