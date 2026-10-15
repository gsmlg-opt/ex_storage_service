Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutableWaiterMatrixTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @fixture Path.expand("../fixtures/differential/waiter-wire.json", __DIR__)
  @oracle JSON.decode!(File.read!(@fixture))

  setup_all do
    root = Path.expand("../..", __DIR__)

    directory =
      Path.join(System.tmp_dir!(), "ess-waiters-#{System.unique_integer([:positive])}")

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

  test "independent capture covers all four pinned waiters, retry and terminal behavior" do
    assert digest(File.read!(@fixture)) ==
             "a038af887ef810e3feb6e6d213940ad6facba438fd171f7d6ab180a3b7c26a5b"

    assert @oracle["aws_sha256"] ==
             "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"

    model = File.read!(Path.expand("../../priv/compatibility/aws/waiters-2.json", __DIR__))
    assert digest(model) == @oracle["waiters_sha256"]
    modeled = model |> JSON.decode!() |> Map.fetch!("waiters") |> Map.keys() |> Enum.sort()
    captured = Enum.group_by(@oracle["cases"], & &1["waiter"])
    assert length(modeled) == 4
    assert Enum.sort(Map.keys(captured)) == modeled
    assert length(@oracle["cases"]) == 14

    for {_waiter, cases} <- captured do
      scenarios = Enum.map(cases, & &1["scenario"])
      assert "retry-then-success" in scenarios
      assert "terminal-error" in scenarios
      assert Enum.any?(scenarios, &String.starts_with?(&1, "success-"))
      assert Enum.all?(cases, &(&1["aws"]["peer_errors"] == []))
    end
  end

  for golden <- @oracle["cases"] do
    @golden golden
    test "pinned AWS waiter #{@golden["waiter"]} #{@golden["scenario"]}", context do
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
      assert stdout == oracle["stdout"]
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
          "ess-waiters",
          stderr,
          System.find_executable("escript"),
          context.executable | args
        ],
        env: env
      )

    {output, File.read!(stderr), status}
  end
end
