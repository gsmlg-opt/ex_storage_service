Code.require_file("support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.AwsOutputPaginationDifferentialTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @fixture Path.expand("fixtures/aws_output_pagination/reference.json", __DIR__)
  @oracle JSON.decode!(File.read!(@fixture))

  setup_all do
    root = Path.expand("..", __DIR__)

    directory =
      Path.join(System.tmp_dir!(), "ess-output-pagination-#{System.unique_integer([:positive])}")

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

  test "pinned executable captures both output formats, queries, limits and real resume token" do
    assert digest(File.read!(@fixture)) ==
             "5cd3d386576416d8f88cffde82a0ab1c3347a0384761204bafe1d2f332a1dc8f"

    assert @oracle["aws_cli_version"] == "2.31.18"

    assert @oracle["aws_sha256"] ==
             "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"

    model = File.read!(Path.expand("../priv/compatibility/aws/service-2.json", __DIR__))
    assert digest(model) == @oracle["model_sha256"]

    assert Enum.sort(Enum.uniq(Enum.map(@oracle["cases"], & &1["output"]))) ==
             ["text", "yaml-stream"]

    for output <- ["text", "yaml-stream"] do
      cases = Enum.filter(@oracle["cases"], &(&1["output"] == output))

      assert Enum.sort(Enum.map(cases, & &1["scenario"])) ==
               ~w(aggregate full-pages full-truncate resume scalar-query token-query truncate)

      resumed = Enum.find(cases, &(&1["scenario"] == "resume"))
      assert ["--starting-token", @oracle["resume_token"]] == Enum.take(resumed["args"], -2)
    end

    assert JSON.decode!(@oracle["token_control"]["aws"]["stdout"])["NextToken"] ==
             @oracle["resume_token"]
  end

  for golden <- @oracle["cases"] do
    @golden golden
    test "pinned AWS #{@golden["output"]} pagination #{@golden["scenario"]}", context do
      golden = @golden

      responses =
        Enum.map(golden["responses"], fn response ->
          {response["status"], Map.to_list(response["headers"]),
           Base.decode64!(response["body_base64"])}
        end)

      {endpoint, server} = WireServer.fixture(responses)
      server_monitor = Process.monitor(server)

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
      assert stderr == oracle["stderr"]
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

      assert_receive {:DOWN, ^server_monitor, :process, ^server, :normal}
      assert normalized_output(stdout) == normalized_output(oracle["stdout"])
      refute_receive {:wire_request, _}, 5
      refute_receive {:wire_error, _, _, _}, 5
    end
  end

  # AWS resume tokens are opaque JSON envelopes; only their encoding whitespace
  # and object member order may differ. All surrounding output remains exact.
  defp normalized_output(output) do
    tokens =
      ~r/[A-Za-z0-9+\/]+={0,2}/
      |> Regex.scan(output)
      |> Enum.flat_map(fn [encoded] ->
        case resume_envelope(encoded) do
          {:ok, envelope} -> [{encoded, envelope}]
          :error -> []
        end
      end)

    bytes =
      Enum.reduce(tokens, output, fn {encoded, _}, bytes ->
        String.replace(bytes, encoded, "<aws-resume-token>")
      end)

    {bytes, Enum.map(tokens, &elem(&1, 1))}
  end

  defp resume_envelope(encoded) do
    with {:ok, json} <- Base.decode64(encoded),
         {:ok, %{} = envelope} <- JSON.decode(json),
         true <- Map.has_key?(envelope, "ContinuationToken"),
         true <-
           Enum.all?(Map.keys(envelope), &(&1 in ["ContinuationToken", "boto_truncate_amount"])) do
      {:ok, envelope}
    else
      _ -> :error
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
