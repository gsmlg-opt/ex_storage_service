Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutableWriteResponseMatrixTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @fixture Path.expand("../fixtures/differential/write-response-wire.json", __DIR__)
  @oracle JSON.decode!(File.read!(@fixture))

  setup_all do
    root = Path.expand("../..", __DIR__)

    directory =
      Path.join(System.tmp_dir!(), "ess-object-lambda-#{System.unique_integer([:positive])}")

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

  test "pinned independent executable captured all three Object Lambda exchanges" do
    assert digest(File.read!(@fixture)) ==
             "a765219d04897f05917d19558062a213f5d208b83800872ad27bea3b5f0d964f"

    assert @oracle["aws_sha256"] ==
             "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"

    model = File.read!(Path.expand("../../priv/compatibility/aws/service-2.json", __DIR__))
    assert digest(model) == @oracle["model_sha256"]

    assert Enum.sort(Enum.map(@oracle["cases"], & &1["scenario"])) ==
             ~w(binary-success forwarded-error-success service-access-denied)

    for golden <- @oracle["cases"] do
      assert golden["operation"] == "WriteGetObjectResponse"
      assert golden["aws"]["peer_errors"] == []
      assert [request] = golden["aws"]["requests"]
      assert URI.parse(request["target"]).host == "fixture.loopback.invalid"
      assert_signature(request["method"], URI.parse(request["target"]).path, request["headers"])
    end
  end

  for golden <- @oracle["cases"] do
    @golden golden
    test "pinned AWS Object Lambda #{@golden["scenario"]}", context do
      golden = @golden
      response = golden["response"]

      {endpoint, _server} =
        WireServer.fixture([
          {response["status"], Map.to_list(response["headers"]),
           Base.decode64!(response["body_base64"])}
        ])

      endpoint = String.replace(endpoint, "127.0.0.1", "loopback.invalid")

      File.write!(
        Path.join(context.tmp_dir, "body.bin"),
        :binary.list_to_bin(Enum.to_list(0..255))
      )

      inetrc = Path.join(context.tmp_dir, "inetrc")

      File.write!(
        inetrc,
        "{host,{127,0,0,1},[\"fixture.loopback.invalid\"]}.\n{lookup,[file]}.\n"
      )

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

      {stdout, stderr, status} = run(context, args, inetrc)
      oracle = golden["aws"]
      assert status == oracle["status"], stderr
      assert String.trim(stderr) == String.trim(oracle["stderr"])
      assert stdout == oracle["stdout"]
      assert [expected] = oracle["requests"]
      assert_receive {:wire_request, request}
      assert request.method == expected["method"]
      assert request.target == URI.parse(expected["target"]).path
      assert request.body == Base.decode64!(expected["body_base64"])
      assert request.headers["host"] == "fixture." <> URI.parse(endpoint).authority

      for {key, value} <- expected["headers"],
          key not in ~w(authorization host x-amz-date expect) do
        assert request.headers[key] == value, "request header #{key}"
      end

      assert String.to_integer(request.headers["content-length"]) == byte_size(request.body)
      assert request.headers["x-amz-content-sha256"] == digest(request.body)
      assert_signature(request.method, request.target, request.headers)
      refute_receive {:wire_request, _}, 5
      refute_receive {:wire_error, _, _, _}, 5
    end
  end

  # Recompute HMAC locally from captured wire headers. No ESS signing code is
  # used, and the host prefix must appear in both the wire host and signed data.
  defp assert_signature(method, path, headers) do
    pattern =
      ~r/\AAWS4-HMAC-SHA256 Credential=fixture-access\/([^,]+), SignedHeaders=([^,]+), Signature=([0-9a-f]+)\z/

    assert [_, scope, names, signature] = Regex.run(pattern, headers["authorization"])
    assert [date, "us-east-1", "s3-object-lambda", "aws4_request"] = String.split(scope, "/")
    signed = String.split(names, ";")
    assert "host" in signed
    assert "x-amz-request-route" in signed
    assert "x-amz-request-token" in signed

    canonical_headers =
      Enum.map_join(signed, "", fn name ->
        name <> ":" <> (headers[name] |> String.split() |> Enum.join(" ")) <> "\n"
      end)

    canonical =
      Enum.join(
        [method, path, "", canonical_headers, names, headers["x-amz-content-sha256"]],
        "\n"
      )

    string =
      Enum.join(["AWS4-HMAC-SHA256", headers["x-amz-date"], scope, digest(canonical)], "\n")

    key =
      Enum.reduce(
        [date, "us-east-1", "s3-object-lambda", "aws4_request"],
        "AWS4fixture-secret",
        fn part, key -> :crypto.mac(:hmac, :sha256, key, part) end
      )

    assert Base.encode16(:crypto.mac(:hmac, :sha256, key, string), case: :lower) == signature
  end

  defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

  defp run(context, args, inetrc) do
    stderr = Path.join(context.tmp_dir, "stderr")

    cleared =
      System.get_env()
      |> Enum.filter(fn {key, _} ->
        String.starts_with?(key, ["AWS_", "ESS_", "MC_"]) or
          String.contains?(String.downcase(key), "proxy")
      end)
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
        "ERL_INETRC" => inetrc,
        "PAGER" => "cat"
      })
      |> Map.to_list()

    {output, status} =
      System.cmd(
        "sh",
        [
          "-c",
          "ess_stderr=$1; shift; exec \"$@\" 2>\"$ess_stderr\"",
          "ess-object-lambda",
          stderr,
          System.find_executable("escript"),
          context.executable | args
        ],
        env: env
      )

    {output, File.read!(stderr), status}
  end
end
