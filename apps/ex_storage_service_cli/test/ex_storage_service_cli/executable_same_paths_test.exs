Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutableSamePathsTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @fixture Path.expand("../fixtures/differential/same-paths-wire.json", __DIR__)
  @oracle JSON.decode!(File.read!(@fixture))

  setup_all do
    root = Path.expand("../..", __DIR__)
    directory = Path.join(System.tmp_dir!(), "ess-paths-#{System.unique_integer([:positive])}")
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

  test "independent pinned AWS capture is immutable" do
    assert digest(File.read!(@fixture)) ==
             "1ac223ee4085bb0c54a8d23585e5892967acb4cab0ee8ee27af7add067e91883"

    assert @oracle["aws_sha256"] ==
             "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"

    assert length(@oracle["cases"]) == 21
  end

  for golden <- @oracle["cases"] do
    @golden golden
    test "pinned AWS same paths #{@golden["name"]}", context do
      golden = @golden

      responses =
        Enum.map(golden["responses"], fn response ->
          {response["status"], Map.to_list(response["headers"]),
           Base.decode64!(response["body_base64"])}
        end)

      {endpoint, _} = WireServer.fixture(responses)
      port = URI.parse(endpoint).port
      config = Path.join(context.tmp_dir, "aws-config")
      credentials = Path.join(context.tmp_dir, "credentials")

      profile =
        case golden["scenario"] do
          "profile-fips" -> "use_fips_endpoint = true\n"
          "profile-dualstack" -> "use_dualstack_endpoint = true\n"
          _ -> ""
        end

      File.write!(
        config,
        "[profile selected]\nregion = us-east-1\n#{profile}services = fixture\n[services fixture]\ns3_control =\n  endpoint_url = http://control.invalid:#{port}\n"
      )

      File.write!(
        credentials,
        "[selected]\naws_access_key_id = fixture-access\naws_secret_access_key = fixture-secret\n"
      )

      inetrc = Path.join(context.tmp_dir, "inetrc")

      File.write!(
        inetrc,
        "{host,{127,0,0,1},[\"123456789012.control.invalid\",\"control.invalid\",\"sts.invalid\",\"data.invalid\"]}.\n{lookup,[file]}.\n"
      )

      args =
        [
          "--endpoint-url",
          "http://data.invalid:#{port}",
          "--region",
          "us-east-1",
          "--no-cli-pager",
          "--cli-connect-timeout",
          "1",
          "--cli-read-timeout",
          "2",
          "s3",
          "mv",
          golden["source"],
          golden["destination"]
        ] ++ golden["flags"]

      extra =
        if String.starts_with?(golden["scenario"], "profile"),
          do: %{},
          else: %{"AWS_ENDPOINT_URL_S3_CONTROL" => "http://control.invalid:#{port}"}

      extra = Map.merge(extra, golden["env"])
      {stdout, stderr, status} = run(context, args, inetrc, config, credentials, port, extra)
      assert status == golden["aws"]["status"], stderr
      assert String.trim(stderr) == String.trim(golden["aws"]["stderr"])
      assert stdout == golden["aws"]["stdout"]

      for expected <- golden["aws"]["requests"] do
        assert_receive {:wire_request, request}
        target = URI.parse(String.replace(expected["target"], "@PORT@", to_string(port)))
        assert request.method == expected["method"]

        assert request.target ==
                 target.path <> if(target.query, do: "?" <> target.query, else: "")

        assert request.body == Base.decode64!(expected["body_base64"])

        for {key, value} <- expected["headers"] do
          assert request.headers[key] == String.replace(value, "@PORT@", to_string(port)),
                 "header #{key}"
        end

        assert_signature(request, expected["signing_scope"])
      end

      refute_receive {:wire_request, _}, 5
      refute_receive {:wire_error, _, _, _}, 5
    end
  end

  for {name, bucket, responses, message} <- [
        {"MRAP repeated tokens", "arn:aws:s3::123456789012:accesspoint/example.mrap",
         List.duplicate(
           "<ListMultiRegionAccessPointsResult><AccessPoints/><NextToken>repeat</NextToken></ListMultiRegionAccessPointsResult>",
           2
         ), "pagination exceeded its bounds"},
        {"missing bucket", "arn:aws:s3:us-east-1:123456789012:accesspoint/example-ap",
         ["<GetAccessPointResult/>"], "Invalid AWS path resolution response"},
        {"oversized response", "arn:aws:s3:us-east-1:123456789012:accesspoint/example-ap",
         [String.duplicate("x", 1_048_577)], "Control response exceeds its size limit"},
        {"invalid STS account", "example-ap-s3alias",
         [
           "<GetCallerIdentityResponse><GetCallerIdentityResult><Account>bad</Account></GetCallerIdentityResult></GetCallerIdentityResponse>"
         ], "Invalid AWS path resolution response"}
      ] do
    @name name
    @bucket bucket
    @responses responses
    @message message
    test "bounded path resolver rejects #{@name}", context do
      {endpoint, _} = WireServer.fixture(Enum.map(@responses, &{200, [], &1}))
      port = URI.parse(endpoint).port
      inetrc = Path.join(context.tmp_dir, "inetrc")

      File.write!(
        inetrc,
        "{host,{127,0,0,1},[\"123456789012.control.invalid\",\"control.invalid\",\"sts.invalid\"]}.\n{lookup,[file]}.\n"
      )

      args = [
        "--region",
        "us-east-1",
        "--no-cli-pager",
        "--cli-read-timeout",
        "2",
        "s3",
        "mv",
        "s3://bucket/key",
        "s3://#{@bucket}/key",
        "--validate-same-s3-paths"
      ]

      {stdout, stderr, status} =
        run(context, args, inetrc, "/dev/null", "/dev/null", port, %{
          "AWS_ENDPOINT_URL_S3_CONTROL" => "http://control.invalid:#{port}"
        })

      assert status == 255, stderr
      assert stdout == ""
      assert stderr =~ @message
      for _ <- @responses, do: assert_receive({:wire_request, _})
      refute_receive {:wire_request, _}, 5
      refute_receive {:wire_error, _, _, _}, 5
    end
  end

  defp assert_signature(request, expected_scope) do
    assert [_, date, scope, names, signature] =
             Regex.run(
               ~r/\AAWS4-HMAC-SHA256 Credential=fixture-access\/([^\/]+)\/([^,]+), SignedHeaders=([^,]+), Signature=([0-9a-f]+)\z/,
               request.headers["authorization"]
             )

    assert scope == expected_scope
    uri = URI.parse(request.target)

    headers =
      Enum.map_join(String.split(names, ";"), "", fn name ->
        name <> ":" <> (request.headers[name] |> String.split() |> Enum.join(" ")) <> "\n"
      end)

    canonical =
      Enum.join(
        [request.method, uri.path, uri.query || "", headers, names, digest(request.body)],
        "\n"
      )

    scope = date <> "/" <> scope

    string =
      Enum.join(
        ["AWS4-HMAC-SHA256", request.headers["x-amz-date"], scope, digest(canonical)],
        "\n"
      )

    key =
      Enum.reduce(String.split(scope, "/"), "AWS4fixture-secret", fn part, key ->
        :crypto.mac(:hmac, :sha256, key, part)
      end)

    assert Base.encode16(:crypto.mac(:hmac, :sha256, key, string), case: :lower) == signature
  end

  defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

  defp run(context, args, inetrc, config, credentials, port, extra) do
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
        "AWS_CONFIG_FILE" => config,
        "AWS_SHARED_CREDENTIALS_FILE" => credentials,
        "AWS_ACCESS_KEY_ID" => "fixture-access",
        "AWS_SECRET_ACCESS_KEY" => "fixture-secret",
        "AWS_EC2_METADATA_DISABLED" => "true",
        "AWS_MAX_ATTEMPTS" => "1",
        "AWS_PAGER" => "",
        "AWS_CLI_AUTO_PROMPT" => "off",
        "AWS_ENDPOINT_URL_STS" => "http://sts.invalid:#{port}",
        "ESS_CONFIG_DIR" => Path.join(context.tmp_dir, "config"),
        "ESS_STATE_DIR" => Path.join(context.tmp_dir, "state"),
        "ERL_FLAGS" => "+S 2:2",
        "ERL_INETRC" => inetrc,
        "PAGER" => "cat"
      })
      |> Map.merge(extra)
      |> Map.to_list()

    {output, status} =
      System.cmd(
        "sh",
        [
          "-c",
          "ess_stderr=$1; shift; exec \"$@\" 2>\"$ess_stderr\"",
          "ess-paths",
          stderr,
          System.find_executable("escript"),
          context.executable | args
        ],
        env: env
      )

    {output, File.read!(stderr), status}
  end
end
