Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.S3.SSECCustomizationsTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{CLI.HighLevelInput, Context, S3.Client, S3.Customizations}
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @fixture Path.expand("../../fixtures/differential/ssec-customizations-wire.json", __DIR__)
  @oracle JSON.decode!(File.read!(@fixture))

  setup_all do
    root = Path.expand("../../..", __DIR__)

    {output, status} =
      System.cmd("mix", ["escript.build", "--no-compile"],
        cd: root,
        env: [{"MIX_ENV", "test"}, {"PAGER", "cat"}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    %{executable: Path.join(root, "ess")}
  end

  test "capture is immutable and limited to the pinned customization hooks" do
    assert Base.encode16(:crypto.hash(:sha256, File.read!(@fixture)), case: :lower) ==
             "964ea227989f3057ddf85c49ffa6f1eae18b1b6bbcb3f768d3e5bd3a90e4b641"

    assert @oracle["aws_sha256"] ==
             "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"

    assert length(@oracle["cases"]) == 22
    assert Enum.count(@oracle["cases"], & &1["reject"]) == 6

    for golden <- @oracle["cases"] do
      assert golden["aws"]["peer_errors"] == []
      assert length(golden["aws"]["requests"]) == if(golden["reject"], do: 0, else: 1)
    end
  end

  for golden <- @oracle["cases"] do
    @golden golden
    test "pinned raw API #{@golden["operation"]} #{@golden["scenario"]}", context do
      golden = @golden
      File.write!(Path.join(context.tmp_dir, "key.txt"), "raw text 雪")

      File.write!(
        Path.join(context.tmp_dir, "key.bin"),
        :binary.list_to_bin(Enum.to_list(0..31)) <> <<255>>
      )

      response = golden["response"]

      {endpoint, _} =
        WireServer.fixture([
          {response["status"], Map.to_list(response["headers"]),
           Base.decode64!(response["body_base64"])}
        ])

      {stdout, stderr, status} = run(context, endpoint, golden["args"])
      assert status == golden["aws"]["status"], stderr

      if golden["reject"] do
        assert stderr =~ "Non ascii characters"
        refute_receive {:wire_request, _}, 20
      else
        assert JSON.decode!(stdout) == JSON.decode!(golden["aws"]["stdout"])
        assert_receive {:wire_request, request}
        [expected] = golden["aws"]["requests"]
        assert request.method == expected["method"]
        assert URI.parse(request.target).path == URI.parse(expected["target"]).path
        assert query(request.target) == query(expected["target"])

        for {name, value} <- expected["headers"],
            String.contains?(name, "customer") or String.starts_with?(name, "x-amz-meta-") do
          assert request.headers[name] == value
        end

        if golden["operation"] == "GetObject",
          do: assert(File.read!(Path.join(context.tmp_dir, "result.bin")) == "")
      end
    end
  end

  test "explicit MD5 presence bypasses both key transformations and preparation is stable" do
    for prefix <- ["SSECustomer", "CopySourceSSECustomer"], md5 <- ["caller-md5", "", nil] do
      input = %{(prefix <> "Key") => "caller-key", (prefix <> "KeyMD5") => md5}
      assert {:ok, ^input, %{}} = Customizations.input("CopyObject", input)
    end

    assert {:ok, prepared, %{}} = Customizations.input("HeadObject", %{"SSECustomerKey" => "raw"})
    assert {:ok, ^prepared, %{}} = Customizations.input("HeadObject", prepared)
  end

  test "high-level preencoded destination and source keys pass through unchanged" do
    assert {:ok, decoded} =
             HighLevelInput.decode(%{
               sse_c: "AES256",
               sse_c_key: String.duplicate("d", 32),
               sse_c_copy_source: "AES256",
               sse_c_copy_source_key: String.duplicate("s", 32)
             })

    source = Map.new(decoded.source_input, fn {key, value} -> {"CopySource" <> key, value} end)
    input = Map.merge(decoded.input, source)
    assert {:ok, ^input, %{}} = Customizations.input("CopyObject", input)
  end

  test "raw keys stay stable across service retries" do
    {endpoint, _} =
      WireServer.fixture([{503, [], "<Error><Code>SlowDown</Code></Error>"}, {200, [], ""}])

    assert {:ok, %{}} =
             Client.call(
               %Context{endpoint: endpoint, unsigned: true},
               "HeadObject",
               %{"Bucket" => "bucket", "Key" => "key", "SSECustomerKey" => "raw"},
               max_attempts: 2,
               sleep: fn _ -> :ok end
             )

    assert_receive {:wire_request, %{headers: first}}
    assert_receive {:wire_request, %{headers: second}}

    for name <- [
          "x-amz-server-side-encryption-customer-key",
          "x-amz-server-side-encryption-customer-key-md5"
        ] do
      assert first[name] == second[name]
    end

    assert first["x-amz-server-side-encryption-customer-key"] == "cmF3"
  end

  test "hook scope leaves unrelated members and validation errors to the model" do
    input = %{"SSECustomerKey" => "raw", "Metadata" => %{"owner" => "雪"}}
    assert {:ok, ^input, %{}} = Customizations.input("ListParts", input)

    assert {:ok, %{"Metadata" => "invalid"}, %{}} =
             Customizations.input("PutObject", %{"Metadata" => "invalid"})
  end

  defp query(target),
    do:
      target
      |> URI.parse()
      |> Map.get(:query)
      |> then(&URI.query_decoder(&1 || ""))
      |> Enum.sort()

  defp run(context, endpoint, args) do
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
        "AWS_MAX_ATTEMPTS" => "1",
        "AWS_EC2_METADATA_DISABLED" => "true",
        "AWS_PAGER" => "",
        "AWS_CLI_AUTO_PROMPT" => "off",
        "AWS_RESPONSE_CHECKSUM_VALIDATION" => "when_required",
        "ERL_FLAGS" => "+S 2:2",
        "NO_PROXY" => "*",
        "PAGER" => "cat"
      })

    args = Enum.map(args, &String.replace(&1, "@TMP@", context.tmp_dir))

    {stdout, status} =
      System.cmd(
        "sh",
        [
          "-c",
          "ess_stderr=$1; shift; exec \"$@\" 2>\"$ess_stderr\"",
          "ess-ssec",
          stderr,
          System.find_executable("escript"),
          context.executable,
          "--endpoint-url",
          endpoint,
          "--region",
          "us-east-1",
          "--no-cli-pager",
          "--no-paginate" | args
        ],
        env: Map.to_list(env)
      )

    {stdout, File.read!(stderr), status}
  end
end
