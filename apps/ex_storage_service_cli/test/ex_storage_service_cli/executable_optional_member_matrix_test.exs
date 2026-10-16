Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutableOptionalMemberMatrixTest do
  use ExUnit.Case, async: false
  import Bitwise
  alias ExStorageServiceCli.S3.XML
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @fixture Path.expand("../fixtures/differential/optional-members-wire.json", __DIR__)
  @oracle JSON.decode!(File.read!(@fixture))
  @checksum_headers ~w(x-amz-checksum-crc32 x-amz-checksum-crc32c x-amz-checksum-crc64nvme x-amz-checksum-sha1 x-amz-checksum-sha256)
  @governance_operations ~w(PutObjectRetention PutObjectLegalHold)

  setup_all do
    root = Path.expand("../..", __DIR__)

    directory =
      Path.join(System.tmp_dir!(), "ess-optional-members-#{System.unique_integer([:positive])}")

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

  test "independent capture names its bounded optional-member scope" do
    assert checksum("crc64nvme", "123456789") == Base.encode64(<<0xAE8B14860A799888::64>>)
    assert checksum("crc32c", "123456789") == Base.encode64(<<0xE3069283::32>>)

    assert Base.encode16(:crypto.hash(:sha256, File.read!(@fixture)), case: :lower) ==
             "c1e9736da1a821a3328c7464bf73c6d5c58148a9d231ce12616e80a196c105db"

    assert @oracle["aws_sha256"] ==
             "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"

    assert length(@oracle["cases"]) == 57
    assert @oracle["cases"] |> Enum.map(& &1["operation"]) |> Enum.uniq() |> length() == 32
    assert @oracle["scope"] =~ "not exhaustive"
    assert Enum.count(@oracle["cases"], &(&1["scenario"] == "access-denied-metadata")) == 6

    model = File.read!(Path.expand("../../priv/compatibility/aws/service-2.json", __DIR__))
    assert Base.encode16(:crypto.hash(:sha256, model), case: :lower) == @oracle["model_sha256"]

    for golden <- @oracle["cases"] do
      assert golden["aws"]["peer_errors"] == []
      assert [request] = golden["aws"]["requests"]

      for {name, value} <- request["headers"], name in @checksum_headers do
        assert value ==
                 checksum(
                   String.replace_prefix(name, "x-amz-checksum-", ""),
                   Base.decode64!(request["body_base64"])
                 )
      end

      assert golden["response"]["headers"]["x-amz-request-id"] == "optional-request"
      assert golden["response"]["headers"]["x-amz-id-2"] == "optional-host-id"
    end
  end

  test "expanded success preserves the raw response metadata" do
    golden = Enum.find(@oracle["cases"], &(&1["operation"] == "GetBucketReplication"))
    response = golden["response"]

    {endpoint, _} =
      WireServer.fixture([
        {response["status"], Map.to_list(response["headers"]),
         Base.decode64!(response["body_base64"])}
      ])

    assert {:ok, _result, metadata} =
             ExStorageServiceCli.S3.Client.call(
               %ExStorageServiceCli.Context{endpoint: endpoint, unsigned: true},
               golden["operation"],
               golden["input"],
               return_response: true,
               max_attempts: 1
             )

    assert metadata.status == response["status"]
    assert metadata.request_id == "optional-request"
    headers = Map.new(metadata.headers)
    for {name, value} <- response["headers"], do: assert(headers[name] == value)
    assert_receive {:wire_request, _}
  end

  test "expanded service errors preserve structured request and region metadata" do
    for golden <- @oracle["cases"], golden["scenario"] == "access-denied-metadata" do
      response = golden["response"]

      {endpoint, _} =
        WireServer.fixture([
          {response["status"], Map.to_list(response["headers"]),
           Base.decode64!(response["body_base64"])}
        ])

      assert {:error, error} =
               ExStorageServiceCli.S3.Client.call(
                 %ExStorageServiceCli.Context{endpoint: endpoint, unsigned: true},
                 golden["operation"],
                 golden["input"],
                 max_attempts: 1
               )

      assert error.category == :service
      assert error.code == "AccessDenied"
      assert error.http_status == 403
      assert error.request_id == "optional-request"
      assert error.operation == golden["operation"]
      assert error.details.bucket_region == "us-east-1"
      assert error.details.service_message == "Optional fixture denied"
      assert_receive {:wire_request, _}
    end
  end

  test "governance successes preserve typed charges and distinguish omitted from empty payloads" do
    for operation <- @governance_operations do
      cases =
        Enum.filter(@oracle["cases"], fn golden ->
          golden["operation"] == operation and golden["scenario"] != "access-denied-metadata"
        end)

      assert length(cases) == 4
      omitted = Enum.find(cases, &(&1["scenario"] == "omitted-payload"))
      empty = Enum.find(cases, &(&1["scenario"] == "empty-payload-structure"))
      assert [omitted_request] = omitted["aws"]["requests"]
      assert [empty_request] = empty["aws"]["requests"]
      assert Base.decode64!(omitted_request["body_base64"]) == ""
      assert Base.decode64!(empty_request["body_base64"]) != ""

      for golden <- cases do
        response = golden["response"]

        {endpoint, _} =
          WireServer.fixture([
            {response["status"], Map.to_list(response["headers"]),
             Base.decode64!(response["body_base64"])}
          ])

        assert {:ok, result, metadata} =
                 ExStorageServiceCli.S3.Client.call(
                   %ExStorageServiceCli.Context{endpoint: endpoint, unsigned: true},
                   operation,
                   golden["input"],
                   return_response: true,
                   max_attempts: 1
                 )

        assert result == (decode_output(golden["aws"]["stdout"]) || %{})
        assert metadata.status == response["status"]
        assert metadata.request_id == "optional-request"
        headers = Map.new(metadata.headers)
        for {name, value} <- response["headers"], do: assert(headers[name] == value)
        assert_receive {:wire_request, _}
      end
    end
  end

  test "the retained invalid provider checksum yields an explicit ESS checksum error" do
    golden = Enum.find(@oracle["cases"], &(&1["scenario"] == "invalid-response-checksum"))
    assert golden["aws"]["status"] == 0
    assert JSON.decode!(golden["aws"]["stdout"])["ChecksumSHA256"] == "YWJj"
    response = golden["response"]

    {endpoint, _} =
      WireServer.fixture([
        {response["status"], Map.to_list(response["headers"]),
         Base.decode64!(response["body_base64"])}
      ])

    assert {:error, %ExStorageServiceCli.Error{code: :upload_checksum_mismatch}} =
             ExStorageServiceCli.S3.Client.call(
               %ExStorageServiceCli.Context{endpoint: endpoint, unsigned: true},
               golden["operation"],
               Map.put(golden["input"], "Body", <<"fixture", 0, 255>>),
               allow_streaming: true,
               max_attempts: 1
             )

    assert_receive {:wire_request, _}
  end

  for golden <- @oracle["cases"] do
    @golden golden
    test "pinned optional AWS #{@golden["operation"]} #{@golden["scenario"]}", context do
      golden = @golden
      response = golden["response"]
      body = Base.decode64!(response["body_base64"])

      {endpoint, _server} =
        WireServer.fixture([{response["status"], Map.to_list(response["headers"]), body}])

      File.write!(
        Path.join(context.tmp_dir, "body.bin"),
        Base.decode64!(golden["source_body_base64"] || "Zml4dHVyZQD/")
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

      {stdout, stderr, status} = run(context, args)
      oracle = golden["aws"]

      if golden["scenario"] == "invalid-response-checksum" do
        # The oracle accepts this deliberately wrong service checksum. ESS keeps
        # its stricter upload-integrity contract, checked as a typed error above.
        assert oracle["status"] == 0
        assert status == 255
        assert String.trim(stderr) == "Upload response checksum does not match local bytes"
        assert stdout == ""
      else
        assert status == oracle["status"], stderr
        assert String.trim(stderr) == String.trim(oracle["stderr"])
        assert decode_output(stdout) == decode_output(oracle["stdout"])
      end

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

      if golden["operation"] in @governance_operations do
        for name <-
              ~w(x-amz-request-payer x-amz-expected-bucket-owner x-amz-bypass-governance-retention) do
          assert request.headers[name] == expected["headers"][name]
        end
      end

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
        key not in ~w(expect content-length content-md5 x-amz-content-sha256),
        key not in @checksum_headers do
      assert headers[key] == value, "request header #{key}"
    end

    if length = headers["content-length"],
      do: assert(String.to_integer(length) == byte_size(body))

    if hash = headers["x-amz-content-sha256"] do
      assert hash == Base.encode16(:crypto.hash(:sha256, body), case: :lower)
    end

    # XML sibling ordering can change bytes. Verify each executable's checksum
    # against its own entity, while comparing XML semantics independently.
    for {key, value} <- expected, key in @checksum_headers do
      assert is_binary(headers[key]), "missing request checksum #{key}"
      algorithm = String.replace_prefix(key, "x-amz-checksum-", "")
      assert headers[key] == checksum(algorithm, body)
      assert is_binary(value)
    end

    if expected["content-md5"], do: assert(headers["content-md5"] == checksum("md5", body))
  end

  defp checksum("crc32", body), do: Base.encode64(<<:erlang.crc32(body)::32>>)
  defp checksum("crc32c", body), do: Base.encode64(<<crc(body, 0x82F63B78, 0xFFFFFFFF)::32>>)

  defp checksum("crc64nvme", body),
    do: Base.encode64(<<crc(body, 0x9A6C9329AC4BC9B5, 0xFFFFFFFFFFFFFFFF)::64>>)

  defp checksum("sha256", body), do: Base.encode64(:crypto.hash(:sha256, body))
  defp checksum("sha1", body), do: Base.encode64(:crypto.hash(:sha, body))
  defp checksum("md5", body), do: Base.encode64(:crypto.hash(:md5, body))

  defp crc(body, polynomial, mask) do
    body
    |> :binary.bin_to_list()
    |> Enum.reduce(mask, fn byte, sum ->
      Enum.reduce(1..8, bxor(sum, byte), fn _, current ->
        bxor(bsr(current, 1), if(band(current, 1) == 1, do: polynomial, else: 0))
      end)
    end)
    |> bxor(mask)
  end

  defp run(context, args) do
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
          "ess-optional-members",
          stderr,
          System.find_executable("escript"),
          context.executable | args
        ],
        env: env
      )

    {output, File.read!(stderr), status}
  end
end
