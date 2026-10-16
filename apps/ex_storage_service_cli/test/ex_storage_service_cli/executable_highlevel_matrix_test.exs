Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutableHighlevelMatrixTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.S3.XML
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @fixture Path.expand("../fixtures/differential/highlevel-wire.json", __DIR__)
  @oracle JSON.decode!(File.read!(@fixture))

  setup_all do
    directory =
      Path.join(System.tmp_dir!(), "ess-highlevel-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)

    {output, status} =
      System.cmd("mix", ["escript.build", "--no-compile"],
        cd: Path.expand("../..", __DIR__),
        env: [{"MIX_ENV", "test"}, {"PAGER", "cat"}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    executable = Path.join(directory, "ess")
    File.cp!(Path.expand("../../ess", __DIR__), executable)
    %{executable: executable}
  end

  test "independent capture includes all nine high-level commands" do
    assert Base.encode16(:crypto.hash(:sha256, File.read!(@fixture)), case: :lower) ==
             "30f94eebef01b1d71912bd7538d1704154eb830229b79c8d8756ffdc091643ac"

    assert @oracle["aws_sha256"] ==
             "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"

    assert length(@oracle["cases"]) == 42

    assert Enum.sort(Enum.uniq(Enum.map(@oracle["cases"], & &1["command"]))) ==
             ~w(cp ls mb mv presign rb rm sync website)

    assert Enum.all?(
             @oracle["cases"],
             &(&1["aws"]["status"] == (&1["expected_status"] || 0) and
                 &1["aws"]["peer_errors"] == [])
           )
  end

  for golden <- @oracle["cases"] do
    @golden golden
    test "pinned high-level #{@golden["name"]}", context do
      golden = @golden
      raw = <<"fixture", 0, 255>>
      File.write!(Path.join(context.tmp_dir, "source.bin"), raw)
      File.mkdir_p!(Path.join(context.tmp_dir, "local"))
      File.write!(Path.join(context.tmp_dir, "local/file.bin"), raw)
      File.write!(Path.join(context.tmp_dir, "local/ignored.txt"), "excluded")

      File.write!(
        Path.join(context.tmp_dir, "destination.key"),
        :binary.list_to_bin(Enum.to_list(0..31))
      )

      File.write!(
        Path.join(context.tmp_dir, "source.key"),
        :binary.list_to_bin(Enum.to_list(32..63))
      )

      for {name, contents} <- golden["local_files"] || %{} do
        path = Path.join(context.tmp_dir, name)
        File.mkdir_p!(Path.dirname(path))
        File.write!(path, Base.decode64!(contents["body_base64"]))
        File.touch!(path, contents["mtime"])
      end

      {endpoint, _server} = peer(golden)
      args = Enum.map(golden["args"], &String.replace(&1, "@TMP@", context.tmp_dir))
      {stdout, stderr, status} = run(context, endpoint, args)
      oracle = golden["aws"]
      requests = collect_requests([])
      assert_ssec_probes(requests, golden)

      assert status == oracle["status"],
             stderr <> inspect(Enum.map(requests, &{&1.method, &1.target}))

      if golden["archive"] && oracle["status"] == 2 do
        # Preserve the captured warning outcome; existing ESS wording is distinct.
        assert oracle["stderr"] =~ "warning:"
        assert stderr =~ "warning:"
      else
        assert stderr == oracle["stderr"]
      end

      assert stdout
             |> String.replace(context.tmp_dir, "@TMP@")
             |> String.replace(endpoint, "@ENDPOINT@") ==
               oracle["stdout"]

      requests = safety_probes(requests, golden)
      assert length(requests) == length(oracle["requests"])

      for {request, expected} <- Enum.zip(requests, oracle["requests"]) do
        assert request.method == expected["method"]
        assert target(request.target) == target(expected["target"])
        assert_body(request.body, expected)
        assert ssec_headers(request.headers) == ssec_headers(expected["headers"])

        for {key, value} <- expected["headers"],
            key not in ~w(expect content-length x-amz-content-sha256),
            not String.starts_with?(key, "x-amz-checksum-") do
          assert header(key, request.headers[key]) == header(key, value),
                 "request header #{key}"
        end

        if length = request.headers["content-length"],
          do: assert(String.to_integer(length) == byte_size(request.body))
      end

      refute_receive {:wire_request, _}, 10
      refute_receive {:wire_error, _, _, _}, 10

      if outfile = oracle["outfile_base64"],
        do:
          assert(File.read!(Path.join(context.tmp_dir, "result.bin")) == Base.decode64!(outfile))

      for {name, contents} <- oracle["local_after"] || %{} do
        path = Path.join(context.tmp_dir, name)

        if contents do
          assert File.read!(path) == Base.decode64!(contents["body_base64"])
          assert File.stat!(path, time: :posix).mtime == contents["mtime"]
        else
          refute File.exists?(path)
        end
      end
    end
  end

  # S3 accepts the optional leading slash before the source bucket. Preserve
  # the raw escaped bucket/key/version bytes after that single separator.
  defp header("x-amz-copy-source", "/" <> value), do: value
  defp header(_key, value), do: value

  defp target(value) do
    uri = URI.parse(value)
    # An explicit service-default page bound preserves this single-page contract.
    query =
      uri.query
      |> to_string()
      |> URI.query_decoder()
      |> Enum.reject(&(&1 == {"max-keys", "1000"}))
      |> Enum.sort()

    {uri.path, query}
  end

  defp peer(golden) do
    WireServer.start(fn listener, owner -> peer_loop(listener, owner, golden) end)
  end

  defp peer_loop(listener, owner, golden) do
    {:ok, socket} = :gen_tcp.accept(listener, 15_000)
    request = WireServer.request(socket)
    send(owner, {:wire_request, request})

    response =
      Enum.zip(golden["aws"]["requests"], golden["responses"])
      |> Enum.find_value(fn {expected, response} ->
        if request.method == expected["method"] and
             target(request.target) == target(expected["target"]),
           do: response
      end)

    response =
      if response == nil and request.method == "HEAD",
        do: (golden["head_responses"] || %{})[URI.parse(request.target).path],
        else: response

    response =
      if response == nil and golden["name"] == "sync-upload" and request.method == "HEAD" and
           URI.parse(request.target).path == "/bucket/p/file.bin",
         do: %{"status" => 404, "headers" => %{}, "body_base64" => ""},
         else: response

    response =
      if response == nil and String.contains?(golden["name"], "server-copy") and
           request.method == "HEAD" and
           URI.parse(request.target).path == "/bucket/p/destination",
         do: hd(golden["responses"]),
         else: response

    if response == nil, do: raise("Unexpected request #{request.method} #{request.target}")

    WireServer.response(
      socket,
      response["status"],
      Map.to_list(response["headers"]),
      Base.decode64!(response["body_base64"])
    )

    :gen_tcp.close(socket)
    peer_loop(listener, owner, golden)
  end

  defp collect_requests(requests) do
    receive do
      {:wire_request, request} -> collect_requests([request | requests])
    after
      10 -> Enum.reverse(requests)
    end
  end

  defp safety_probes(requests, %{"name" => "sync-upload"}) do
    Enum.reject(requests, fn request ->
      request.method == "HEAD" and URI.parse(request.target).path == "/bucket/p/file.bin"
    end)
  end

  defp safety_probes(requests, %{"sync_direction" => _, "head_responses" => heads} = golden) do
    expected = golden["aws"]["requests"]

    Enum.reject(requests, fn request ->
      request.method == "HEAD" and Map.has_key?(heads, URI.parse(request.target).path) and
        not Enum.any?(expected, &(&1["method"] == "HEAD" and &1["target"] == request.target))
    end)
  end

  defp safety_probes(requests, %{"name" => "mv-server-copy"}), do: copy_probes(requests)

  defp safety_probes(requests, %{"name" => "mv-server-copy-archive-" <> _}),
    do: copy_probes(requests)

  defp safety_probes(requests, %{"name" => "cp-server-copy-" <> _}), do: copy_probes(requests)
  defp safety_probes(requests, _golden), do: requests

  defp assert_ssec_probes(requests, %{"name" => "cp-server-copy-ssec", "aws" => oracle}) do
    copy = Enum.find(oracle["requests"], &(&1["method"] == "PUT"))
    source = Enum.find(oracle["requests"], &(&1["method"] == "HEAD"))

    destination_headers =
      Map.reject(ssec_headers(copy["headers"]), fn {key, _} ->
        String.starts_with?(key, "x-amz-copy-source-")
      end)

    for request <- requests, request.method == "HEAD" do
      expected =
        if URI.parse(request.target).path == "/bucket/p/source",
          do: ssec_headers(source["headers"]),
          else: destination_headers

      assert ssec_headers(request.headers) == expected
    end
  end

  defp assert_ssec_probes(_requests, _golden), do: :ok

  defp ssec_headers(headers),
    do:
      Map.filter(headers, fn {key, _} ->
        String.contains?(key, "server-side-encryption-customer")
      end)

  defp copy_probes(requests) do
    requests =
      Enum.reject(requests, fn request ->
        request.method == "HEAD" and URI.parse(request.target).path == "/bucket/p/destination"
      end)

    {requests, _seen} =
      Enum.map_reduce(requests, false, fn request, seen ->
        if request.method == "HEAD" and URI.parse(request.target).path == "/bucket/p/source",
          do: {if(seen, do: nil, else: request), true},
          else: {request, seen}
      end)

    Enum.reject(requests, &is_nil/1)
  end

  defp assert_body(body, %{"xml_semantics" => expected}) do
    assert {:ok, node} = XML.parse(body)
    assert xml(node) == expected
  end

  defp assert_body(body, expected), do: assert(body == Base.decode64!(expected["body_base64"]))

  defp xml(node) do
    children =
      node.content
      |> Enum.filter(&match?(%XML.Node{}, &1))
      |> Enum.group_by(&XML.local_name(&1.name))

    %{
      "name" => XML.local_name(node.name),
      "attributes" =>
        node.attributes
        |> Enum.reject(fn {key, _} -> String.starts_with?(key, "xmlns") end)
        |> Map.new(fn {key, value} -> {XML.local_name(key), value} end),
      "text" => node.content |> Enum.filter(&is_binary/1) |> Enum.join(),
      "children" => Map.new(children, fn {name, values} -> {name, Enum.map(values, &xml/1)} end)
    }
  end

  defp run(context, endpoint, args) do
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
        "PAGER" => "cat",
        "TZ" => "UTC"
      })
      |> Map.to_list()

    {stdout, status} =
      System.cmd(
        "sh",
        [
          "-c",
          "ess_stderr=$1; shift; exec \"$@\" 2>\"$ess_stderr\"",
          "ess-highlevel",
          stderr,
          System.find_executable("escript"),
          context.executable,
          "--endpoint-url",
          endpoint,
          "--region",
          "us-east-1",
          "--no-cli-pager",
          "--cli-connect-timeout",
          "1",
          "--cli-read-timeout",
          "2" | args
        ],
        env: env,
        cd: context.tmp_dir
      )

    {stdout, File.read!(stderr), status}
  end
end
