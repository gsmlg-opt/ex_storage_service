Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativePutOptionsTest do
  # Journal mutex ports share the host namespace with wire fixtures.
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, Parser}
  alias ExStorageServiceCli.Transfer.{Journal, Planner}
  @moduletag :tmp_dir
  @fixture_attempts 4
  @limits %{
    max_single_put_bytes: 100,
    min_nonfinal_part_bytes: 5,
    max_part_bytes: 100,
    max_parts: 10,
    max_object_bytes: 1000
  }
  @create "<InitiateMultipartUploadResult><UploadId>owned</UploadId></InitiateMultipartUploadResult>"
  @complete "<CompleteMultipartUploadResult><ETag>&quot;done&quot;</ETag></CompleteMultipartUploadResult>"

  defp remote, do: %Target{kind: :remote, bucket: "bucket", key: "key"}

  defp invocation(options, command \\ "put"),
    do: %Invocation{
      frontend: :native,
      command: [command],
      options: Map.put(options, :progress, false)
    }

  defp put(root, endpoint, data, options, opts \\ []),
    do: put(root, endpoint, data, options, opts, 0)

  defp put(root, endpoint, data, options, opts, attempt) do
    path = Path.join(root, "source")
    File.write!(path, data)

    name = if attempt == 0, do: "journals", else: "journals-#{attempt}"

    result =
      HighLevel.run(
        invocation(options),
        [%Target{kind: :local, path: path}, remote()],
        [
          nil,
          %Context{
            endpoint: endpoint,
            unsigned: true,
            options: %{request_checksum_calculation: "when_required"}
          }
        ],
        Keyword.merge(
          [
            limits: @limits,
            state_root: Path.join(root, name),
            tmp_dir: root,
            max_attempts: 1,
            emit: fn _ -> :ok end
          ],
          opts
        )
      )

    case result do
      {:error,
       %Error{
         code: :journal_busy,
         details: %{mutex_port: _, socket_reason: :eaddrinuse} = details
       }}
      when map_size(details) == 2 and attempt + 1 < @fixture_attempts ->
        # Retry only initial fixture acquisition, before any owned upload exists.
        # Later errors carry journal_id/cleanup and must retain their outcome.
        refute_receive {:wire_request, _}, 0
        put(root, endpoint, data, options, opts, attempt + 1)

      result ->
        result
    end
  end

  test "initial occupied journal mutex retries setup before the first upload request", %{
    tmp_dir: root
  } do
    {fixture_root, blocked_root, listener} =
      Enum.find_value(1..10, fn attempt ->
        fixture_root = Path.join(root, "occupied-#{attempt}")
        blocked_root = Path.join(fixture_root, "journals")
        File.mkdir_p!(blocked_root)
        File.chmod!(blocked_root, 0o700)
        {:ok, port} = Journal.lock_port(blocked_root, "index")

        case :gen_tcp.listen(port, [:binary, active: false, ip: {127, 0, 0, 1}]) do
          {:ok, listener} -> {fixture_root, blocked_root, listener}
          {:error, :eaddrinuse} -> nil
        end
      end) || flunk("Could not reserve the initial journal fixture mutex")

    try do
      {endpoint, _} =
        WireServer.fixture([
          {200, [], @create},
          {200, [{"ETag", "\"one\""}], ""},
          {200, [{"ETag", "\"two\""}], ""},
          {200, [], @complete},
          {200, [{"ETag", "\"done\""}, {"Content-Length", "10"}], ""}
        ])

      assert :ok = put(fixture_root, endpoint, "abcdefghij", %{part_size: "5B"})
      refute File.exists?(Path.join(blocked_root, "index.jsonl"))
      assert {:ok, _} = :inet.sockname(listener)

      for method <- ["POST", "PUT", "PUT", "POST", "HEAD"] do
        assert_receive {:wire_request, %{method: ^method}}
      end

      refute_receive {:wire_request, _}
    after
      :gen_tcp.close(listener)
    end
  end

  test "put alone registers real multipart and checksum controls while pending flags remain rejected" do
    assert {:ok, %{options: %{part_size: "7B", disable_multipart: true, checksum: "sha256"}}} =
             Parser.parse([
               "put",
               "source",
               "s3://bucket/key",
               "--part-size",
               "7B",
               "--disable-multipart",
               "--checksum",
               "sha256"
             ])

    for prefix <- [["cp"], ["s3", "cp"], ["get"]], flag <- ["--part-size", "--checksum"] do
      assert {:error, %Error{}} =
               Parser.parse(prefix ++ ["source", "s3://bucket/key", flag, "7B"])
    end

    for flag <- ["--enc-c", "--enc-kms", "--enc-s3"] do
      assert {:error, %Error{}} =
               Parser.parse(["put", "source", "s3://bucket/key", flag, "value"])
    end
  end

  test "explicit part size changes actual part boundaries and preserves the completed identity",
       %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], @create},
        {200, [{"ETag", "\"one\""}], ""},
        {200, [{"ETag", "\"two\""}], ""},
        {200, [{"ETag", "\"three\""}], ""},
        {200, [], @complete},
        {200, [{"ETag", "\"done\""}, {"Content-Length", "15"}], ""}
      ])

    assert :ok =
             put(root, endpoint, "abcdefghijklmno", %{
               part_size: "7B",
               checksum: "SHA256",
               if_not_exists: true
             })

    assert_receive {:wire_request,
                    %{method: "POST", target: create_target, headers: create_headers}}

    assert URI.decode_query(URI.parse(create_target).query) == %{"uploads" => ""}

    assert create_headers["x-amz-checksum-algorithm"] == "SHA256"
    assert create_headers["x-amz-checksum-type"] == "COMPOSITE"

    for {number, bytes} <- [{1, "abcdefg"}, {2, "hijklmn"}, {3, "o"}] do
      assert_receive {:wire_request,
                      %{method: "PUT", target: target, body: ^bytes, headers: part_headers}}

      assert URI.decode_query(URI.parse(target).query)["partNumber"] == to_string(number)
      assert part_headers["x-amz-checksum-sha256"] == Base.encode64(:crypto.hash(:sha256, bytes))
    end

    assert_receive {:wire_request, %{method: "POST", body: manifest, headers: complete_headers}}
    assert manifest =~ "<PartNumber>3</PartNumber>"
    assert manifest =~ "<ChecksumSHA256>"
    assert complete_headers["if-none-match"] == "*"
    assert_receive {:wire_request, %{method: "HEAD"}}
    refute_receive {:wire_request, _}
  end

  test "selected checksum detects a provider digest that disagrees with source bytes", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"done\""}, {"x-amz-checksum-sha256", Base.encode64(<<0::256>>)}], ""}
      ])

    assert {:error, %Error{code: :upload_checksum_mismatch}} =
             put(root, endpoint, "123456789", %{checksum: "SHA256"})

    assert_receive {:wire_request, %{method: "PUT", body: "123456789"}}
    refute_receive {:wire_request, _}
  end

  test "an object exactly the explicit part size remains a single PUT", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"done\""}], ""}])
    assert :ok = put(root, endpoint, "abcdefg", %{part_size: "7B"})
    assert_receive {:wire_request, %{method: "PUT", target: "/bucket/key", body: "abcdefg"}}
    refute_receive {:wire_request, _}
  end

  test "disable multipart overrides thresholds and retains explicit checksum on a single PUT", %{
    tmp_dir: root
  } do
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"done\""}], ""}])

    assert :ok =
             put(
               root,
               endpoint,
               "abcdefghijklmno",
               %{part_size: "7B", disable_multipart: true, checksum: "sha256"},
               multipart_threshold: 1
             )

    assert_receive {:wire_request,
                    %{method: "PUT", target: "/bucket/key", body: bytes, headers: headers}}

    assert bytes == "abcdefghijklmno"
    assert headers["x-amz-checksum-sha256"] == Base.encode64(:crypto.hash(:sha256, bytes))
    refute_receive {:wire_request, _}
  end

  for {selected, algorithm, digest} <- [
        {"crc32", "crc32", "y/Q5Jg=="},
        {"CRC32C", "crc32c", "4waSgw=="},
        {"CRC64N", "crc64nvme", "rosUhgp5mIg="},
        {"CRC64NVME", "crc64nvme", "rosUhgp5mIg="},
        {"sha1", "sha1", "98O8HYCOBHMq32eZZczDTKeuNEE="},
        {"sHa256", "sha256", "FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU="}
      ] do
    test "checksum #{selected} hashes actual uploaded bytes", %{tmp_dir: root} do
      {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"done\""}], ""}])
      assert :ok = put(root, endpoint, "123456789", %{checksum: unquote(selected)})
      assert_receive {:wire_request, %{method: "PUT", body: "123456789", headers: headers}}
      assert headers["x-amz-checksum-" <> unquote(algorithm)] == unquote(digest)
    end
  end

  test "invalid, unsupported and conflicting selections fail before source or provider access", %{
    tmp_dir: root
  } do
    bad = [
      %{part_size: "wrong"},
      %{part_size: "-1MiB"},
      %{part_size: "0"},
      %{part_size: "4B"},
      %{part_size: "101B"},
      %{part_size: "1..2MB"},
      %{checksum: "MD5"},
      %{checksum: "CRC32-FO"},
      %{checksum: "CRC32C-FO"},
      %{checksum: "unknown"},
      %{checksum: "SHA256", checksum_algorithm: "CRC32"},
      %{disable_multipart: "true"}
    ]

    for options <- bad do
      assert {:error, %Error{category: :syntax, code: :native_put_option}} =
               HighLevel.run(
                 invocation(options),
                 [%Target{kind: :local, path: Path.join(root, "missing")}, remote()],
                 [nil, %Context{endpoint: "http://127.0.0.1:1"}],
                 limits: @limits
               )
    end

    for targets <- [
          [remote(), %Target{kind: :local, path: Path.join(root, "dest")}],
          [remote(), remote()]
        ] do
      assert {:error, %Error{category: :syntax, code: :native_put_option}} =
               HighLevel.run(invocation(%{checksum: "SHA256"}), targets, [nil, %Context{}])
    end

    assert {:error, %Error{category: :syntax, code: :native_put_option}} =
             HighLevel.run(
               invocation(%{part_size: "7B"}, "cp"),
               [%Target{kind: :local, path: "missing"}, remote()],
               [nil, %Context{}]
             )
  end

  test "disabling multipart cannot exceed provider single PUT limits or conflict with force", %{
    tmp_dir: root
  } do
    {endpoint, _} = WireServer.fixture([{500, [], ""}])

    assert {:error, %Error{code: :single_put_limit}} =
             put(root, endpoint, String.duplicate("x", 101), %{disable_multipart: true})

    assert {:error, %Error{category: :syntax, code: :native_put_option}} =
             put(root, endpoint, "bytes", %{disable_multipart: true}, force_multipart: true)

    refute_receive {:wire_request, _}

    assert {:error, %Error{code: :single_put_limit}} =
             Planner.plan(101, @limits, disable_multipart: true)
  end

  test "SI IEC comma and decimal units retain exact part boundaries", %{tmp_dir: root} do
    limits = %{
      @limits
      | max_part_bytes: 10_000,
        max_single_put_bytes: 10_000,
        max_object_bytes: 100_000
    }

    for {text, size} <- [
          {".5 KiB", 512},
          {"1,000 B", 1000},
          {"1.5KB", 1500},
          {"1.KiB", 1024},
          {"5b", 5}
        ] do
      data = String.duplicate("x", size + 1)

      {endpoint, _} =
        WireServer.fixture([
          {200, [], @create},
          {200, [{"ETag", "\"one\""}], ""},
          {200, [{"ETag", "\"two\""}], ""},
          {200, [], @complete},
          {200, [{"ETag", "\"done\""}, {"Content-Length", to_string(size + 1)}], ""}
        ])

      assert :ok = put(root, endpoint, data, %{part_size: text}, limits: limits)
      assert_receive {:wire_request, %{method: "POST"}}
      assert_receive {:wire_request, %{method: "PUT", body: first}}
      assert byte_size(first) == size
      assert_receive {:wire_request, %{method: "PUT", body: "x"}}
      assert_receive {:wire_request, %{method: "POST"}}
      assert_receive {:wire_request, %{method: "HEAD"}}
      refute_receive {:wire_request, _}
    end
  end

  test "explicit part sizes never silently grow to evade the provider part count", %{
    tmp_dir: root
  } do
    {endpoint, _} = WireServer.fixture([{500, [], ""}])

    assert {:error, %Error{code: :multipart_limit}} =
             put(root, endpoint, String.duplicate("x", 71), %{part_size: "7B"})

    refute_receive {:wire_request, _}

    assert {:error, %Error{code: :multipart_limit}} =
             Planner.plan(71, @limits,
               part_size: 7,
               strict_part_size: true,
               force_multipart: true
             )
  end

  test "explicit zero and empty sizes select the pinned SDK fallback sizes", %{tmp_dir: root} do
    for size <- ["0", ""] do
      {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"done\""}], ""}])

      assert :ok =
               put(root, endpoint, "bytes", %{part_size: size},
                 limits: %{@limits | max_part_bytes: 40_000_000, max_single_put_bytes: 40_000_000}
               )

      assert_receive {:wire_request, %{method: "PUT", body: "bytes"}}
    end
  end
end
