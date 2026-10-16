Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.AWSExpectedSizeTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation}
  @moduletag :tmp_dir
  @reference Path.expand("../../fixtures/aws_expected_size/reference.json", __DIR__)

  defp run(context, hint, root, input) do
    invocation = %Invocation{
      frontend: :s3,
      command: ["cp"],
      options: %{expected_size: hint, progress: false}
    }

    HighLevel.run(
      invocation,
      [%Target{kind: :stdio, path: "-"}, %Target{kind: :remote, bucket: "bucket", key: "key"}],
      [nil, context],
      stdin: input,
      tmp_dir: root,
      state_root: Path.join(root, "journals"),
      max_attempts: 1,
      emit: fn _ -> :ok end
    )
  end

  test "AWS stdin expected-size is a hint for both smaller and larger values", %{tmp_dir: root} do
    reference = @reference |> File.read!() |> JSON.decode!()
    assert reference["reference"]["version"] == "2.31.18"

    assert reference["reference"]["binary_sha256"] ==
             "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"

    for captured <- reference["small_cases"] do
      assert captured["exit"] == 0
      hint = Integer.to_string(captured["expected_size"])
      {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"uploaded\""}], ""}])
      {:ok, input} = StringIO.open("abc")
      assert :ok = run(%Context{endpoint: endpoint, unsigned: true}, hint, root, input)

      assert_receive {:wire_request,
                      %{method: "PUT", target: "/bucket/key", body: "abc", headers: headers}}

      assert headers["content-length"] == Integer.to_string(captured["wire"]["content_length"])
      assert File.ls!(root) == []
    end
  end

  test "large AWS hint changes multipart sizing while publishing actual bytes", %{tmp_dir: root} do
    captured = (@reference |> File.read!() |> JSON.decode!())["multipart_case"]
    assert captured["exit"] == 0
    bytes = String.duplicate("a", captured["actual_size"])
    {:ok, input} = StringIO.open(bytes)

    create =
      "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>owned</UploadId></InitiateMultipartUploadResult>"

    complete =
      "<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><ETag>&quot;done-1&quot;</ETag></CompleteMultipartUploadResult>"

    {endpoint, _} =
      WireServer.fixture([
        {200, [], create},
        {200, [{"ETag", "\"part\""}], ""},
        {200, [], complete},
        {200, [{"ETag", "\"done-1\""}, {"Content-Length", Integer.to_string(byte_size(bytes))}],
         ""}
      ])

    assert :ok =
             run(
               %Context{
                 frontend: :aws,
                 endpoint: endpoint,
                 unsigned: true,
                 options: %{resolved_endpoint: endpoint}
               },
               Integer.to_string(captured["expected_size"]),
               root,
               input
             )

    assert_receive {:wire_request, %{method: "POST", target: "/bucket/key?uploads="}}

    assert_receive {:wire_request,
                    %{method: "PUT", target: target, body: uploaded, headers: headers}}

    assert target =~ "partNumber=1"
    assert byte_size(uploaded) == captured["actual_size"]
    assert headers["content-length"] == Integer.to_string(captured["actual_size"])
    assert Base.encode16(:crypto.hash(:sha256, uploaded), case: :lower) == captured["body_sha256"]
    assert_receive {:wire_request, %{method: "POST", body: completion}}
    assert completion =~ "<PartNumber>1</PartNumber>"
    refute completion =~ "<PartNumber>2</PartNumber>"
    assert_receive {:wire_request, %{method: "HEAD"}}
    refute_receive {:wire_request, %{method: "PUT"}}
    refute Enum.any?(File.ls!(root), &String.starts_with?(&1, "ess-cli-"))
  end

  test "AWS hints never bypass the actual provider object limit", %{tmp_dir: root} do
    {:ok, input} = StringIO.open("abc")

    limits = %{
      max_single_put_bytes: 5,
      min_nonfinal_part_bytes: 5,
      max_part_bytes: 10,
      max_parts: 10,
      max_object_bytes: 2
    }

    context = %Context{unsigned: true, capabilities: %{transfer_limits: limits}}
    assert {:error, %Error{code: :object_size_limit}} = run(context, "1", root, input)
    assert File.ls!(root) == []
  end

  test "invalid AWS expected-size fails before reading stdin or starting a request", %{
    tmp_dir: root
  } do
    for hint <- ["-1", "1.5", "abc"] do
      {:ok, input} = StringIO.open("abc")
      assert {:error, %Error{}} = run(%Context{unsigned: true}, hint, root, input)
      assert StringIO.contents(input) == {"abc", ""}
      assert File.ls!(root) == []
    end
  end
end
