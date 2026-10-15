Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutableRawTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir

  setup_all do
    root = Path.expand("../..", __DIR__)

    {output, status} =
      System.cmd("mix", ["escript.build", "--no-compile"],
        cd: root,
        env: [{"MIX_ENV", "test"}, {"PAGER", "cat"}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    directory = Path.join(System.tmp_dir!(), "ess-raw-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    executable = Path.join(directory, "ess")
    File.cp!(Path.join(root, "ess"), executable)
    on_exit(fn -> File.rm_rf!(directory) end)
    %{executable: executable}
  end

  test "cat, byte head and AWS stdout download preserve all 256 byte values", context do
    bytes = :binary.copy(:binary.list_to_bin(Enum.to_list(0..255)), 257)

    for command <- ["cat", "head", "s3-cp"] do
      get =
        {200, [{"Content-Length", Integer.to_string(byte_size(bytes))}, {"ETag", "\"e\""}], bytes}

      head =
        {200, [{"Content-Length", Integer.to_string(byte_size(bytes))}, {"ETag", "\"e\""}], ""}

      responses = if command == "s3-cp", do: [head, get], else: [get]
      {endpoint, _} = WireServer.fixture(responses)

      args =
        case command do
          "s3-cp" ->
            [
              "--endpoint-url",
              endpoint,
              "--no-cli-pager",
              "s3",
              "cp",
              "s3://bucket/key",
              "-",
              "--no-progress"
            ]

          "cat" ->
            ["--endpoint", endpoint, "cat", "s3://bucket/key"]

          "head" ->
            [
              "--endpoint",
              endpoint,
              "head",
              "s3://bucket/key",
              "--bytes",
              Integer.to_string(byte_size(bytes))
            ]
        end

      stderr = Path.join(context.tmp_dir, "stderr-" <> command)

      {output, status} =
        System.cmd(
          "sh",
          [
            "-c",
            "ess_raw_stderr=$1; shift; exec \"$@\" 2>\"$ess_raw_stderr\"",
            "ess-raw",
            stderr,
            System.find_executable("escript"),
            context.executable,
            "--no-sign-request" | args
          ],
          env: [
            {"PAGER", "cat"},
            {"ERL_FLAGS", "+S 2:2"},
            {"AWS_CONFIG_FILE", "/dev/null"},
            {"AWS_SHARED_CREDENTIALS_FILE", "/dev/null"},
            {"AWS_EC2_METADATA_DISABLED", "true"},
            {"ESS_CONFIG_DIR", context.tmp_dir},
            {"AWS_DEFAULT_REGION", "us-east-1"}
          ]
        )

      assert status == 0, File.read!(stderr)
      assert output == bytes
      assert File.read!(stderr) == ""
      for _ <- responses, do: assert_receive({:wire_request, _})
    end
  end
end
