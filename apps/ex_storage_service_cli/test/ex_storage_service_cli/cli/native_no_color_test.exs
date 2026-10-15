Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeNoColorTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Error, Output, Test.WireServer}
  alias ExStorageServiceCli.CLI.Runner
  @moduletag :tmp_dir

  test "native no-color removes bucket success styling and preserves the next default", %{
    tmp_dir: root
  } do
    for no_color <- [true, false] do
      {endpoint, _} = WireServer.fixture([{200, [], ""}])
      output = capture_io(fn -> assert :ok = run(["mb", "bucket"], endpoint, root, no_color) end)
      assert output =~ "Bucket 'bucket' created"
      assert String.contains?(output, "\e[") == not no_color
    end
  end

  test "native no-color removes error styling and restores defaults after failure", %{
    tmp_dir: root
  } do
    for no_color <- [true, false] do
      body = "<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>"
      {endpoint, _} = WireServer.fixture([{403, [], body}])

      output =
        capture_io(:stderr, fn ->
          assert {:error, %Error{}} = run(["mb", "bucket"], endpoint, root, no_color)
        end)

      assert output =~ "Error:"
      assert String.contains?(output, "\e[") == not no_color
    end
  end

  test "all legacy help honors no-color without changing later defaults" do
    for command <- ~w(configure mb rb ls tree cp rm mv presign info) do
      plain = capture_io(fn -> assert :ok = Runner.run([command, "--help", "--no-color"]) end)
      refute plain =~ "\e["
      assert plain =~ "USAGE"
      colored = capture_io(fn -> assert :ok = Runner.run([command, "--help"]) end)
      assert colored =~ "\e["
    end
  end

  test "native table styling obeys no-color", %{tmp_dir: root} do
    body =
      "<ListAllMyBucketsResult><Buckets><Bucket><Name>bucket</Name><CreationDate>2026-10-15T00:00:00Z</CreationDate></Bucket></Buckets></ListAllMyBucketsResult>"

    {endpoint, _} = WireServer.fixture([{200, [], body}])
    output = capture_io(fn -> assert :ok = run(["ls"], endpoint, root, true) end)
    assert output =~ "BUCKET"
    assert output =~ "bucket"
    refute output =~ "\e["
  end

  test "native info routes its direct headings endpoint and status styling", %{tmp_dir: root} do
    for no_color <- [true, false] do
      {endpoint, _} =
        WireServer.fixture([
          {200, [{"Content-Type", "application/json"}], ~s({"status":"ok","objects":1})}
        ])

      output = capture_io(fn -> assert :ok = run(["info"], endpoint, root, no_color) end)
      assert output =~ "Server Information"
      assert output =~ "Status:"
      assert output =~ "ok"
      assert String.contains?(output, "\e[") == not no_color
    end
  end

  test "output color policy is process-local nested and restored after exceptions" do
    escape = IO.ANSI.red()
    assert Output.color(escape) == escape

    assert_raise RuntimeError, "fixture", fn ->
      Output.with_options(%{no_color: true}, fn ->
        assert Output.color(escape) == ""
        assert Task.await(Task.async(fn -> Output.color(escape) end)) == escape

        assert capture_io(fn -> Output.info(escape <> "object bytes") end) ==
                 escape <> "object bytes\n"

        Output.with_options(%{}, fn -> assert Output.color(escape) == escape end)
        assert Output.color(escape) == ""
        raise "fixture"
      end)
    end

    assert Output.color(escape) == escape
  end

  defp run(args, endpoint, root, no_color) do
    args = args ++ ["--endpoint", endpoint, "--no-sign-request", "--config-dir", root]
    Runner.run(if(no_color, do: args ++ ["--no-color"], else: args))
  end
end
