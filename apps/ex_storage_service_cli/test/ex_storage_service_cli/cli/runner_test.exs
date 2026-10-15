defmodule ExStorageServiceCli.CLI.RunnerTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{CLI.Runner, Error}

  test "help and version do not resolve configuration or credentials" do
    for argv <- [
          ["--help"],
          ["--help", "cp"],
          ["alias", "set", "--help"],
          ["s3", "cp", "--help"],
          ["--version"]
        ] do
      assert capture_io(fn -> assert :ok = Runner.run(argv) end) =~ "ess"
    end
  end

  test "parse failures are typed and command argument values are redacted" do
    assert {:error, %Error{category: :syntax, message: message}} =
             Runner.run(["ls", "--unknown", "private-secret"])

    refute message =~ "private-secret"
  end

  test "completion is generated without a credential or config read" do
    output = capture_io(fn -> assert :ok = Runner.run(["completion", "bash"]) end)
    assert output =~ "complete"
    assert output =~ "configure"
    assert output =~ "alias"
  end
end
