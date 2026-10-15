defmodule ExStorageServiceCli.CLI.AutoPromptTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Error, CLI.AutoPrompt, CLI.AWSInput}

  defp reader(answers) do
    owner = self()
    Enum.each(answers, &send(owner, {:answer, &1}))

    fn question ->
      send(owner, {:question, question})

      receive do
        {:answer, value} -> {:ok, value}
      after
        100 -> :eof
      end
    end
  end

  test "opt-in fills missing model input and asks for confirmation without writing stdout" do
    opts = [tty: true, profile_settings: %{}, prompt_read: reader(["bucket", "key", "yes"])]

    stdout =
      capture_io(fn ->
        stderr =
          capture_io(:stderr, fn ->
            assert {:ok, parsed} = AWSInput.parse("HeadObject", ["--cli-auto-prompt"], opts)
            assert parsed.input == %{"Bucket" => "bucket", "Key" => "key"}
          end)

        assert stderr =~ "Bucket"
        assert stderr =~ "Key"
        assert stderr =~ "Run HeadObject"
      end)

    assert stdout == ""
  end

  test "explicit disable overrides inherited prompt modes and non-TTY opt-in fails safely" do
    assert {:ok, "off"} = AutoPrompt.mode(%{no_cli_auto_prompt: true, cli_auto_prompt_mode: "on"})
    assert {:ok, %{}} = AutoPrompt.run("HeadBucket", %{}, %{no_cli_auto_prompt: true}, tty: false)

    assert {:error, %Error{code: :auto_prompt_requires_tty}} =
             AutoPrompt.run("HeadBucket", %{}, %{cli_auto_prompt: true}, tty: false)

    assert {:error, %Error{code: :conflicting_auto_prompt}} =
             AutoPrompt.mode(%{cli_auto_prompt: true, no_cli_auto_prompt: true})

    assert {:error, %Error{code: :invalid_auto_prompt}} =
             AutoPrompt.mode(%{cli_auto_prompt_mode: "wrong"})
  end

  test "on-partial leaves complete inputs alone and declined prompts never execute" do
    assert {:ok, %{"Bucket" => "bucket"}} =
             AutoPrompt.run(
               "HeadBucket",
               %{"Bucket" => "bucket"},
               %{cli_auto_prompt_mode: "on-partial"},
               tty: false
             )

    assert {:error, %Error{code: :auto_prompt_declined}} =
             AutoPrompt.run("HeadBucket", %{}, %{cli_auto_prompt: true},
               tty: true,
               prompt_read: reader(["bucket", "no"]),
               prompt_write: fn _ -> :ok end
             )
  end

  test "registry questions preserve literal argument bytes and never evaluate shell text" do
    opts = [
      tty: true,
      prompt_read: reader(["a $(touch /tmp/must-not-exist)", "s3://bucket/key"]),
      prompt_write: fn _ -> :ok end
    ]

    assert {:ok, ["a $(touch /tmp/must-not-exist)", "s3://bucket/key"]} =
             AutoPrompt.arguments(:s3, ["cp"], [], %{cli_auto_prompt: true}, opts)
  end

  test "sensitive required model values are marked without exposing their contents" do
    assert {:error, %Error{code: :auto_prompt_declined}} =
             AutoPrompt.run(
               "WriteGetObjectResponse",
               %{"RequestRoute" => "route"},
               %{cli_auto_prompt: true},
               tty: true,
               prompt_read: reader(["sensitive-value", "no"]),
               prompt_write: fn _ -> :ok end
             )

    assert_receive {:question, %{label: "RequestToken", sensitive: true}}
  end

  test "prompt input is bounded and help/skeleton never need a terminal" do
    assert {:error, %Error{code: :auto_prompt_input}} =
             AutoPrompt.run("HeadBucket", %{}, %{cli_auto_prompt: true},
               tty: true,
               prompt_read: reader([String.duplicate("a", 65_537)]),
               prompt_write: fn _ -> :ok end
             )

    assert {:ok, "off"} = AutoPrompt.mode(%{help: true, cli_auto_prompt: true})

    assert {:ok, "off"} =
             AutoPrompt.mode(%{generate_cli_skeleton: "input", cli_auto_prompt: true})
  end
end
