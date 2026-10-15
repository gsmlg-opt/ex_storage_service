defmodule ExStorageServiceCli.CLI.CompletionTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, CLI.Completion}
  @moduletag :tmp_dir

  setup %{tmp_dir: root} do
    scripts =
      for shell <- ~w(bash zsh fish), into: %{} do
        assert {:ok, script} = Completion.generate(shell)
        path = Path.join(root, "ess." <> shell)
        File.write!(path, script)
        {shell, path}
      end

    %{scripts: scripts}
  end

  test "unsupported shell is a typed syntax error" do
    assert {:error, %Error{category: :syntax}} = Completion.generate("unknown")
  end

  test "installed bash and zsh parse generated scripts and complete nested namespaces", context do
    for shell <- ~w(bash zsh) do
      assert {_, 0} = System.cmd(shell, ["-n", context.scripts[shell]], stderr_to_stdout: true)
      assert "s3api" in candidates(context, shell, "s3", [])
      assert candidates(context, shell, "l", ["alias"]) == ["list"]
      assert candidates(context, shell, "e", ["version"]) == ["enable"]
      assert candidates(context, shell, "res", ["transfer"]) == ["resume"]
      assert candidates(context, shell, "cp", ["s3"]) == ["cp"]
      assert candidates(context, shell, "wai", ["s3api"]) == ["wait"]
      assert candidates(context, shell, "bucket-e", ["s3api", "wait"]) == ["bucket-exists"]
    end
  end

  test "options follow command applicability and remain available after positional values",
       context do
    for shell <- ~w(bash zsh) do
      assert "--storage-class" in candidates(context, shell, "--sto", ["s3", "cp", "from", "to"])
      refute "--storage-class" in candidates(context, shell, "--sto", ["s3", "ls"])

      assert "--cli-input-json" in candidates(context, shell, "--cli-input", [
               "s3api",
               "put-object"
             ])

      assert "--bucket" in candidates(context, shell, "--buck", ["s3api", "wait", "bucket-exists"])

      assert "--no-bucket-key-enabled" in candidates(context, shell, "--no-bucket", [
               "s3api",
               "put-object"
             ])

      assert candidates(context, shell, "--", ["s3", "cp", "--", "literal"]) == []
    end
  end

  test "option values cannot become commands and enumerated values stay literal", context do
    for shell <- ~w(bash zsh) do
      assert candidates(context, shell, "l", ["--profile", "s3api", "alias"]) == ["list"]
      assert candidates(context, shell, "l", ["--profile=s3api", "alias"]) == ["list"]
      assert candidates(context, shell, "s", ["s3", "--profile"]) == []

      assert candidates(context, shell, "G", ["s3", "cp", "--storage-class"]) == [
               "GLACIER",
               "GLACIER_IR"
             ]

      assert candidates(context, shell, "--storage-class=G", ["s3", "cp"]) == [
               "--storage-class=GLACIER",
               "--storage-class=GLACIER_IR"
             ]

      assert "--bucket" in candidates(context, shell, "--buck", [
               "s3api",
               "put-object",
               "--generate-cli-skeleton"
             ])

      marker = Path.join(context.tmp_dir, shell <> "-must-not-exist")
      assert candidates(context, shell, "$(touch #{marker})", ["s3api"]) == []
      refute File.exists?(marker)
    end
  end

  test "bash registration consumes actual COMP_WORDS and fills COMPREPLY", context do
    code =
      "source \"$1\"; COMP_WORDS=(ess s3api wait bucket-exists --buck); COMP_CWORD=4; _ess_complete; printf '%s\\n' \"${COMPREPLY[@]}\""

    assert {output, 0} =
             System.cmd("bash", [
               "--noprofile",
               "--norc",
               "-c",
               code,
               "ess-test",
               context.scripts["bash"]
             ])

    assert "--bucket" in String.split(output, "\n", trim: true)
  end

  test "zsh registration consumes actual words and CURRENT without evaluating values", context do
    code =
      "compdef() { :; }; compadd() { shift; printf '%s\\n' \"$@\"; }; source \"$1\"; words=(ess s3api wait bucket-exists --buck); CURRENT=5; _ess_complete"

    assert {output, 0} = System.cmd("zsh", ["-f", "-c", code, "ess-test", context.scripts["zsh"]])
    assert "--bucket" in String.split(output, "\n", trim: true)
  end

  test "fish uses contextual registry metadata and never invokes ess or credentials", context do
    script = File.read!(context.scripts["fish"])
    assert script =~ "function __ess_candidates"
    assert script =~ "s3api/wait/bucket-exists:--bucket"
    assert script =~ "native/alias:list"
    refute script =~ "eval "
    refute script =~ "ess --"
  end

  defp candidates(context, shell, current, words) do
    prefix = if shell == "zsh", do: "compdef() { :; }; ", else: ""
    code = prefix <> "source \"$1\"; shift; __ess_candidates \"$@\""
    arguments = if shell == "zsh", do: ["-f"], else: ["--noprofile", "--norc"]

    assert {output, 0} =
             System.cmd(
               shell,
               arguments ++ ["-c", code, "ess-test", context.scripts[shell], current | words],
               stderr_to_stdout: true
             )

    String.split(output, "\n", trim: true)
  end
end
