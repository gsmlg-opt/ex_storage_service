defmodule ExStorageServiceCli.CLI.NativePolicyFlagsTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, Parser}

  test "native mirror and diff reject AWS-only policy switches in the parser" do
    for {command, flags} <- [
          {"mirror", ~w(size-only exact-timestamps delete)},
          {"diff", ["delete"]}
        ],
        flag <- flags do
      assert {:error, %Error{category: :syntax}} =
               Parser.parse([command, "s3://source/prefix/", "s3://destination/", "--" <> flag])
    end
  end

  test "direct invocations reject inert policy switches before inventory I/O" do
    source = %Target{kind: :remote, bucket: "source", key: "prefix/"}
    destination = %Target{kind: :remote, bucket: "destination", key: ""}
    context = %Context{unsigned: true}

    for {command, keys, code} <- [
          {"mirror", [:size_only, :exact_timestamps, :delete], :mirror_option},
          {"diff", [:delete], :diff_option}
        ],
        key <- keys,
        value <- [true, false] do
      invocation = %Invocation{command: [command], options: %{key => value}}

      assert {:error, %Error{code: ^code}} =
               HighLevel.run(invocation, [source, destination], [context, context],
                 fetch: fn _, _ -> flunk("Rejected policy must not enumerate objects") end,
                 call: fn _, _ -> flunk("Rejected policy must not mutate objects") end
               )
    end
  end

  test "AWS sync retains its size, timestamp and delete policy grammar" do
    assert {:ok,
            %Invocation{
              frontend: :s3,
              options: %{size_only: true, exact_timestamps: true, delete: true}
            }} =
             Parser.parse([
               "s3",
               "sync",
               "s3://source/",
               "s3://destination/",
               "--size-only",
               "--exact-timestamps",
               "--delete"
             ])
  end
end
