defmodule ExStorageServiceCli.CLI.NativeEventsTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.CLI.{Invocation, NativeEvents, NativeListing, Parser, Runner}

  test "native NDJSON emits a versioned item and final summary; AWS parser rejects it" do
    output = capture_io(fn -> assert :ok = Runner.run(["version", "--output", "ndjson"]) end)
    [item, summary] = records(output)
    assert item["schema_version"] == 1
    assert item["type"] == "item"
    assert item["operation"] == "version"
    assert item["data"]["version"] == ExStorageServiceCli.version()
    assert summary["type"] == "summary"
    assert summary["items"] == 1
    assert summary["status"] == "ok"
    assert {:error, %Error{category: :syntax}} = Parser.parse(["s3", "ls", "--output", "ndjson"])
  end

  test "native listing streams limited Unicode items and explicitly reports partial results" do
    invocation = %Invocation{
      command: ["ls"],
      args: ["s3://bucket/"],
      options: %{output: "ndjson", limit: 1}
    }

    target = %Target{kind: :remote, bucket: "bucket", key: ""}

    fetch = fn "ListObjectsV2", _ ->
      {:ok,
       %{
         "Contents" => [%{"Key" => "雪", "Size" => 3}, %{"Key" => "other", "Size" => 9}],
         "IsTruncated" => false
       }}
    end

    output =
      capture_io(fn ->
        assert :ok =
                 NativeEvents.with_invocation(invocation, fn invocation ->
                   NativeListing.run(invocation, target, %Context{}, fetch: fetch)
                 end)
      end)

    [item, limit, summary] = records(output)
    assert item["data"] == %{"Key" => "雪", "Size" => 3}
    assert limit["data"] == %{"Partial" => true, "Returned" => 1}
    assert summary["bytes"] == 3
    assert summary["status"] == "ok"
  end

  test "incomplete enumeration emits a typed error and failed summary" do
    invocation = %Invocation{
      command: ["tree"],
      args: ["s3://bucket/"],
      options: %{output: "ndjson"}
    }

    fetch = fn "ListObjectsV2", _ ->
      {:error, Error.new(:service, "Denied", code: "AccessDenied")}
    end

    output =
      capture_io(fn ->
        assert {:error, %Error{code: "AccessDenied"}} =
                 NativeEvents.with_invocation(invocation, fn invocation ->
                   NativeListing.run(
                     invocation,
                     %Target{kind: :remote, bucket: "bucket", key: ""},
                     %Context{},
                     fetch: fetch
                   )
                 end)
      end)

    [error, summary] = records(output)
    assert error["type"] == "error"
    assert error["error"]["code"] == "AccessDenied"
    assert summary["status"] == "error"
    assert summary["items"] == 0
  end

  defp records(output), do: output |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)
end
