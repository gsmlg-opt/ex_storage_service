defmodule ExStorageServiceCli.TargetTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, Target}

  test "preserves key bytes, slash grammar and ARN resource boundaries" do
    key = "a//../%2F+ ?#雪/"

    assert {:ok, %Target{bucket: "bucket", key: ^key, trailing_slash: true}} =
             Target.parse("s3://bucket/" <> key)

    arn = "arn:aws:s3:us-east-1:123456789012:accesspoint/example"

    assert {:ok, %Target{bucket: ^arn, arn: ^arn, key: "a//b"}} =
             Target.parse("s3://" <> arn <> "/a//b")

    assert {:ok, %Target{bucket: ^arn, arn: ^arn, key: "a//b"}} =
             Target.parse(arn <> "/a//b")
  end

  test "explicit aliases resolve independently and collisions fail closed" do
    assert {:ok, %Target{alias: "prod", bucket: "bucket", key: "a//b"}} =
             Target.parse("ess://prod/bucket/a//b", aliases: ["prod"])

    assert {:error, %Error{category: :syntax}} =
             Target.parse("prod/bucket", aliases: ["prod"], local_exists?: fn _ -> true end)

    assert {:ok, %Target{kind: :local, path: "./prod/bucket"}} =
             Target.parse("./prod/bucket", aliases: ["prod"])

    assert {:error, %Error{}} = Target.parse("ess://unknown/bucket", aliases: ["prod"])
  end

  test "preserves platform paths and requires opt-in raw stream targets" do
    for path <- ["/tmp/a", "../a", "C:\\data\\a", "\\\\server\\share\\a"] do
      assert {:ok, %Target{kind: :local, path: ^path}} = Target.parse(path)
    end

    assert {:ok, %Target{path: "/tmp/a"}} = Target.parse("file:///tmp/a")
    assert {:error, %Error{}} = Target.parse("-")
    assert {:ok, %Target{kind: :stdio}} = Target.parse("-", stdio: true)
  end

  test "only explicit legacy mode accepts bare bucket and prefix" do
    assert {:ok, %Target{kind: :local}} = Target.parse("bucket/prefix")

    assert {:ok, %Target{kind: :remote, bucket: "bucket", key: "prefix"}} =
             Target.parse("bucket/prefix", legacy: true)

    assert {:error, %Error{}} = Target.parse("s3:///key")
    assert {:error, %Error{}} = Target.parse("")
  end
end
