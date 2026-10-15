defmodule ExStorageServiceCli.CLI.NativeAnonymousTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.CLI.NativeAnonymous
  alias ExStorageServiceCli.{Context, Error, Target}

  test "commands expose literal file grammar and bounded link flags" do
    commands = NativeAnonymous.commands()
    assert {2..2, %{"replace" => _, "dry-run" => _}, _} = commands[["anonymous", "set-json"]]
    assert {1..1, %{"limit" => _, "recursive" => _}, _} = commands[["anonymous", "links"]]
    assert {1..1, %{}, _} = commands[["anonymous", "get-json"]]
    assert {1..1, %{}, _} = commands[["anonymous", "list"]]
  end

  test "get-json preserves complete custom policy documents including unknown fields" do
    document =
      policy([Map.put(grant("p/*"), "Condition", %{"StringEquals" => %{"x" => "y"}})])
      |> Map.put("FutureField", %{"unknown" => [1, 2]})

    assert {:ok, ^document} = run("get-json", "p/", %{}, read(document))
  end

  test "set-json passes exact complete bytes without discarding unrelated policy data" do
    bytes =
      JSON.encode!(policy([grant("p/*"), %{"Sid" => "unrelated", "Effect" => "Deny"}])) <> "\n"

    path = file(bytes)

    call = fn "PutBucketPolicy", input ->
      send(self(), {:put, input})
      {:ok, %{}}
    end

    assert {:ok, %{}} = run("set-json", "", %{replace: true}, call, [path, "fixture/bucket"])
    assert_receive {:put, %{"Bucket" => "bucket", "Policy" => ^bytes}}
  end

  test "existing policy requires explicit replacement and service failures are preserved" do
    path = file(JSON.encode!(policy([])))

    assert {:error, %Error{category: :syntax}} =
             run("set-json", "", %{}, read(policy([])), [path, "x"])

    denied = Error.new(:service, "denied", code: "AccessDenied")

    assert {:error, ^denied} =
             run("set-json", "", %{}, fn _, _ -> {:error, denied} end, [path, "x"])
  end

  test "new policy accepts only exact NoSuchBucketPolicy and dry-run never mutates" do
    path = file(JSON.encode!(policy([grant("*")])))

    call = fn "GetBucketPolicy", _ ->
      {:error, Error.new(:service, "missing", code: "NoSuchBucketPolicy")}
    end

    assert {:ok, %{"DryRun" => true, "Operation" => "PutBucketPolicy", "Input" => input}} =
             run("set-json", "", %{dry_run: true}, call, [path, "x"])

    assert JSON.decode!(input["Policy"]) == policy([grant("*")])
  end

  test "set-json rejects prefix targets, empty oversized malformed files, and stdin notation" do
    good = file(JSON.encode!(policy([])))
    no_call = fn _, _ -> flunk("invalid input must not call service") end
    assert {:error, _} = run("set-json", "p/", %{replace: true}, no_call, [good, "x"])

    for path <- [
          file(""),
          file(String.duplicate(" ", 120 * 1024 + 1)),
          file("[]"),
          file("{"),
          "file://" <> good
        ] do
      assert {:error, %Error{category: :syntax}} =
               run("set-json", "", %{replace: true}, no_call, [path, "x"])
    end
  end

  test "get-json rejects invalid provider policy shape and propagates service errors" do
    for bytes <- ["[]", "{}", "{\"Statement\":[42]}", String.duplicate("x", 120 * 1024 + 1)] do
      assert {:error, _} = run("get-json", "", %{}, fn _, _ -> {:ok, %{"Policy" => bytes}} end)
    end

    error = Error.new(:service, "missing", code: "NoSuchBucketPolicy")
    assert {:error, ^error} = run("get-json", "", %{}, fn _, _ -> {:error, error} end)
  end

  test "list combines exact simple object grants and filters related prefixes" do
    document =
      policy([grant("p/*"), grant("p/*", "s3:PutObject"), grant("other/*"), grant("p/exact")])

    assert {:ok,
            %{"Rules" => rules, "UnclassifiedStatements" => [], "PublicAccessVerified" => false}} =
             run("list", "p/", %{}, read(document))

    assert rules == [
             %{"resource" => "bucket/p/*", "allow" => "readwrite"},
             %{"resource" => "bucket/p/exact", "allow" => "readonly"}
           ]
  end

  test "conditions denies negation principals actions and complex resources stay unclassified" do
    complex = [
      Map.put(grant("p/*"), "Condition", %{}),
      Map.put(grant("p/*"), "Effect", "Deny"),
      Map.put(grant("p/*"), "NotAction", "s3:DeleteObject"),
      Map.put(grant("p/*"), "Principal", %{"AWS" => "arn:aws:iam::123:user/a"}),
      grant("p/*", "s3:*"),
      grant("p/**"),
      grant("p/?"),
      grant("${aws:username}/*")
    ]

    document = policy([grant("p/*") | complex])
    assert {:ok, %{"UnclassifiedStatements" => indices}} = run("list", "", %{}, read(document))
    assert indices == Enum.to_list(2..9)
    assert {:error, %Error{category: :syntax}} = run("links", "", %{}, read(document))
  end

  test "links paginate fully, encode raw unsigned URLs, bound output, and clean manifests" do
    root = tmp()

    call = fn
      "GetBucketPolicy", _ ->
        {:ok, %{"Policy" => JSON.encode!(policy([grant("p/*")]))}}

      "ListObjectsV2", %{"Bucket" => "bucket", "Prefix" => "p/", "ContinuationToken" => "next"} ->
        {:ok, %{"Contents" => [%{"Key" => "p/z"}], "IsTruncated" => false}}

      "ListObjectsV2", input ->
        refute Map.has_key?(input, "Delimiter")

        {:ok,
         %{
           "Contents" => [%{"Key" => "p/é #?"}, %{"Key" => "private/no"}],
           "IsTruncated" => true,
           "NextContinuationToken" => "next"
         }}
    end

    # A provider key outside the requested prefix is rejected, not included or silently omitted.
    assert {:error, _} = run("links", "p/", %{recursive: true, limit: 1}, call, [], tmp_dir: root)
    assert File.ls!(root) == []

    clean = fn op, input ->
      case call.(op, input) do
        {:ok, %{"Contents" => entries} = page} ->
          {:ok, Map.put(page, "Contents", Enum.reject(entries, &(&1["Key"] == "private/no")))}

        other ->
          other
      end
    end

    assert {:ok, result} =
             run("links", "p/", %{recursive: true, limit: 1}, clean, [], tmp_dir: root)

    assert result["Links"] == ["http://localhost:9000/base/bucket/p/z"]
    assert result["MatchedObjects"] == 2 and result["Returned"] == 1 and result["Partial"]
    assert result["PolicyOnly"] and result["PublicAccessVerified"] == false
    assert File.ls!(root) == []
    assert {:ok, all} = run("links", "p/", %{recursive: true}, clean)
    assert "http://localhost:9000/base/bucket/p/%C3%A9%20%23%3F" in all["Links"]
    refute Enum.any?(all["Links"], &String.contains?(&1, "X-Amz-"))
  end

  test "nonrecursive links use delimiter and preserve directory URLs under covered grants" do
    call = fn
      "GetBucketPolicy", _ ->
        {:ok, %{"Policy" => JSON.encode!(policy([grant("p/*")]))}}

      "ListObjectsV2", %{"Delimiter" => "/"} ->
        {:ok,
         %{
           "Contents" => [%{"Key" => "p/one"}],
           "CommonPrefixes" => [%{"Prefix" => "p/sub/"}],
           "IsTruncated" => false
         }}
    end

    assert {:ok, %{"Links" => [a, b]}} = run("links", "p/", %{}, call)
    assert String.ends_with?(a, "/p/one") and String.ends_with?(b, "/p/sub/")
  end

  test "later page errors fail the complete links inventory even after result limit" do
    root = tmp()
    error = Error.new(:service, "denied", code: "AccessDenied")

    call = fn
      "GetBucketPolicy", _ ->
        {:ok, %{"Policy" => JSON.encode!(policy([grant("*")]))}}

      "ListObjectsV2", %{"ContinuationToken" => _} ->
        {:error, error}

      "ListObjectsV2", _ ->
        {:ok,
         %{
           "Contents" => [%{"Key" => "p/one"}],
           "IsTruncated" => true,
           "NextContinuationToken" => "next"
         }}
    end

    assert {:error, ^error} = run("links", "", %{limit: 0}, call, [], tmp_dir: root)
    assert File.ls!(root) == []
  end

  test "write-only and empty policies do not list objects and limits are validated" do
    assert {:ok, %{"Links" => [], "MatchedObjects" => 0}} =
             run("links", "", %{}, read(policy([grant("*", "s3:PutObject")])))

    for limit <- [-1, 10001, "1"] do
      assert {:error, _} = run("links", "", %{limit: limit}, read(policy([])))
    end

    assert {:error, _} =
             NativeAnonymous.run("get-json", [], %Target{kind: :local}, %Context{}, %{}, fn _,
                                                                                            _ ->
               flunk("local")
             end)
  end

  defp run(action, key, options, call, args \\ [], opts \\ []) do
    NativeAnonymous.run(
      action,
      args,
      %Target{kind: :remote, bucket: "bucket", key: key},
      %Context{endpoint: "http://localhost:9000/base", unsigned: true},
      options,
      call,
      opts
    )
  end

  defp read(document),
    do: fn "GetBucketPolicy", %{"Bucket" => "bucket"} ->
      {:ok, %{"Policy" => JSON.encode!(document)}}
    end

  defp policy(statements), do: %{"Version" => "2012-10-17", "Statement" => statements}

  defp grant(key, action \\ "s3:GetObject"),
    do: %{
      "Effect" => "Allow",
      "Principal" => "*",
      "Action" => action,
      "Resource" => "arn:aws:s3:::bucket/" <> key
    }

  defp tmp do
    path =
      Path.join(
        System.tmp_dir!(),
        "ess-anonymous-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      )

    File.mkdir!(path)
    on_exit(fn -> File.rm_rf(path) end)
    path
  end

  defp file(bytes) do
    path = Path.join(tmp(), "policy.json")
    File.write!(path, bytes)
    path
  end
end
