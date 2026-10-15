defmodule ExStorageServiceCli.CLI.ManagementConvenienceTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.CLI.{Invocation, Management, Parser}
  alias ExStorageServiceCli.S3.Encoder

  test "lifecycle grammar accepts portable actions and rejects MinIO extensions" do
    for action <- ~w(add edit list remove export import) do
      args = if action == "edit", do: ["--id", "selected"], else: []

      assert {:ok, %{command: ["ilm", "rule", ^action]}} =
               Parser.parse(["ilm", "rule", action, "s3://bucket" | args])
    end

    for flag <- ~w(expire-all-object-versions purge-all-object-versions) do
      assert {:error, %Error{}} =
               Parser.parse(["ilm", "rule", "add", "s3://bucket", "--" <> flag])
    end
  end

  test "lifecycle add preserves complete rules and transition default while building all portable fields" do
    existing = %{
      "Rules" => [existing_rule()],
      "TransitionDefaultMinimumObjectSize" => "varies_by_storage_class"
    }

    call = capture(existing, "GetBucketLifecycleConfiguration", "PutBucketLifecycleConfiguration")

    options = %{
      prefix: "docs/",
      tags: "team=ops&empty=",
      size_gt: "1MiB",
      size_lt: "2 MB",
      expire_days: 90,
      transition_days: 30,
      transition_tier: "STANDARD_IA",
      noncurrent_expire_days: 60,
      noncurrent_expire_newer: 2,
      noncurrent_transition_days: 30,
      noncurrent_transition_tier: "GLACIER"
    }

    assert {:ok, _} = run(["ilm", "rule", "add"], ["s3://bucket"], options, call)
    assert_receive {:written, input}
    assert input["TransitionDefaultMinimumObjectSize"] == "varies_by_storage_class"
    assert [preserved, added] = input["LifecycleConfiguration"]["Rules"]
    assert preserved == existing_rule()
    assert is_binary(added["ID"]) and byte_size(added["ID"]) > 0
    assert added["Status"] == "Enabled"

    assert added["Filter"] == %{
             "And" => %{
               "Prefix" => "docs/",
               "Tags" => [
                 %{"Key" => "team", "Value" => "ops"},
                 %{"Key" => "empty", "Value" => ""}
               ],
               "ObjectSizeGreaterThan" => 1_048_576,
               "ObjectSizeLessThan" => 2_000_000
             }
           }

    assert added["Expiration"] == %{"Days" => 90}
    assert added["Transitions"] == [%{"Days" => 30, "StorageClass" => "STANDARD_IA"}]

    assert added["NoncurrentVersionExpiration"] == %{
             "NoncurrentDays" => 60,
             "NewerNoncurrentVersions" => 2
           }

    assert added["NoncurrentVersionTransitions"] == [
             %{"NoncurrentDays" => 30, "StorageClass" => "GLACIER"}
           ]
  end

  test "add treats only NoSuchLifecycleConfiguration as empty" do
    for code <- ["AccessDenied", "NoSuchBucket"] do
      error = Error.new(:service, "unavailable", code: code)
      call = fn "GetBucketLifecycleConfiguration", _ -> {:error, error} end

      assert {:error, ^error} =
               run(["ilm", "rule", "add"], ["s3://bucket"], %{expire_days: 10}, call)
    end

    call = fn
      "GetBucketLifecycleConfiguration", _ ->
        {:error, Error.new(:service, "absent", code: "NoSuchLifecycleConfiguration")}

      "PutBucketLifecycleConfiguration", input ->
        assert [rule] = input["LifecycleConfiguration"]["Rules"]
        assert rule["Expiration"] == %{"ExpiredObjectDeleteMarker" => true}
        {:ok, %{}}
    end

    assert {:ok, %{}} =
             run(["ilm", "rule", "add"], ["s3://bucket"], %{expire_delete_marker: true}, call)
  end

  test "lifecycle edit changes only selected fields and preserves other actions and rules" do
    original = existing_rule()

    call =
      capture(
        %{
          "Rules" => [
            original,
            %{"ID" => "other", "Status" => "Disabled", "Expiration" => %{"Days" => 99}}
          ]
        },
        "GetBucketLifecycleConfiguration",
        "PutBucketLifecycleConfiguration"
      )

    assert {:ok, _} =
             run(
               ["ilm", "rule", "edit"],
               ["s3://bucket"],
               %{id: "selected", disable: true, prefix: "new/", expire_days: 40},
               call
             )

    assert_receive {:written, input}
    assert [edited, other] = input["LifecycleConfiguration"]["Rules"]

    assert edited ==
             original
             |> Map.put("Status", "Disabled")
             |> Map.put("Expiration", %{"Days" => 40})
             |> Map.put("Filter", %{
               "And" => %{
                 "Prefix" => "new/",
                 "Tags" => [%{"Key" => "team", "Value" => "old"}],
                 "ObjectSizeGreaterThan" => 1
               }
             })

    assert other["ID"] == "other"
    assert other["Expiration"] == %{"Days" => 99}
  end

  test "lifecycle mutation fails closed for unknown fields, ambiguous IDs and invalid flags" do
    no_call = fn _, _ -> flunk("Invalid options must fail before I/O") end

    for options <- [
          %{},
          %{expire_days: -1},
          %{expire_days: 0},
          %{expire_days: 10, expire_delete_marker: true},
          %{transition_days: 1},
          %{transition_tier: "MINIOTIER-1", transition_days: 1},
          %{size_gt: "2MiB", size_lt: "1MiB", expire_days: 1},
          %{tags: "x=1&x=2", expire_days: 1},
          %{noncurrent_expire_newer: 2},
          %{enable: true, disable: true, expire_days: 1}
        ] do
      assert {:error, %Error{}} = run(["ilm", "rule", "add"], ["s3://bucket"], options, no_call)
    end

    for rules <- [
          [Map.put(existing_rule(), "ProviderExtension", true)],
          [existing_rule(), existing_rule()]
        ] do
      call = fn "GetBucketLifecycleConfiguration", _ -> {:ok, %{"Rules" => rules}} end

      assert {:error, %Error{}} =
               run(
                 ["ilm", "rule", "edit"],
                 ["s3://bucket"],
                 %{id: "selected", expire_days: 20},
                 call
               )
    end
  end

  test "lifecycle remove requires selected ID or explicit all force and deletes last configuration" do
    no_call = fn _, _ -> flunk("Invalid delete must fail before I/O") end

    for options <- [%{}, %{all: true}, %{force: true}, %{id: "selected", all: true, force: true}] do
      assert {:error, %Error{}} =
               run(["ilm", "rule", "remove"], ["s3://bucket"], options, no_call)
    end

    call = fn
      "GetBucketLifecycleConfiguration", _ ->
        {:ok, %{"Rules" => [existing_rule()]}}

      "DeleteBucketLifecycle", input ->
        assert input == %{"Bucket" => "bucket"}
        {:ok, %{}}
    end

    assert {:ok, %{}} = run(["ilm", "rule", "remove"], ["s3://bucket"], %{id: "selected"}, call)

    assert {:ok, %{"DryRun" => true, "Operation" => "DeleteBucketLifecycle"}} =
             run(
               ["ilm", "rule", "remove"],
               ["s3://bucket"],
               %{all: true, force: true, dry_run: true},
               no_call
             )
  end

  test "lifecycle listing projections never mutate and export preserves complete configuration" do
    document = %{"Rules" => [existing_rule()]}
    call = fn "GetBucketLifecycleConfiguration", _ -> {:ok, document} end
    assert {:ok, ^document} = run(["ilm", "rule", "export"], ["s3://bucket"], %{}, call)

    assert {:ok, %{"Rules" => [projected]}} =
             run(["ilm", "rule", "list"], ["s3://bucket"], %{expiry: true}, call)

    assert projected["Expiration"] == existing_rule()["Expiration"]
    refute Map.has_key?(projected, "Transitions")

    assert {:ok, %{"Rules" => [projected]}} =
             run(["ilm", "rule", "list"], ["s3://bucket"], %{transition: true}, call)

    refute Map.has_key?(projected, "Expiration")
    assert projected["Transitions"] == existing_rule()["Transitions"]
  end

  test "lifecycle stdin import is bounded validates and refuses an empty replacement" do
    for document <- [%{"Rules" => [existing_rule()]}, %{"Rules" => []}, %{"Unknown" => true}] do
      {:ok, io} = StringIO.open(JSON.encode!(document))

      call = fn operation, input ->
        assert operation == "PutBucketLifecycleConfiguration"
        send(self(), {:imported, input})
        {:ok, %{}}
      end

      result = run(["ilm", "rule", "import"], ["s3://bucket"], %{}, call, stdin: io)

      if document["Rules"] == [existing_rule()] do
        assert {:ok, %{}} = result
        assert_received {:imported, %{"LifecycleConfiguration" => ^document}}
      else
        assert {:error, %Error{}} = result
        refute_received {:imported, _}
      end

      StringIO.close(io)
    end
  end

  test "tag list and encoded key-value set select bucket or versioned object operations" do
    assert {:ok, %{command: ["tag", "list"]}} =
             Parser.parse(["tag", "list", "s3://bucket/key", "--version-id", "v1"])

    call = fn
      "GetObjectTagging", input ->
        assert input["VersionId"] == "v1"
        {:ok, %{"TagSet" => []}}

      "PutObjectTagging", input ->
        assert input["VersionId"] == "v1"

        assert input["Tagging"] == %{
                 "TagSet" => [
                   %{"Key" => "team name", "Value" => "a+b=c"},
                   %{"Key" => "empty", "Value" => ""}
                 ]
               }

        {:ok, %{}}
    end

    assert {:ok, _} = run(["tag", "list"], ["s3://bucket/key"], %{version_id: "v1"}, call)

    assert {:ok, _} =
             run(
               ["tag", "set"],
               ["s3://bucket/key", "team+name=a%2Bb%3Dc&empty="],
               %{version_id: "v1"},
               call
             )

    for tags <- ["a=1&a=2", "=v", "a=%xx", Enum.map_join(1..11, "&", &"k#{&1}=v")] do
      assert {:error, %Error{}} =
               run(["tag", "set"], ["s3://bucket/key", tags], %{}, fn _, _ ->
                 flunk("Invalid tags wrote")
               end)
    end
  end

  test "event add and targeted removal preserve unrelated destinations and EventBridge" do
    original = %{
      "QueueConfigurations" => [
        %{
          "Id" => "old",
          "QueueArn" => arn(),
          "Events" => ["s3:ObjectCreated:*"],
          "Filter" => %{"Key" => %{"FilterRules" => [%{"Name" => "prefix", "Value" => "old/"}]}}
        }
      ],
      "EventBridgeConfiguration" => %{}
    }

    call =
      capture(
        original,
        "GetBucketNotificationConfiguration",
        "PutBucketNotificationConfiguration"
      )

    assert {:ok, _} =
             run(
               ["event", "add"],
               ["s3://bucket", arn()],
               %{event: "put,delete", prefix: "new/", suffix: ".jpg"},
               call
             )

    assert_receive {:written, input}
    assert input["NotificationConfiguration"]["EventBridgeConfiguration"] == %{}
    assert [kept, added] = input["NotificationConfiguration"]["QueueConfigurations"]
    assert kept == hd(original["QueueConfigurations"])
    assert added["Events"] == ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]

    assert added["Filter"]["Key"]["FilterRules"] == [
             %{"Name" => "prefix", "Value" => "new/"},
             %{"Name" => "suffix", "Value" => ".jpg"}
           ]

    updated = input["NotificationConfiguration"]

    call =
      capture(updated, "GetBucketNotificationConfiguration", "PutBucketNotificationConfiguration")

    assert {:ok, _} =
             run(
               ["event", "remove"],
               ["s3://bucket", arn()],
               %{event: "put,delete", prefix: "new/", suffix: ".jpg"},
               call
             )

    assert_receive {:written, input}
    assert input["NotificationConfiguration"] == original
  end

  test "event list optional ARN filters only matching entries without writes" do
    doc = %{
      "QueueConfigurations" => [
        %{"Id" => "one", "QueueArn" => arn(), "Events" => ["s3:ObjectCreated:*"]}
      ],
      "TopicConfigurations" => [
        %{
          "TopicArn" => "arn:aws:sns:us-east-1:123456789012:other",
          "Events" => ["s3:ObjectCreated:*"]
        }
      ],
      "EventBridgeConfiguration" => %{}
    }

    call = fn "GetBucketNotificationConfiguration", _ -> {:ok, doc} end
    assert {:ok, ^doc} = run(["event", "list"], ["s3://bucket"], %{}, call)

    assert {:ok, %{"QueueConfigurations" => [_], "TopicConfigurations" => []}} =
             run(["event", "list"], ["s3://bucket", arn()], %{}, call)
  end

  test "event options reject unsupported defaults extensions and malformed ARN before I/O" do
    for {destination, options} <- [
          {arn(), %{}},
          {arn(), %{event: "get"}},
          {arn(), %{event: "ilm"}},
          {"http://unsafe", %{event: "put"}},
          {arn(), %{event: "scanner"}}
        ] do
      assert {:error, %Error{}} =
               run(["event", "add"], ["s3://bucket", destination], options, fn _, _ ->
                 flunk("Invalid event reached I/O")
               end)
    end
  end

  test "event duplicate ignore is exact and permission failures never imply empty config" do
    entry = %{"QueueArn" => arn(), "Events" => ["s3:ObjectCreated:*"]}

    call = fn "GetBucketNotificationConfiguration", _ ->
      {:ok, %{"QueueConfigurations" => [entry]}}
    end

    assert {:ok, %{"Unchanged" => true}} =
             run(
               ["event", "add"],
               ["s3://bucket", arn()],
               %{event: "put", ignore_existing: true},
               call
             )

    assert {:error, %Error{}} =
             run(["event", "add"], ["s3://bucket", arn()], %{event: "put"}, call)

    denied = Error.new(:service, "denied", code: "AccessDenied")
    call = fn "GetBucketNotificationConfiguration", _ -> {:error, denied} end

    assert {:error, ^denied} =
             run(["event", "add"], ["s3://bucket", arn()], %{event: "put"}, call)
  end

  test "all accepted lifecycle flags parse through the real registry to typed effects" do
    assert {:ok, %{options: options}} =
             Parser.parse([
               "ilm",
               "rule",
               "add",
               "s3://bucket",
               "--prefix",
               "docs/",
               "--tags",
               "team=ops",
               "--size-lt",
               "2MiB",
               "--size-gt",
               "1KiB",
               "--expire-days",
               "90",
               "--transition-days",
               "30",
               "--transition-tier",
               "STANDARD_IA",
               "--noncurrent-expire-days",
               "60",
               "--noncurrent-expire-newer",
               "2",
               "--noncurrent-transition-days",
               "30",
               "--noncurrent-transition-tier",
               "GLACIER",
               "--dry-run"
             ])

    assert options[:expire_days] == 90
    assert options[:noncurrent_transition_days] == 30
    assert options[:dry_run]
  end

  test "lifecycle edit refuses to choose one of multiple transitions" do
    rule =
      Map.put(existing_rule(), "Transitions", [
        %{"Days" => 10, "StorageClass" => "GLACIER"},
        %{"Days" => 30, "StorageClass" => "DEEP_ARCHIVE"}
      ])

    call = fn "GetBucketLifecycleConfiguration", _ -> {:ok, %{"Rules" => [rule]}} end

    assert {:error, %Error{message: message}} =
             run(
               ["ilm", "rule", "edit"],
               ["s3://bucket"],
               %{id: "selected", transition_days: 20},
               call
             )

    assert message =~ "Multiple transitions"
  end

  test "lifecycle dry run validates complete mutation without writing and selected removal preserves peers" do
    other = Map.put(existing_rule(), "ID", "other")

    call =
      capture(
        %{"Rules" => [existing_rule(), other]},
        "GetBucketLifecycleConfiguration",
        "PutBucketLifecycleConfiguration"
      )

    assert {:ok, %{"DryRun" => true, "Input" => input}} =
             run(
               ["ilm", "rule", "edit"],
               ["s3://bucket"],
               %{id: "selected", enable: true, dry_run: true},
               call
             )

    assert input["LifecycleConfiguration"]["Rules"] == [existing_rule(), other]
    refute_received {:written, _}
    assert {:ok, _} = run(["ilm", "rule", "remove"], ["s3://bucket"], %{id: "selected"}, call)
    assert_received {:written, %{"LifecycleConfiguration" => %{"Rules" => [^other]}}}
  end

  test "event replica is portable while unknown fields and ambiguous destructive selectors fail closed" do
    call =
      capture(
        %{"EventBridgeConfiguration" => %{}},
        "GetBucketNotificationConfiguration",
        "PutBucketNotificationConfiguration"
      )

    assert {:ok, _} = run(["event", "add"], ["s3://bucket", arn()], %{event: "replica"}, call)

    assert_received {:written,
                     %{
                       "NotificationConfiguration" => %{
                         "QueueConfigurations" => [%{"Events" => ["s3:Replication:*"]}]
                       }
                     }}

    call = fn "GetBucketNotificationConfiguration", _ -> {:ok, %{"ProviderExtension" => true}} end

    assert {:error, %Error{}} =
             run(["event", "add"], ["s3://bucket", arn()], %{event: "put"}, call)

    no_call = fn _, _ -> flunk("Conflicting selectors must not reach I/O") end

    assert {:error, %Error{}} =
             run(["event", "remove"], ["s3://bucket", arn()], %{force: true}, no_call)

    assert {:error, %Error{}} =
             run(["event", "remove"], ["s3://bucket"], %{prefix: "selected/"}, no_call)

    assert {:error, %Error{}} =
             run(["tag", "list"], ["s3://bucket"], %{version_id: "v1"}, no_call)
  end

  test "notification ARN shapes route SNS Lambda and queue to their actual API fields" do
    for {destination, collection, field} <- [
          {"arn:aws:sns:us-east-1:123456789012:topic", "TopicConfigurations", "TopicArn"},
          {"arn:aws:lambda:us-east-1:123456789012:function:handler",
           "LambdaFunctionConfigurations", "LambdaFunctionArn"},
          {arn(), "QueueConfigurations", "QueueArn"}
        ] do
      call = fn "GetBucketNotificationConfiguration", _ -> {:ok, %{}} end

      assert {:ok, %{"DryRun" => true, "Input" => input}} =
               run(
                 ["event", "add"],
                 ["s3://bucket", destination],
                 %{event: "put", dry_run: true},
                 call
               )

      assert [entry] = input["NotificationConfiguration"][collection]
      assert entry[field] == destination
    end
  end

  test "oversized stdin import cannot write a configuration" do
    {:ok, io} = StringIO.open(String.duplicate(" ", 8_388_609))

    assert {:error, %Error{}} =
             run(
               ["ilm", "rule", "import"],
               ["s3://bucket"],
               %{},
               fn _, _ -> flunk("Oversized input wrote") end,
               stdin: io
             )

    StringIO.close(io)
  end

  test "lifecycle list rejects conflicting projections before I/O like the pinned client" do
    assert {:error, %Error{}} =
             run(
               ["ilm", "rule", "list"],
               ["s3://bucket"],
               %{expiry: true, transition: true},
               fn _, _ -> flunk("Conflicting projections reached I/O") end
             )
  end

  defp existing_rule,
    do: %{
      "ID" => "selected",
      "Status" => "Enabled",
      "Filter" => %{
        "And" => %{
          "Prefix" => "old/",
          "Tags" => [%{"Key" => "team", "Value" => "old"}],
          "ObjectSizeGreaterThan" => 1
        }
      },
      "Expiration" => %{"Days" => 30},
      "Transitions" => [%{"Days" => 10, "StorageClass" => "GLACIER"}],
      "AbortIncompleteMultipartUpload" => %{"DaysAfterInitiation" => 7}
    }

  defp arn, do: "arn:aws:sqs:us-east-1:123456789012:queue"

  defp capture(document, get, put),
    do: fn
      ^get, _ ->
        {:ok, document}

      ^put, input ->
        assert {:ok, _} = Encoder.encode(put, input)
        send(self(), {:written, input})
        {:ok, %{}}
    end

  defp run(command, args, options, call, opts \\ []) do
    {:ok, target} = Target.parse(hd(args))

    Management.run(
      %Invocation{command: command, args: args, options: options},
      target,
      %Context{},
      [call: call] ++ opts
    )
  end
end
