defmodule ExStorageServiceCli.CLI.NativeReplicationTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.CLI.NativeReplication
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.S3.Encoder
  @role "arn:aws:iam::123456789012:role/replication"
  @destination "arn:aws:s3:::destination"

  test "command entries expose the bounded portable grammar and pinned aliases" do
    commands = NativeReplication.commands()

    for action <- ~w(add update edit list ls import export remove rm) do
      assert {1..1, flags, _} = commands[["replicate", action]]
      refute Map.has_key?(flags, "bandwidth")
      refute Map.has_key?(flags, "healthcheck-seconds")
      refute Map.has_key?(flags, "sync")
      refute Map.has_key?(flags, "proxy")
      refute Map.has_key?(flags, "disable-proxy")
      refute Map.has_key?(flags, "path")
    end

    assert {_, %{"role" => {:role, :string}, "remote-bucket" => {:remote_bucket, :string}}, _} =
             commands[["replicate", "add"]]
  end

  test "add appends a portable rule while preserving every existing modeled field" do
    call = capture(document())

    options = %{
      id: "new",
      priority: 3,
      remote_bucket: @destination,
      replicate: "metadata-sync",
      tags: "team=ops&empty=",
      storage_class: "STANDARD",
      disable: true
    }

    assert {:ok, %{}} = run("add", "logs/", options, call)
    assert_receive {:written, "PutBucketReplication", input}
    assert input["ReplicationConfiguration"]["Role"] == @role
    assert [preserved, added] = input["ReplicationConfiguration"]["Rules"]
    assert preserved == rule()

    assert added == %{
             "ID" => "new",
             "Priority" => 3,
             "Status" => "Disabled",
             "Destination" => %{"Bucket" => @destination, "StorageClass" => "STANDARD"},
             "DeleteMarkerReplication" => %{"Status" => "Disabled"},
             "SourceSelectionCriteria" => %{"ReplicaModifications" => %{"Status" => "Enabled"}},
             "Filter" => %{
               "And" => %{
                 "Prefix" => "logs/",
                 "Tags" => [
                   %{"Key" => "team", "Value" => "ops"},
                   %{"Key" => "empty", "Value" => ""}
                 ]
               }
             }
           }

    assert {:ok, %{body: body}} = Encoder.encode("PutBucketReplication", input)
    assert body =~ "<Role>#{@role}</Role>"
    assert body =~ "<ReplicationTime>"
    assert body =~ "<ReplicaModifications><Status>Enabled</Status></ReplicaModifications>"
  end

  test "first add requires an explicit role and accepts only configuration absence as empty" do
    options = %{remote_bucket: @destination, priority: 0, replicate: ""}

    absent = fn
      "GetBucketReplication", _ ->
        {:error, Error.new(:service, "absent", code: "ReplicationConfigurationNotFoundError")}

      "PutBucketReplication", input ->
        send(self(), {:first, input})
        {:ok, %{}}
    end

    assert {:error, %Error{category: :syntax}} = run("add", "", options, absent)
    assert {:ok, %{}} = run("add", "", Map.put(options, :role, @role), absent)

    assert_receive {:first,
                    %{"ReplicationConfiguration" => %{"Role" => @role, "Rules" => [created]}}}

    assert is_binary(created["ID"]) and created["ID"] != ""
    assert created["Priority"] == 0
    assert created["DeleteMarkerReplication"] == %{"Status" => "Disabled"}

    for code <- ["AccessDenied", "NoSuchBucket", "NotImplemented"] do
      error = Error.new(:service, "unavailable", code: code)
      call = fn "GetBucketReplication", _ -> {:error, error} end
      assert {:error, ^error} = run("add", "", Map.put(options, :role, @role), call)
    end
  end

  test "update changes only selected fields and preserves all other nested fields and rules" do
    other = rule() |> Map.put("ID", "other") |> Map.put("Priority", 5)
    original = Map.put(document(), "Rules", [rule(), other])
    call = capture(original)

    assert {:ok, %{}} =
             run(
               "update",
               "",
               %{
                 id: "selected",
                 state: "disable",
                 priority: 2,
                 storage_class: "REDUCED_REDUNDANCY"
               },
               call
             )

    assert_receive {:written, "PutBucketReplication", input}
    assert [updated, ^other] = input["ReplicationConfiguration"]["Rules"]

    assert updated ==
             rule()
             |> Map.put("Status", "Disabled")
             |> Map.put("Priority", 2)
             |> put_in(["Destination", "StorageClass"], "REDUCED_REDUNDANCY")
  end

  test "tag and prefix updates preserve independent filter predicates and support clearing tags" do
    call = capture(document())
    assert {:ok, %{}} = run("update", "only-prefix/", %{id: "selected"}, call)
    assert_receive {:written, _, input}
    assert [prefix_only] = input["ReplicationConfiguration"]["Rules"]

    assert prefix_only["Filter"] == %{
             "And" => %{
               "Prefix" => "only-prefix/",
               "Tags" => [%{"Key" => "team", "Value" => "old"}]
             }
           }

    assert {:ok, %{}} = run("edit", "new-prefix/", %{id: "selected", tags: ""}, call)
    assert_receive {:written, _, input}
    assert [updated] = input["ReplicationConfiguration"]["Rules"]
    assert updated["Filter"] == %{"Prefix" => "new-prefix/"}
    assert updated["Destination"] == rule()["Destination"]

    assert {:ok, %{}} = run("update", "", %{id: "selected", tags: "new=value"}, call)
    assert_receive {:written, _, input}
    assert [updated] = input["ReplicationConfiguration"]["Rules"]

    assert updated["Filter"] == %{
             "And" => %{"Prefix" => "old/", "Tags" => [%{"Key" => "new", "Value" => "value"}]}
           }
  end

  test "explicit replication selection preserves encryption criteria and historical modeled fields" do
    call = capture(document())

    assert {:ok, %{}} =
             run("update", "", %{id: "selected", replicate: "replica-metadata-sync"}, call)

    assert_receive {:written, _, input}
    assert [updated] = input["ReplicationConfiguration"]["Rules"]

    assert updated["SourceSelectionCriteria"] == %{
             "SseKmsEncryptedObjects" => %{"Status" => "Enabled"},
             "ReplicaModifications" => %{"Status" => "Enabled"}
           }

    assert updated["ExistingObjectReplication"] == rule()["ExistingObjectReplication"]
    assert updated["DeleteMarkerReplication"] == %{"Status" => "Disabled"}
  end

  test "MinIO remote credentials, hidden ARN and unsupported replication modes fail before I/O" do
    no_call = fn _, _ -> flunk("unsupported input must not make requests") end
    base = %{remote_bucket: @destination, priority: 1, replicate: ""}

    for options <- [
          Map.put(base, :remote_bucket, "https://access:secret@remote/bucket"),
          Map.put(base, :remote_bucket, "alias/bucket"),
          Map.put(base, :arn, "arn:minio:replication::target"),
          Map.put(base, :replicate, "delete"),
          Map.put(base, :replicate, "existing-objects")
        ] do
      assert {:error, %Error{category: :unsupported, code: :provider_extension} = error} =
               run("add", "", options, no_call)

      refute inspect(error) =~ "access:secret"
    end

    assert {:error, %Error{}} = run("add", "", Map.delete(base, :replicate), no_call)
  end

  test "invalid options, conflicts, duplicate IDs and priorities fail without publishing" do
    no_call = fn _, _ -> flunk("invalid input must not make requests") end
    base = %{remote_bucket: @destination, priority: 1, replicate: ""}

    for options <- [
          Map.put(base, :priority, -1),
          Map.delete(base, :priority),
          Map.put(base, :id, ""),
          Map.put(base, :role, "invalid"),
          Map.put(base, :tags, "x=1&x=2"),
          Map.put(base, :storage_class, "MINIO-TIER"),
          Map.merge(base, %{replicate: "delete-marker", tags: "x=1"})
        ] do
      assert {:error, %Error{}} = run("add", "", options, no_call)
    end

    for options <- [%{id: "selected"}, %{id: "selected", state: "invalid"}, %{state: "disable"}] do
      assert {:error, %Error{}} = run("update", "", options, no_call)
    end

    for existing <- [document(), Map.put(document(), "Rules", [rule(), rule()])] do
      call = fn "GetBucketReplication", _ -> {:ok, %{"ReplicationConfiguration" => existing}} end
      assert {:error, %Error{}} = run("add", "", Map.put(base, :id, "selected"), call)
    end

    call = fn "GetBucketReplication", _ -> {:ok, %{"ReplicationConfiguration" => document()}} end

    assert {:error, %Error{}} =
             run("add", "", Map.put(base, :role, "arn:aws:iam::123456789012:role/other"), call)
  end

  test "list filters by status and export returns the exact reusable modeled document" do
    disabled =
      rule()
      |> Map.put("ID", "disabled")
      |> Map.put("Priority", 9)
      |> Map.put("Status", "Disabled")

    original = Map.put(document(), "Rules", [rule(), disabled])
    call = capture(original)
    assert {:ok, ^original} = run("export", "", %{}, call)

    assert {:ok, %{"Role" => @role, "Rules" => [^disabled]}} =
             run("ls", "", %{status: "dIsAbLeD"}, call)

    assert {:ok, ^original} = run("list", "", %{}, call)

    assert {:error, %Error{}} =
             run("list", "", %{status: "bogus"}, fn _, _ -> flunk("invalid status") end)

    assert {:error, %Error{}} = run("export", "prefix", %{}, fn _, _ -> flunk("bucket only") end)
    refute_receive {:written, _, _}
  end

  test "selected removal preserves unrelated rules and last removal deletes the configuration" do
    other = rule() |> Map.put("ID", "other") |> Map.put("Priority", 5)
    call = capture(Map.put(document(), "Rules", [rule(), other]))
    assert {:ok, %{}} = run("remove", "", %{id: "selected"}, call)

    assert_receive {:written, "PutBucketReplication",
                    %{"ReplicationConfiguration" => %{"Role" => @role, "Rules" => [^other]}}}

    assert {:ok, %{}} = run("rm", "", %{id: "selected"}, capture(document()))
    assert_receive {:written, "DeleteBucketReplication", %{"Bucket" => "source"}}
  end

  test "remove requires one ID or explicit all force and dry run never publishes" do
    no_call = fn _, _ -> flunk("unsafe remove") end

    for options <- [%{}, %{all: true}, %{force: true}, %{id: "selected", all: true, force: true}] do
      assert {:error, %Error{}} = run("remove", "", options, no_call)
    end

    assert {:ok, %{"DryRun" => true, "Operation" => "DeleteBucketReplication"}} =
             run("remove", "", %{all: true, force: true, dry_run: true}, no_call)

    call = capture(document())

    assert {:ok, %{"DryRun" => true, "Input" => input}} =
             run("update", "", %{id: "selected", state: "disable", dry_run: true}, call)

    assert [updated] = input["ReplicationConfiguration"]["Rules"]
    assert updated["Status"] == "Disabled"
    refute_receive {:written, _, _}
  end

  test "stdin import is bounded and validates without dropping unsupported fields" do
    for {data, success?} <- [
          {document(), true},
          {%{"Role" => @role, "Rules" => []}, false},
          {Map.put(document(), "ProviderExtension", true), false},
          {Map.put(document(), "Rules", [
             Map.put(rule(), "DeleteReplication", %{"Status" => "Enabled"})
           ]), false}
        ] do
      {:ok, io} = StringIO.open(JSON.encode!(data))
      result = run("import", "", %{stdin: io}, capture(document()))

      if success? do
        assert {:ok, %{}} = result
        assert_receive {:written, "PutBucketReplication", %{"ReplicationConfiguration" => ^data}}
      else
        assert {:error, %Error{}} = result
        refute_receive {:written, _, _}
      end
    end

    {:ok, io} = StringIO.open(String.duplicate(" ", 8_388_609))

    assert {:error, %Error{}} =
             run("import", "", %{stdin: io}, fn _, _ -> flunk("oversize import") end)
  end

  test "unknown modeled fields and absent selected IDs are rejected before mutation" do
    for existing <- [
          Map.put(document(), "Unknown", true),
          Map.put(document(), "Rules", [rule(), rule()]),
          document()
        ] do
      call = fn "GetBucketReplication", _ -> {:ok, %{"ReplicationConfiguration" => existing}} end
      assert {:error, %Error{}} = run("update", "", %{id: "missing", state: "disable"}, call)
      assert {:error, %Error{}} = run("remove", "", %{id: "missing"}, call)
    end
  end

  defp run(action, prefix, options, call),
    do:
      NativeReplication.run(
        action,
        %Target{kind: :remote, bucket: "source", key: prefix},
        options,
        call
      )

  defp capture(document) do
    fn
      "GetBucketReplication", %{"Bucket" => "source"} ->
        {:ok, %{"ReplicationConfiguration" => document}}

      operation, input ->
        send(self(), {:written, operation, input})
        {:ok, %{}}
    end
  end

  defp document, do: %{"Role" => @role, "Rules" => [rule()]}

  defp rule do
    %{
      "ID" => "selected",
      "Priority" => 1,
      "Status" => "Enabled",
      "Filter" => %{
        "And" => %{"Prefix" => "old/", "Tags" => [%{"Key" => "team", "Value" => "old"}]}
      },
      "DeleteMarkerReplication" => %{"Status" => "Disabled"},
      "ExistingObjectReplication" => %{"Status" => "Disabled"},
      "SourceSelectionCriteria" => %{"SseKmsEncryptedObjects" => %{"Status" => "Enabled"}},
      "Destination" => %{
        "Bucket" => @destination,
        "Account" => "123456789012",
        "StorageClass" => "STANDARD",
        "AccessControlTranslation" => %{"Owner" => "Destination"},
        "EncryptionConfiguration" => %{"ReplicaKmsKeyID" => "kms-key"},
        "Metrics" => %{"Status" => "Enabled", "EventThreshold" => %{"Minutes" => 15}},
        "ReplicationTime" => %{"Status" => "Enabled", "Time" => %{"Minutes" => 15}}
      }
    }
  end
end
