Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeRestoreTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.CLI.{Management, NativeRestore, Parser}
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.S3.{Capabilities, Client}
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transfer.Coordinator

  test "selected vid grammar reaches RestoreObject and HEAD with the same immutable identity" do
    assert {:ok, invocation} =
             Parser.parse([
               "ilm",
               "restore",
               "fixture/bucket/p/a",
               "--days",
               "3",
               "--vid",
               "v+1/雪"
             ])

    assert invocation.options[:version_id] == "v+1/雪"

    call = fn
      "RestoreObject", input ->
        assert input == %{
                 "Bucket" => "bucket",
                 "Key" => "p/a",
                 "VersionId" => "v+1/雪",
                 "RestoreRequest" => %{
                   "Days" => 3,
                   "GlacierJobParameters" => %{"Tier" => "Expedited"}
                 }
               }

        {:ok, %{}}

      "HeadObject", input ->
        assert input == %{"Bucket" => "bucket", "Key" => "p/a", "VersionId" => "v+1/雪"}
        {:ok, complete()}
    end

    assert {:ok, %{"Requested" => 1, "Restored" => 1}} =
             Management.run(invocation, target("p/a"), context(), call: call)
  end

  test "actual wire request defaults to one day Expedited and encodes selected version consistently" do
    {endpoint, _server} =
      WireServer.fixture([
        {202, [], ""},
        {200,
         [
           {"x-amz-restore",
            "ongoing-request=\"false\", expiry-date=\"Fri, 16 Oct 2026 00:00:00 GMT\""}
         ], ""}
      ])

    context = %{context() | endpoint: endpoint}
    call = fn operation, input -> Client.call(context, operation, input) end

    assert {:ok, %{"Restored" => 1}} =
             NativeRestore.run(target("p/a 雪"), context, %{version_id: "v+1/雪"}, call)

    assert_receive {:wire_request, %{method: "POST", target: path, body: body}}
    assert path == "/bucket/p/a%20%E9%9B%AA?restore=&versionId=v%2B1%2F%E9%9B%AA"
    assert body =~ "<Days>1</Days>"
    assert body =~ "<GlacierJobParameters><Tier>Expedited</Tier></GlacierJobParameters>"
    assert_receive {:wire_request, %{method: "HEAD", target: head}}
    assert head == "/bucket/p/a%20%E9%9B%AA?versionId=v%2B1%2F%E9%9B%AA"
    refute_received {:wire_error, _, _, _}
  end

  test "recursive inventory finishes every page before restoring and is not materialized in output" do
    root = tmp()

    call = fn
      "ListObjectsV2", %{"Bucket" => "bucket", "Prefix" => "p", "ContinuationToken" => "next"} ->
        Process.put(:restore_inventory_complete, true)
        {:ok, %{"Contents" => [%{"Key" => "p/a"}], "IsTruncated" => false}}

      "ListObjectsV2", %{"Bucket" => "bucket", "Prefix" => "p"} ->
        {:ok,
         %{
           "Contents" => [%{"Key" => "prefix-b"}],
           "IsTruncated" => true,
           "NextContinuationToken" => "next"
         }}

      "RestoreObject", input ->
        assert Process.get(:restore_inventory_complete) == true
        send(self(), {:restored_key, input["Key"]})
        {:ok, %{}}

      "HeadObject", _ ->
        {:ok, complete()}
    end

    assert {:ok, result} =
             NativeRestore.run(target("p"), context(), %{recursive: true}, call, tmp_dir: root)

    assert result == %{"Requested" => 2, "Restored" => 2, "Versions" => false}
    assert_receive {:restored_key, "p/a"}
    assert_receive {:restored_key, "prefix-b"}
    assert File.ls!(root) == []
  end

  test "recursive versions restore every immutable data version and skip delete markers" do
    call = fn
      "ListObjectVersions", %{"Bucket" => "bucket", "Prefix" => "p/"} ->
        {:ok,
         %{
           "Versions" => [
             %{"Key" => "p/a", "VersionId" => "new"},
             %{"Key" => "p/a", "VersionId" => "old"},
             %{"Key" => "p/b", "VersionId" => "null"}
           ],
           "DeleteMarkers" => [%{"Key" => "p/gone", "VersionId" => "delete"}],
           "IsTruncated" => false
         }}

      "RestoreObject", input ->
        send(self(), {:version, input["Key"], input["VersionId"]})
        {:ok, %{}}

      "HeadObject", input ->
        send(self(), {:head_version, input["VersionId"]})
        {:ok, complete()}
    end

    assert {:ok, %{"Requested" => 3, "Restored" => 3, "Versions" => true}} =
             NativeRestore.run(target("p/"), context(), %{recursive: true, versions: true}, call)

    for version <- ~w(new old null) do
      assert_receive {:version, _, ^version}
      assert_receive {:head_version, ^version}
    end

    refute_receive {:version, "p/gone", _}
  end

  test "a later inventory error and invalid provider identities produce no restore effect" do
    root = tmp()
    denied = Error.new(:service, "denied", code: "AccessDenied")

    call = fn
      "ListObjectsV2", %{"ContinuationToken" => _} ->
        {:error, denied}

      "ListObjectsV2", _ ->
        {:ok,
         %{
           "Contents" => [%{"Key" => "p/a"}],
           "IsTruncated" => true,
           "NextContinuationToken" => "next"
         }}
    end

    assert {:error, ^denied} =
             NativeRestore.run(target("p/"), context(), %{recursive: true}, call, tmp_dir: root)

    assert File.ls!(root) == []

    for version <- [nil, "", String.duplicate("x", 8193)] do
      call = fn "ListObjectVersions", _ ->
        {:ok,
         %{"Versions" => [%{"Key" => "p/a", "VersionId" => version}], "IsTruncated" => false}}
      end

      assert {:error, _} =
               NativeRestore.run(
                 target("p/"),
                 context(),
                 %{recursive: true, versions: true},
                 call,
                 tmp_dir: root
               )

      assert File.ls!(root) == []
    end

    call = fn "ListObjectsV2", _ ->
      {:ok, %{"Contents" => [%{"Key" => "outside"}], "IsTruncated" => false}}
    end

    assert {:error, _} =
             NativeRestore.run(target("p/"), context(), %{recursive: true}, call, tmp_dir: root)

    assert File.ls!(root) == []
  end

  test "polling observes pending then completion without resubmitting restore requests" do
    call = fn
      "RestoreObject", _ ->
        send(self(), :submitted)
        {:ok, %{}}

      "HeadObject", _ ->
        n = Process.get(:restore_polls, 0)
        Process.put(:restore_polls, n + 1)
        {:ok, if(n == 0, do: pending(), else: complete())}
    end

    sleep = fn delay ->
      send(self(), {:sleep, delay})
      :ok
    end

    assert {:ok, %{"Restored" => 1}} =
             NativeRestore.run(target("p/a"), context(), %{}, call, sleep: sleep)

    assert_receive :submitted
    refute_receive :submitted
    assert_receive {:sleep, 5000}
    assert Process.get(:restore_polls) == 2
  end

  test "default polling exhausts exactly forty HEAD attempts and preserves submitted count" do
    root = tmp()

    call = fn
      "RestoreObject", _ ->
        {:ok, %{}}

      "HeadObject", _ ->
        send(self(), :polled)
        {:ok, pending()}
    end

    sleep = fn 5000 ->
      send(self(), :slept)
      :ok
    end

    assert {:error,
            %Error{
              category: :waiter,
              code: :restore_exhausted,
              attempt: 40,
              details: %{requested: 1, restored: 0}
            }} =
             NativeRestore.run(target("p/a"), context(), %{}, call, sleep: sleep, tmp_dir: root)

    assert length(drain(:polled)) == 40
    assert length(drain(:slept)) == 39
    assert File.ls!(root) == []
  end

  test "SIGTERM interrupts the default poll delay and removes the manifest" do
    root = tmp()

    call = fn
      "RestoreObject", _ ->
        {:ok, %{}}

      "HeadObject", _ ->
        send(self(), {:ess_cancel, {make_ref(), :sigterm}})
        {:ok, pending()}
    end

    assert {:error, %Error{category: :terminated, details: %{requested: 1, restored: 0}}} =
             NativeRestore.run(target("p/a"), context(), %{}, call, tmp_dir: root)

    assert File.ls!(root) == []
  end

  test "coordinator cancellation stops before any inventory or effects" do
    coordinator = start_supervised!({Coordinator, []})
    assert :ok = Coordinator.cancel(coordinator)
    context = %{context() | coordinator: coordinator}

    assert {:error, %Error{category: :interrupted}} =
             NativeRestore.run(target("p/"), context, %{recursive: true}, no_call())
  end

  test "missing malformed and unavailable restore status never claims completion" do
    for response <- [
          %{},
          %{"Restore" => "garbage"},
          %{"Restore" => "ongoing-request=\"unknown\""}
        ] do
      call = fn
        "RestoreObject", _ -> {:ok, %{}}
        "HeadObject", _ -> {:ok, response}
      end

      assert {:error, %Error{code: :restore_status}} =
               NativeRestore.run(target("p/a"), context(), %{}, call)
    end

    denied = Error.new(:service, "denied", code: "AccessDenied")

    call = fn
      "RestoreObject", _ -> {:ok, %{}}
      "HeadObject", _ -> {:error, denied}
    end

    assert {:error, %Error{code: "AccessDenied", details: %{requested: 1}}} =
             NativeRestore.run(target("p/a"), context(), %{}, call)
  end

  test "partial submission errors stop further effects and report the completed request count" do
    root = tmp()
    invalid = Error.new(:service, "not archived", code: "InvalidObjectState")

    call = fn
      "ListObjectsV2", _ ->
        {:ok, %{"Contents" => [%{"Key" => "p/a"}, %{"Key" => "p/b"}], "IsTruncated" => false}}

      "RestoreObject", %{"Key" => "p/a"} ->
        {:ok, %{}}

      "RestoreObject", %{"Key" => "p/b"} ->
        {:error, invalid}
    end

    assert {:error, %Error{code: "InvalidObjectState", details: %{requested: 1, restored: 0}}} =
             NativeRestore.run(target("p/"), context(), %{recursive: true}, call, tmp_dir: root)

    assert File.ls!(root) == []
  end

  test "dry-run inventories and validates but sends no restore request or HEAD" do
    call = fn "ListObjectsV2", _ ->
      {:ok, %{"Contents" => [%{"Key" => "p/a"}, %{"Key" => "p/b"}], "IsTruncated" => false}}
    end

    assert {:ok,
            %{
              "DryRun" => true,
              "Objects" => 2,
              "RestoreRequest" => %{
                "Days" => 5,
                "GlacierJobParameters" => %{"Tier" => "Expedited"}
              }
            }} =
             NativeRestore.run(
               target("p/"),
               context(),
               %{recursive: true, dry_run: true, days: 5},
               call
             )
  end

  test "unsupported providers bucket types and incompatible flags fail before I/O" do
    for options <- [
          %{days: 0},
          %{days: -1},
          %{days: 2_147_483_648},
          %{version_id: ""},
          %{versions: true},
          %{version_id: "v", recursive: true},
          %{version_id: "v", versions: true, recursive: true}
        ] do
      assert {:error, %Error{category: :syntax}} =
               NativeRestore.run(target("p/a"), context(), options, no_call())
    end

    assert {:error, _} = NativeRestore.run(target(""), context(), %{}, no_call())

    assert {:error, _} =
             NativeRestore.run(%Target{kind: :local, path: "/tmp/x"}, context(), %{}, no_call())

    for context <- [
          %{context() | capabilities: Capabilities.record("ess", "0.6.7")},
          %{context() | capabilities: Capabilities.record("aws", nil, "directory")}
        ] do
      assert {:error, %Error{code: :provider_unsupported}} =
               NativeRestore.run(target("p/a"), context, %{dry_run: true}, no_call())
    end

    for flags <- [
          ["--enc-c", "fixture/bucket/=secret"],
          ["--tier", "Bulk"],
          ["--restore-tier", "Bulk"]
        ] do
      assert {:error, _} = Parser.parse(["ilm", "restore", "fixture/bucket/p/a" | flags])
    end
  end

  defp target(key), do: %Target{kind: :remote, alias: "fixture", bucket: "bucket", key: key}
  defp context, do: %Context{unsigned: true, capabilities: Capabilities.record("s3")}

  defp complete,
    do: %{"Restore" => "ongoing-request=\"false\", expiry-date=\"Fri, 16 Oct 2026 00:00:00 GMT\""}

  defp pending, do: %{"Restore" => "ongoing-request=\"true\""}
  defp no_call, do: fn _, _ -> flunk("Invalid restore input must fail before I/O") end

  defp drain(message) do
    receive do
      ^message -> [message | drain(message)]
    after
      0 -> []
    end
  end

  defp tmp do
    path =
      Path.join(
        System.tmp_dir!(),
        "ess-restore-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      )

    File.mkdir!(path)
    on_exit(fn -> File.rm_rf(path) end)
    path
  end
end
