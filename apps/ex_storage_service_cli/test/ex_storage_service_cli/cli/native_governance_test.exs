Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeGovernanceTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{Invocation, Management, Parser, Runner}
  alias ExStorageServiceCli.Transfer.Coordinator
  @moduletag :tmp_dir
  @time ~U[2026-10-14 00:00:00Z]
  @now ~U[2026-10-15 00:00:00Z]

  test "portable forms and aliases coexist with full-document arities" do
    for argv <- [
          ["legalhold", "set", "s3://bucket/key"],
          ["legalhold", "set", "s3://bucket/key", ~s({"Status":"ON"})],
          ["retention", "set", "s3://bucket/key", ~s({"Mode":"GOVERNANCE"})],
          ["retention", "set", "governance", "30d", "s3://bucket/key"],
          ["retention", "set", "--default", "compliance", "1y", "s3://bucket/"],
          ["retention", "clear", "--bypass", "--vid", "v1", "s3://bucket/key"],
          ["legalhold", "info", "--versions", "--rewind", "2026-10-15", "s3://bucket/key"]
        ],
        do: assert({:ok, _} = Parser.parse(argv))

    assert Management.target_argument(%{
             command: ["retention", "set"],
             args: ["governance", "1d", "s3://bucket/key"]
           }) == "s3://bucket/key"

    assert {:error, _} =
             Parser.parse([
               "retention",
               "clear",
               "--bypass",
               "--bypass-governance-retention=false",
               "s3://bucket/key"
             ])
  end

  test "full document get set and remove preserve their existing paths", %{tmp_dir: root} do
    for {family, document, operation} <- [
          {"legalhold", ~s({"Status":"ON"}), "PutObjectLegalHold"},
          {"retention", ~s({"Mode":"GOVERNANCE","RetainUntilDate":"2030-01-01T00:00:00Z"}),
           "PutObjectRetention"}
        ] do
      call = fn actual, input ->
        assert actual == operation
        assert input["VersionId"] == "explicit"
        {:ok, %{"kept" => true}}
      end

      assert {:ok, %{"kept" => true}} =
               run(
                 family,
                 "set",
                 ["s3://bucket/key", document],
                 "key",
                 %{version_id: "explicit"},
                 call,
                 root
               )
    end

    call = fn "PutObjectLegalHold", %{"LegalHold" => %{"Status" => "OFF"}} -> {:ok, %{}} end
    assert {:ok, %{}} = run("legalhold", "remove", ["s3://bucket/key"], "key", %{}, call, root)
    call = fn "GetObjectRetention", _ -> {:ok, %{"Retention" => %{}}} end

    assert {:ok, %{"Retention" => %{}}} =
             run("retention", "get", ["s3://bucket/key"], "key", %{}, call, root)
  end

  test "full document set rejects convenience-only flags before provider access", %{tmp_dir: root} do
    for family <- ~w(legalhold retention),
        {option, value} <- [
          {:recursive, true},
          {:recursive, false},
          {:versions, true},
          {:versions, false},
          {:rewind, "2026-10-15"},
          {:default, true},
          {:default, false},
          {:limit, 0}
        ] do
      assert {:error, %Error{category: :syntax, code: :governance_options}} =
               run(
                 family,
                 "set",
                 ["s3://bucket/key", "file://must-not-be-read"],
                 "key",
                 %{option => value},
                 fn _, _ -> flunk("Unsupported full-document selector reached the provider") end,
                 root
               )
    end
  end

  test "full document set retains version dry-run and retention bypass", %{tmp_dir: root} do
    for {family, document, operation} <- [
          {"legalhold", ~s({"Status":"ON"}), "PutObjectLegalHold"},
          {"retention", ~s({"Mode":"GOVERNANCE","RetainUntilDate":"2030-01-01T00:00:00Z"}),
           "PutObjectRetention"}
        ] do
      options = %{version_id: "explicit", dry_run: true}

      options =
        if family == "retention",
          do: Map.put(options, :bypass_governance_retention, true),
          else: options

      assert {:ok, %{"DryRun" => true, "Operation" => ^operation, "Input" => input}} =
               run(
                 family,
                 "set",
                 ["s3://bucket/key", document],
                 "key",
                 options,
                 fn _, _ -> flunk("Full-document dry run reached the provider") end,
                 root
               )

      assert input["VersionId"] == "explicit"
      assert Map.get(input, "BypassGovernanceRetention", false) == (family == "retention")
    end
  end

  test "legal hold set clear and info pin confirmed explicit versions", %{tmp_dir: root} do
    for {action, operation, status} <- [
          {"set", "PutObjectLegalHold", "ON"},
          {"clear", "PutObjectLegalHold", "OFF"},
          {"info", "GetObjectLegalHold", nil}
        ] do
      call = fn
        "HeadObject", input ->
          assert input["VersionId"] == "v1"
          {:ok, head("v1")}

        actual, input ->
          assert actual == operation
          assert input["VersionId"] == "v1"
          if status, do: assert(input["LegalHold"] == %{"Status" => status})
          {:ok, %{"LegalHold" => %{"Status" => status || "ON"}}}
      end

      assert {:ok, %{"Processed" => 1}} =
               run(
                 "legalhold",
                 action,
                 ["s3://bucket/key"],
                 "key",
                 %{version_id: "v1"},
                 call,
                 root
               )
    end

    assert File.ls!(root) == []
  end

  test "retention days uppercase units and calendar years use one UTC instant", %{tmp_dir: root} do
    for {duration, now, expected} <- [
          {"30D", @now, ~U[2026-11-14 00:00:00Z]},
          {"1y", ~U[2024-02-29 12:34:56.123456Z], ~U[2025-03-01 12:34:56Z]}
        ] do
      call = fn
        "HeadObject", _ ->
          {:ok, head("v1")}

        "PutObjectRetention", input ->
          assert input["Retention"] == %{"Mode" => "GOVERNANCE", "RetainUntilDate" => expected}
          refute Map.has_key?(input, "BypassGovernanceRetention")
          {:ok, %{}}
      end

      assert {:ok, _} =
               run(
                 "retention",
                 "set",
                 ["governance", duration, "s3://bucket/key"],
                 "key",
                 %{},
                 call,
                 root,
                 now: now
               )
    end
  end

  test "retention clear sends an empty document and only explicit bypass", %{tmp_dir: root} do
    for bypass <- [false, true] do
      call = fn
        "HeadObject", _ ->
          {:ok, head("v1")}

        "PutObjectRetention", input ->
          assert input["Retention"] == %{}
          assert Map.get(input, "BypassGovernanceRetention", false) == bypass
          {:ok, %{}}
      end

      assert {:ok, _} =
               run(
                 "retention",
                 "clear",
                 ["s3://bucket/key"],
                 "key",
                 %{bypass_governance_retention: bypass},
                 call,
                 root
               )
    end
  end

  test "bucket default edits retain enabled state and remove only the rule", %{tmp_dir: root} do
    for action <- ["set", "clear"] do
      call = fn
        "GetObjectLockConfiguration", _ ->
          {:ok, %{"ObjectLockConfiguration" => config()}}

        "PutObjectLockConfiguration", input ->
          assert input["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled"

          if action == "clear",
            do: refute(Map.has_key?(input["ObjectLockConfiguration"], "Rule")),
            else:
              assert(
                input["ObjectLockConfiguration"]["Rule"]["DefaultRetention"] == %{
                  "Mode" => "COMPLIANCE",
                  "Years" => 2
                }
              )

          {:ok, %{}}
      end

      args = if action == "set", do: ["compliance", "2y", "s3://bucket/"], else: ["s3://bucket/"]
      assert {:ok, _} = run("retention", action, args, "", %{default: true}, call, root)
    end
  end

  test "default info returns the full document and unknown fields block editing without loss", %{
    tmp_dir: root
  } do
    original = Map.put(config(), "FutureField", %{"preserve" => true})

    call = fn "GetObjectLockConfiguration", _ ->
      {:ok, %{"ObjectLockConfiguration" => original}}
    end

    assert {:ok, %{"ObjectLockConfiguration" => ^original}} =
             run("retention", "info", ["s3://bucket/"], "", %{default: true}, call, root)

    assert {:error, %Error{}} =
             run("retention", "clear", ["s3://bucket/"], "", %{default: true}, call, root)

    assert original["Rule"] == config()["Rule"]
    call = fn "GetObjectLockConfiguration", _ -> {:ok, %{"ObjectLockConfiguration" => %{}}} end

    assert {:error, %Error{}} =
             run(
               "retention",
               "set",
               ["governance", "1d", "s3://bucket/"],
               "",
               %{default: true},
               call,
               root
             )
  end

  test "invalid selection modes durations and bucket combinations fail before I/O", %{
    tmp_dir: root
  } do
    no_call = fn _, _ -> flunk("Invalid governance request issued I/O") end

    for options <- [
          %{version_id: "v1", rewind: "1d"},
          %{version_id: "v1", versions: true},
          %{default: true, recursive: true},
          %{default: true, bypass_governance_retention: true},
          %{limit: -1},
          %{limit: 10001}
        ] do
      assert {:error, %Error{}} =
               run("retention", "clear", ["s3://bucket/key"], "key", options, no_call, root)
    end

    for args <- [
          ["bad", "1d", "target"],
          ["governance", "0d", "target"],
          ["governance", "1h", "target"],
          ["governance", "999999999y", "target"]
        ] do
      assert {:error, %Error{}} = run("retention", "set", args, "key", %{}, no_call, root)
    end
  end

  test "declared ESS and directory buckets refuse governance before I/O", %{tmp_dir: root} do
    for context <- [
          %Context{capabilities: %{provider: "ess"}},
          %Context{capabilities: %{bucket_type: "directory"}}
        ] do
      assert {:error, %Error{code: :provider_unsupported}} =
               run(
                 "legalhold",
                 "set",
                 ["s3://bucket/key"],
                 "key",
                 %{},
                 fn _, _ -> flunk("Unsupported I/O") end,
                 root,
                 context: context
               )
    end
  end

  test "complete recursive inventory and identity preflight precede every mutation", %{
    tmp_dir: root
  } do
    parent = self()

    call = fn
      "ListObjectsV2", input ->
        assert input["Prefix"] == "p/"
        send(parent, :listed)
        {:ok, current_page()}

      "HeadObject", input ->
        send(parent, {:head, input["Key"]})
        {:ok, head("v1")}

      "PutObjectLegalHold", input ->
        assert input["VersionId"] == "v1"
        send(parent, {:put, input["Key"]})
        {:ok, %{}}
    end

    assert {:ok, %{"MatchedObjects" => 2, "Processed" => 2, "Returned" => 1, "Partial" => true}} =
             run(
               "legalhold",
               "set",
               ["s3://bucket/p/"],
               "p/",
               %{recursive: true, limit: 1},
               call,
               root
             )

    assert_receive :listed
    assert_receive {:head, "p/a"}
    assert_receive {:head, "p/b"}
    assert_receive {:put, "p/a"}
    assert_receive {:put, "p/b"}
    assert File.ls!(root) == []
  end

  test "later inventory or identity failures prevent all mutations and clean manifests", %{
    tmp_dir: root
  } do
    for stage <- [:listing, :identity] do
      call = fn
        "ListObjectsV2", %{"ContinuationToken" => "next"} ->
          {:error, Error.new(:service, "Denied", code: "AccessDenied")}

        "ListObjectsV2", _ ->
          {:ok,
           Map.merge(
             current_page(),
             if(stage == :listing,
               do: %{"IsTruncated" => true, "NextContinuationToken" => "next"},
               else: %{}
             )
           )}

        "HeadObject", %{"Key" => "p/a"} ->
          {:ok, head("v1")}

        "HeadObject", %{"Key" => "p/b"} ->
          {:ok, Map.put(head("v1"), "ETag", "changed")}
      end

      assert {:error, %Error{}} =
               run("legalhold", "set", ["s3://bucket/p/"], "p/", %{recursive: true}, call, root)

      assert File.ls!(root) == []
    end
  end

  test "rewind pins strict prior versions and excludes equality and tombstones", %{tmp_dir: root} do
    parent = self()

    call = fn
      "ListObjectVersions", _ ->
        {:ok, version_page()}

      "HeadObject", input ->
        assert input["VersionId"] == "old"
        {:ok, head("old")}

      "PutObjectLegalHold", input ->
        send(parent, {:selected, input["VersionId"]})
        {:ok, %{}}
    end

    assert {:ok, %{"Processed" => 1}} =
             run(
               "legalhold",
               "set",
               ["s3://bucket/key"],
               "key",
               %{rewind: "2026-10-15"},
               call,
               root
             )

    assert_receive {:selected, "old"}

    assert {:error, %Error{}} =
             run(
               "legalhold",
               "set",
               ["s3://bucket/key"],
               "key",
               %{rewind: "2026-10-14"},
               call,
               root
             )

    assert File.ls!(root) == []
  end

  test "all versions rewind uses every older data version and skips delete markers", %{
    tmp_dir: root
  } do
    parent = self()

    call = fn
      "ListObjectVersions", _ ->
        {:ok, version_page()}

      "HeadObject", input ->
        {:ok, head(input["VersionId"])}

      "PutObjectRetention", input ->
        send(parent, {:version_put, input["VersionId"]})
        {:ok, %{}}
    end

    assert {:ok, %{"Processed" => 1}} =
             run(
               "retention",
               "clear",
               ["s3://bucket/key"],
               "key",
               %{versions: true, rewind: "2026-10-15"},
               call,
               root
             )

    assert_receive {:version_put, "old"}
    refute_receive {:version_put, _}
  end

  test "mutable selected null versions cannot be mutated but may be inspected", %{tmp_dir: root} do
    call = fn
      "HeadObject", _ -> {:ok, head("null")}
      "GetObjectLegalHold", _ -> {:ok, %{"LegalHold" => %{"Status" => "ON"}}}
    end

    assert {:error, %Error{code: :governance_options}} =
             run(
               "legalhold",
               "clear",
               ["s3://bucket/key"],
               "key",
               %{version_id: "null"},
               call,
               root
             )

    assert {:ok, %{"Processed" => 1}} =
             run(
               "legalhold",
               "info",
               ["s3://bucket/key"],
               "key",
               %{version_id: "null"},
               call,
               root
             )
  end

  test "info output cap also bounds HEAD and object governance GET calls", %{tmp_dir: root} do
    parent = self()

    call = fn
      "ListObjectsV2", _ ->
        {:ok, current_page()}

      "HeadObject", input ->
        send(parent, {:bounded_head, input["Key"]})
        {:ok, head("v1")}

      "GetObjectLegalHold", input ->
        send(parent, {:bounded_get, input["Key"]})
        {:ok, %{}}
    end

    assert {:ok, %{"MatchedObjects" => 2, "Processed" => 1, "Partial" => true}} =
             run(
               "legalhold",
               "info",
               ["s3://bucket/p/"],
               "p/",
               %{recursive: true, limit: 1},
               call,
               root
             )

    assert_receive {:bounded_head, "p/a"}
    assert_receive {:bounded_get, "p/a"}
    refute_receive {:bounded_head, _}
    refute_receive {:bounded_get, _}
  end

  test "dry run validates complete candidates and emits no PUT even at zero output limit", %{
    tmp_dir: root
  } do
    call = fn
      "ListObjectsV2", _ -> {:ok, current_page()}
      "HeadObject", _ -> {:ok, head("v1")}
    end

    assert {:ok, %{"DryRun" => true, "Processed" => 2, "Entries" => [], "Returned" => 0}} =
             run(
               "legalhold",
               "clear",
               ["s3://bucket/p/"],
               "p/",
               %{recursive: true, dry_run: true, limit: 0},
               call,
               root
             )

    assert File.ls!(root) == []
  end

  test "partial failure records completed mutation count", %{tmp_dir: root} do
    call = fn
      "ListObjectsV2", _ ->
        {:ok, current_page()}

      "HeadObject", _ ->
        {:ok, head("v1")}

      "PutObjectLegalHold", %{"Key" => "p/a"} ->
        {:ok, %{}}

      "PutObjectLegalHold", %{"Key" => "p/b"} ->
        {:error, Error.new(:service, "Denied", code: "AccessDenied")}
    end

    assert {:error, %Error{details: %{processed: 1, matched_objects: 2}}} =
             run("legalhold", "clear", ["s3://bucket/p/"], "p/", %{recursive: true}, call, root)
  end

  test "cancellation prevents provider access", %{tmp_dir: root} do
    coordinator = start_supervised!({Coordinator, []})
    Coordinator.cancel(coordinator)

    assert {:error, %Error{category: :interrupted}} =
             run(
               "legalhold",
               "set",
               ["s3://bucket/key"],
               "key",
               %{},
               fn _, _ -> flunk("Cancelled I/O") end,
               root,
               context: %Context{coordinator: coordinator}
             )
  end

  test "real wire legalhold and retention use selected versions and explicit bypass", %{
    tmp_dir: root
  } do
    metadata = [
      {"Content-Length", "5"},
      {"ETag", "e"},
      {"x-amz-version-id", "v1"},
      {"Last-Modified", "Wed, 14 Oct 2026 00:00:00 GMT"}
    ]

    for {family, action, options, part} <- [
          {"legalhold", "set", %{version_id: "v1"}, "legal-hold="},
          {"retention", "clear", %{version_id: "v1", bypass_governance_retention: true},
           "retention="}
        ] do
      {endpoint, _} = WireServer.fixture([{200, metadata, ""}, {200, [], ""}])

      invocation = %Invocation{
        frontend: :native,
        command: [family, action],
        args: ["s3://bucket/key"],
        options: options
      }

      assert {:ok, _} =
               Management.run(
                 invocation,
                 target("key"),
                 %Context{unsigned: true, endpoint: endpoint},
                 tmp_dir: root
               )

      assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/key?versionId=v1"}}
      assert_receive {:wire_request, %{method: "PUT", target: path, headers: headers, body: body}}
      assert path =~ part and path =~ "versionId=v1"

      if family == "legalhold",
        do: assert(body =~ "<Status>ON</Status>"),
        else: assert(headers["x-amz-bypass-governance-retention"] == "true")
    end
  end

  test "actual Runner resolves the third retention argument as the target", %{tmp_dir: root} do
    metadata = [
      {"Content-Length", "5"},
      {"ETag", "e"},
      {"x-amz-version-id", "v1"},
      {"Last-Modified", "Wed, 14 Oct 2026 00:00:00 GMT"}
    ]

    {endpoint, _} = WireServer.fixture([{200, metadata, ""}, {200, [], ""}])

    output =
      ExUnit.CaptureIO.capture_io(fn ->
        assert :ok =
                 Runner.run([
                   "retention",
                   "set",
                   "governance",
                   "1d",
                   "s3://bucket/key",
                   "--service",
                   "s3",
                   "--endpoint",
                   endpoint,
                   "--no-sign-request",
                   "--config-dir",
                   root,
                   "--output",
                   "json"
                 ])
      end)

    assert %{"Processed" => 1, "Entries" => [%{"Key" => "key", "VersionId" => "v1"}]} =
             JSON.decode!(output)

    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/key"}}
    assert_receive {:wire_request, %{method: "PUT", target: path, body: body}}
    assert path =~ "retention=" and path =~ "versionId=v1"
    assert body =~ "<Mode>GOVERNANCE</Mode>"
  end

  test "retained pinned executable oracle records governance wire behavior" do
    oracle =
      Path.expand("../../fixtures/differential/native-governance-wire.json", __DIR__)
      |> File.read!()
      |> JSON.decode!()

    assert oracle["mc_commit"] == "77f82e18b5401a65958f1619df6ebb994634bd88"

    assert oracle["mc_sha256"] ==
             "a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2"

    cases = oracle["cases"]
    assert length(cases) == 14
    clear = Enum.at(cases, 7)["requests"] |> Enum.find(&(&1["method"] == "PUT"))
    assert clear["bypass"] == "true"
    assert clear["body"] == "<Retention></Retention>"
    default = Enum.at(cases, 10)["requests"] |> Enum.find(&(&1["method"] == "PUT"))

    assert default["body"] ==
             "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>"

    ignored = Enum.at(cases, 3)["requests"] |> Enum.find(&(&1["method"] == "PUT"))
    refute ignored["target"] =~ "versionId="
    assert Enum.at(cases, 12)["exit"] == 1
  end

  defp config,
    do: %{
      "ObjectLockEnabled" => "Enabled",
      "Rule" => %{"DefaultRetention" => %{"Mode" => "GOVERNANCE", "Days" => 30}}
    }

  defp head(version),
    do: %{"ETag" => "e", "ContentLength" => 5, "VersionId" => version, "LastModified" => @time}

  defp record(key), do: %{"Key" => key, "Size" => 5, "ETag" => "e", "LastModified" => @time}

  defp current_page,
    do: %{"IsTruncated" => false, "Contents" => Enum.map(["p/a", "p/b"], &record/1)}

  defp version_page,
    do: %{
      "IsTruncated" => false,
      "Versions" => [
        Map.merge(record("key"), %{"VersionId" => "equal", "LastModified" => @now}),
        Map.put(record("key"), "VersionId", "old")
      ],
      "DeleteMarkers" => [
        %{"Key" => "key", "VersionId" => "deleted", "LastModified" => ~U[2026-10-13 00:00:00Z]}
      ]
    }

  defp target(key),
    do: %Target{
      kind: :remote,
      bucket: "bucket",
      key: key,
      trailing_slash: String.ends_with?(key, "/")
    }

  defp run(family, action, args, key, options, call, root, opts \\ []) do
    invocation = %Invocation{
      frontend: :native,
      command: [family, action],
      args: args,
      options: options
    }

    Management.run(
      invocation,
      target(key),
      opts[:context] || %Context{},
      Keyword.merge([call: call, tmp_dir: root, now: @now], opts)
    )
  end
end
