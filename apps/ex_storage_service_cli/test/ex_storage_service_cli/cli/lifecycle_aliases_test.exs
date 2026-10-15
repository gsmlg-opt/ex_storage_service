defmodule ExStorageServiceCli.CLI.LifecycleAliasesTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.CLI.{Management, Parser}
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.S3.Encoder

  test "hidden expiry and delete-marker aliases have the same modeled effects" do
    for flags <- [["--expire-days", "12"], ["--expiry-days", "12"]] do
      assert added(flags)["Expiration"] == %{"Days" => 12}
    end

    for flag <- ~w(expire-delete-marker expired-object-delete-marker) do
      assert added(["--" <> flag])["Expiration"] == %{"ExpiredObjectDeleteMarker" => true}
    end
  end

  test "current storage class aliases normalize portable classes to uppercase" do
    for flag <- ~w(storage-class tier transition-tier) do
      assert added(["--transition-days", "0", "--" <> flag, "glacier"])["Transitions"] ==
               [%{"Days" => 0, "StorageClass" => "GLACIER"}]
    end

    assert added([
             "--transition-days",
             "0",
             "--storage-class",
             "glacier",
             "--transition-tier",
             "GLACIER"
           ])["Transitions"] ==
             [%{"Days" => 0, "StorageClass" => "GLACIER"}]
  end

  test "noncurrent expiry aliases preserve days and retained version count semantics" do
    for days <- ~w(noncurrent-expire-days noncurrentversion-expiration-days),
        newer <- ~w(noncurrent-expire-newer newer-noncurrentversions-expiration) do
      assert added(["--" <> days, "20", "--" <> newer, "3"])["NoncurrentVersionExpiration"] ==
               %{"NoncurrentDays" => 20, "NewerNoncurrentVersions" => 3}
    end
  end

  test "noncurrent transition aliases map to AWS days storage class and newer count fields" do
    for days <- ~w(noncurrent-transition-days noncurrentversion-transition-days),
        tier <-
          ~w(noncurrent-transition-tier noncurrentversion-transition-storage-class noncurrentversion-tier),
        newer <- ~w(noncurrent-transition-newer newer-noncurrentversions-transition) do
      assert added(["--" <> days, "20", "--" <> tier, "glacier", "--" <> newer, "3"])[
               "NoncurrentVersionTransitions"
             ] ==
               [
                 %{
                   "NoncurrentDays" => 20,
                   "StorageClass" => "GLACIER",
                   "NewerNoncurrentVersions" => 3
                 }
               ]
    end
  end

  test "absolute dates become UTC midnight in operation input and actual encoded XML" do
    flags = [
      "--expiry-date",
      "2032-02-29",
      "--transition-date",
      "2031-01-01",
      "--storage-class",
      "GLACIER"
    ]

    assert {:ok, %{}} = run("add", flags, capture(%{"Rules" => []}))
    assert_receive {:written, input}
    assert [rule] = input["LifecycleConfiguration"]["Rules"]
    assert rule["Expiration"] == %{"Date" => "2032-02-29T00:00:00Z"}

    assert rule["Transitions"] == [
             %{"Date" => "2031-01-01T00:00:00Z", "StorageClass" => "GLACIER"}
           ]

    assert {:ok, request} = Encoder.encode("PutBucketLifecycleConfiguration", input)
    assert request.body =~ "<Date>2032-02-29T00:00:00Z</Date>"
    assert request.body =~ "<Date>2031-01-01T00:00:00Z</Date>"
    refute request.body =~ "<Days>"
  end

  test "editing dates clears conflicting day fields and preserves other rules filters and actions" do
    selected = rule()
    other = Map.put(selected, "ID", "other")
    flags = ["--id", "selected", "--expiry-date", "2032-02-29", "--transition-date", "2031-01-01"]
    assert {:ok, %{}} = run("edit", flags, capture(%{"Rules" => [selected, other]}))
    assert_receive {:written, %{"LifecycleConfiguration" => %{"Rules" => [edited, ^other]}}}

    assert edited ==
             selected
             |> Map.put("Expiration", %{"Date" => "2032-02-29T00:00:00Z"})
             |> Map.put("Transitions", [
               %{"Date" => "2031-01-01T00:00:00Z", "StorageClass" => "GLACIER"}
             ])
  end

  test "editing days clears dates and delete-marker choices replace all timing alternatives" do
    dated =
      rule()
      |> Map.put("Expiration", %{"Date" => "2032-02-29T00:00:00Z"})
      |> Map.put("Transitions", [%{"Date" => "2031-01-01T00:00:00Z", "StorageClass" => "GLACIER"}])

    assert {:ok, %{}} =
             run(
               "edit",
               ["--id", "selected", "--expiry-days", "90", "--transition-days", "30"],
               capture(%{"Rules" => [dated]})
             )

    assert_receive {:written, input}
    assert [updated] = input["LifecycleConfiguration"]["Rules"]
    assert updated["Expiration"] == %{"Days" => 90}
    assert updated["Transitions"] == [%{"Days" => 30, "StorageClass" => "GLACIER"}]

    assert {:ok, %{}} =
             run(
               "edit",
               ["--id", "selected", "--expired-object-delete-marker"],
               capture(%{"Rules" => [dated]})
             )

    assert_receive {:written, input}

    assert hd(input["LifecycleConfiguration"]["Rules"])["Expiration"] == %{
             "ExpiredObjectDeleteMarker" => true
           }
  end

  test "selected tier-only edit retains absolute timing and noncurrent count-only edits retain days" do
    dated =
      rule()
      |> Map.put("Transitions", [%{"Date" => "2031-01-01T00:00:00Z", "StorageClass" => "GLACIER"}])

    flags = [
      "--id",
      "selected",
      "--tier",
      "deep_archive",
      "--noncurrent-transition-newer",
      "7",
      "--noncurrent-expire-newer",
      "8"
    ]

    assert {:ok, %{}} = run("edit", flags, capture(%{"Rules" => [dated]}))
    assert_receive {:written, input}
    assert [updated] = input["LifecycleConfiguration"]["Rules"]

    assert updated["Transitions"] == [
             %{"Date" => "2031-01-01T00:00:00Z", "StorageClass" => "DEEP_ARCHIVE"}
           ]

    assert updated["NoncurrentVersionExpiration"] == %{
             "NoncurrentDays" => 50,
             "NewerNoncurrentVersions" => 8
           }

    assert updated["NoncurrentVersionTransitions"] == [
             %{
               "NoncurrentDays" => 30,
               "StorageClass" => "GLACIER",
               "NewerNoncurrentVersions" => 7
             }
           ]

    assert updated["Filter"] == dated["Filter"]
    assert updated["AbortIncompleteMultipartUpload"] == dated["AbortIncompleteMultipartUpload"]
  end

  test "invalid dates conflicting date/day selectors and invalid counts fail before service I/O" do
    flags = [
      ["--expiry-date", "2031-02-29"],
      ["--expiry-date", "0001-01-01"],
      ["--expiry-date", "0000-01-01"],
      ["--expiry-date", "2032-2-29"],
      ["--expiry-date", "2032-02-29T00:00:00Z"],
      ["--transition-date", "2031-02-29", "--transition-tier", "GLACIER"],
      ["--expiry-date", "2032-02-29", "--expire-days", "1"],
      ["--expiry-date", "2032-02-29", "--expired-object-delete-marker"],
      ["--expiry-days", "1", "--expire-delete-marker"],
      [
        "--transition-date",
        "2032-02-29",
        "--transition-days",
        "1",
        "--transition-tier",
        "GLACIER"
      ],
      ["--noncurrent-transition-newer", "2"],
      ["--noncurrent-expire-newer", "2"],
      [
        "--noncurrent-transition-days",
        "1",
        "--noncurrent-transition-tier",
        "GLACIER",
        "--noncurrent-transition-newer",
        "101"
      ],
      ["--noncurrent-expire-days", "1", "--noncurrent-expire-newer", "0"],
      ["--transition-date", "2032-02-29"]
    ]

    for values <- flags do
      assert {:error, %Error{category: :syntax}} = run("add", values, no_call())
    end
  end

  test "aliases with conflicting values fail rather than applying precedence" do
    for flags <- [
          ["--expiry-days", "1", "--expire-days", "2"],
          [
            "--transition-days",
            "1",
            "--storage-class",
            "GLACIER",
            "--transition-tier",
            "STANDARD_IA"
          ],
          ["--transition-days", "1", "--tier", "GLACIER", "--transition-tier", "STANDARD_IA"],
          ["--noncurrent-expire-newer", "2", "--newer-noncurrentversions-expiration", "3"]
        ] do
      assert {:error, %Error{category: :syntax}} = run("add", flags, no_call())
    end
  end

  test "MinIO custom tiers and all-version expiration never become different portable effects" do
    for flag <- ~w(storage-class tier transition-tier) do
      assert {:error, %Error{category: :syntax}} =
               run("add", ["--transition-days", "1", "--" <> flag, "MINIOTIER-1"], no_call())
    end

    assert {:error, _} = run("add", ["--expire-all-object-versions"], no_call())
    assert {:error, _} = run("add", ["--purge-all-object-versions"], no_call())
  end

  test "ls and rm aliases retain selection safeguards and operation effects" do
    doc = %{"Rules" => [rule()]}
    assert {:ok, ^doc} = run("ls", [], fn "GetBucketLifecycleConfiguration", _ -> {:ok, doc} end)
    assert {:error, _} = run("rm", [], no_call())

    call = fn
      "GetBucketLifecycleConfiguration", _ ->
        {:ok, doc}

      "DeleteBucketLifecycle", input ->
        send(self(), {:deleted, input})
        {:ok, %{}}
    end

    assert {:ok, %{}} = run("rm", ["--id", "selected"], call)
    assert_receive {:deleted, %{"Bucket" => "bucket"}}
  end

  test "date and retained-count edits reject implicit selection of multiple transitions" do
    original =
      Map.put(rule(), "Transitions", [
        %{"Days" => 1, "StorageClass" => "GLACIER"},
        %{"Days" => 2, "StorageClass" => "DEEP_ARCHIVE"}
      ])

    assert {:error, %Error{message: message}} =
             run(
               "edit",
               ["--id", "selected", "--transition-date", "2032-02-29"],
               capture(%{"Rules" => [original]})
             )

    assert message =~ "Multiple transitions"
    refute_received {:written, _}

    original =
      Map.put(rule(), "NoncurrentVersionTransitions", [
        %{"NoncurrentDays" => 1, "StorageClass" => "GLACIER"},
        %{"NoncurrentDays" => 2, "StorageClass" => "DEEP_ARCHIVE"}
      ])

    assert {:error, _} =
             run(
               "edit",
               ["--id", "selected", "--noncurrent-transition-newer", "5"],
               capture(%{"Rules" => [original]})
             )

    refute_received {:written, _}
  end

  test "date dry-run validates full effect without performing a PUT" do
    call = fn "GetBucketLifecycleConfiguration", _ -> {:ok, %{"Rules" => [rule()]}} end

    assert {:ok, %{"DryRun" => true, "Input" => input}} =
             run("edit", ["--id", "selected", "--expiry-date", "2032-02-29", "--dry-run"], call)

    assert hd(input["LifecycleConfiguration"]["Rules"])["Expiration"] == %{
             "Date" => "2032-02-29T00:00:00Z"
           }
  end

  defp added(flags) do
    assert {:ok, %{}} = run("add", flags, capture(%{"Rules" => []}))
    assert_receive {:written, input}
    hd(input["LifecycleConfiguration"]["Rules"])
  end

  defp run(action, flags, call) do
    with {:ok, invocation} <- Parser.parse(["ilm", "rule", action, "s3://bucket" | flags]),
         {:ok, target} <- Target.parse("s3://bucket") do
      Management.run(invocation, target, %Context{}, call: call)
    end
  end

  defp capture(document),
    do: fn
      "GetBucketLifecycleConfiguration", %{"Bucket" => "bucket"} ->
        {:ok, document}

      "PutBucketLifecycleConfiguration", input ->
        assert {:ok, _} = Encoder.encode("PutBucketLifecycleConfiguration", input)
        send(self(), {:written, input})
        {:ok, %{}}
    end

  defp no_call, do: fn _, _ -> flunk("Invalid lifecycle options must fail before I/O") end

  defp rule,
    do: %{
      "ID" => "selected",
      "Status" => "Enabled",
      "Filter" => %{"Prefix" => "logs/"},
      "Expiration" => %{"Days" => 90},
      "Transitions" => [%{"Days" => 20, "StorageClass" => "GLACIER"}],
      "NoncurrentVersionExpiration" => %{"NoncurrentDays" => 50, "NewerNoncurrentVersions" => 2},
      "NoncurrentVersionTransitions" => [
        %{"NoncurrentDays" => 30, "StorageClass" => "GLACIER", "NewerNoncurrentVersions" => 3}
      ],
      "AbortIncompleteMultipartUpload" => %{"DaysAfterInitiation" => 7}
    }
end
