Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeLsOptionsTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}

  alias ExStorageServiceCli.CLI.{
    Invocation,
    NativeEvents,
    NativeInspection,
    NativeListing,
    Parser,
    Runner
  }

  @moduletag :tmp_dir
  defp invocation(options), do: %Invocation{frontend: :native, command: ["ls"], options: options}
  defp target, do: %Target{kind: :remote, bucket: "bucket", key: "p/"}

  defp listing(options, fetch) do
    capture_io(fn ->
      assert :ok = NativeListing.run(invocation(options), target(), %Context{}, fetch: fetch)
    end)
    |> JSON.decode!()
  end

  test "native ls registers incomplete summary and exact storage class flags only" do
    assert {:ok, %{options: %{incomplete: true, summarize: true, storage_class: "GLACIER"}}} =
             Parser.parse([
               "ls",
               "s3://bucket/",
               "--incomplete",
               "--summarize",
               "--storage-class",
               "GLACIER"
             ])

    assert {:error, %Error{}} = Parser.parse(["s3", "ls", "s3://bucket/", "--incomplete"])
  end

  test "storage filtering is exact and preserves unclassified prefix rows; summary counts retained entries" do
    page = %{
      "IsTruncated" => false,
      "Contents" => [
        %{"Key" => "p/cold", "StorageClass" => "GLACIER", "Size" => 7},
        %{"Key" => "p/warm", "StorageClass" => "STANDARD", "Size" => 13}
      ],
      "CommonPrefixes" => [%{"Prefix" => "p/dir/"}]
    }

    fetch = fn "ListObjectsV2", _ -> {:ok, page} end
    result = listing(%{storage_class: "GLACIER", summarize: true}, fetch)
    assert Enum.map(result["Entries"], &(&1["Key"] || &1["Prefix"])) == ["p/cold", "p/dir/"]
    assert result["Summary"] == %{"Entries" => 2, "Bytes" => 7, "Complete" => true}
    assert listing(%{storage_class: "glacier"}, fetch)["Returned"] == 1
    assert listing(%{storage_class: "*"}, fetch)["Returned"] == 3
  end

  test "bounded summary is explicitly partial and filtering happens before the result cap" do
    fetch = fn _, _ ->
      {:ok,
       %{
         "IsTruncated" => false,
         "Contents" => [
           %{"Key" => "p/warm", "StorageClass" => "STANDARD", "Size" => 20},
           %{"Key" => "p/one", "StorageClass" => "GLACIER", "Size" => 7},
           %{"Key" => "p/two", "StorageClass" => "GLACIER", "Size" => 9}
         ]
       }}
    end

    result = listing(%{storage_class: "GLACIER", summarize: true, limit: 1}, fetch)
    assert result["Entries"] == [%{"Key" => "p/one", "StorageClass" => "GLACIER", "Size" => 7}]
    assert result["Partial"]
    assert result["Summary"] == %{"Entries" => 1, "Bytes" => 7, "Complete" => false}
  end

  test "version listing applies raw storage classes and summarizes retained version and prefix rows" do
    fetch = fn "ListObjectVersions", _ ->
      {:ok,
       %{
         "IsTruncated" => false,
         "Versions" => [
           %{"Key" => "p/a", "VersionId" => "one", "StorageClass" => "GLACIER", "Size" => 5},
           %{"Key" => "p/b", "VersionId" => "two", "StorageClass" => "STANDARD", "Size" => 8}
         ],
         "DeleteMarkers" => [%{"Key" => "p/c", "VersionId" => "marker"}],
         "CommonPrefixes" => [%{"Prefix" => "p/dir/"}]
       }}
    end

    assert {:ok, result} =
             NativeListing.versions(
               invocation(%{
                 versions: true,
                 json: true,
                 storage_class: "GLACIER",
                 summarize: true
               }),
               target(),
               %Context{},
               fetch: fetch
             )

    assert result["Returned"] == 3
    assert result["Summary"] == %{"Entries" => 3, "Bytes" => 5, "Complete" => true}
  end

  test "incomplete upload listing follows paired markers and delimiters using only listing requests" do
    first =
      "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated><NextKeyMarker>p/a+</NextKeyMarker><NextUploadIdMarker>id/+</NextUploadIdMarker><Upload><Key>p/a</Key><UploadId>first</UploadId><StorageClass>STANDARD</StorageClass><Initiated>2026-01-01T00:00:00Z</Initiated></Upload></ListMultipartUploadsResult>"

    second =
      "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>p/a</Key><UploadId>second</UploadId><StorageClass>GLACIER</StorageClass></Upload><CommonPrefixes><Prefix>p/dir/</Prefix></CommonPrefixes></ListMultipartUploadsResult>"

    {endpoint, _} = WireServer.fixture([{200, [], first}, {200, [], second}])

    output =
      capture_io(fn ->
        assert :ok =
                 NativeListing.run(
                   invocation(%{incomplete: true, summarize: true, max_keys: 2}),
                   target(),
                   %Context{endpoint: endpoint, unsigned: true}
                 )
      end)
      |> JSON.decode!()

    assert output["Returned"] == 3
    assert Enum.map(Enum.take(output["Entries"], 2), & &1["UploadId"]) == ["first", "second"]
    assert output["Summary"] == %{"Entries" => 3, "Bytes" => nil, "Complete" => true}
    assert_receive {:wire_request, %{method: "GET", target: first_target}}
    assert_receive {:wire_request, %{method: "GET", target: second_target}}
    query = URI.decode_query(URI.parse(first_target).query)

    assert Map.take(query, ["uploads", "prefix", "delimiter", "max-uploads"]) ==
             %{"uploads" => "", "prefix" => "p/", "delimiter" => "/", "max-uploads" => "2"}

    second_query = URI.decode_query(URI.parse(second_target).query)
    assert second_query["key-marker"] == "p/a+"
    assert second_query["upload-id-marker"] == "id/+"
    refute_receive {:wire_request, _}
  end

  test "recursive incomplete listing removes delimiters and filters actual upload classes" do
    fetch = fn "ListMultipartUploads", input ->
      assert input == %{"Bucket" => "bucket", "Prefix" => "other/", "MaxUploads" => 1}

      {:ok,
       %{
         "IsTruncated" => false,
         "Uploads" => [
           %{"Key" => "other/a", "UploadId" => "owned", "StorageClass" => "GLACIER"},
           %{"Key" => "other/b", "UploadId" => "other", "StorageClass" => "STANDARD"}
         ]
       }}
    end

    assert listing(
             %{
               incomplete: true,
               recursive: true,
               delimiter: ":",
               prefix: "other/",
               max_keys: 1,
               storage_class: "GLACIER"
             },
             fetch
           )["Returned"] == 1
  end

  test "incomplete conflicts and historical unsupported combinations fail before provider access" do
    no_call = fn _, _ -> flunk("invalid combination made a request") end

    for options <- [%{incomplete: true, versions: true}, %{incomplete: true, rewind: "1d"}] do
      assert {:error, %Error{category: :syntax}} =
               NativeListing.run(invocation(options), target(), %Context{}, fetch: no_call)
    end

    for options <- [
          %{rewind: "1d", incomplete: true},
          %{rewind: "1d", storage_class: "GLACIER"},
          %{rewind: "1d", summarize: true}
        ] do
      assert {:error, %Error{category: :syntax}} =
               NativeInspection.ls(target(), %Context{}, options, fetch: no_call)
    end

    assert {:error, %Error{category: :syntax}} =
             NativeListing.run(invocation(%{incomplete: true}), nil, %Context{}, fetch: no_call)
  end

  test "invalid incomplete rows, later errors and repeated tokens cannot become successful summaries" do
    for mode <- [:invalid, :denied, :repeated] do
      fetch = fn "ListMultipartUploads", input ->
        cond do
          mode == :invalid ->
            {:ok, %{"IsTruncated" => false, "Uploads" => [%{"Key" => "p/a"}]}}

          mode == :denied and input["KeyMarker"] ->
            {:error, Error.new(:service, "Denied", code: "AccessDenied")}

          true ->
            {:ok,
             %{
               "IsTruncated" => true,
               "NextKeyMarker" => "p/a",
               "NextUploadIdMarker" => "id",
               "Uploads" => [%{"Key" => "p/a", "UploadId" => "id"}]
             }}
        end
      end

      assert {:error, %Error{}} =
               NativeListing.run(
                 invocation(%{incomplete: true, summarize: true}),
                 target(),
                 %Context{},
                 fetch: fetch
               )
    end
  end

  test "NDJSON summary stays explicit when incomplete results are capped" do
    fetch = fn "ListMultipartUploads", _ ->
      {:ok,
       %{
         "IsTruncated" => false,
         "Uploads" => [
           %{"Key" => "p/a", "UploadId" => "one"},
           %{"Key" => "p/a", "UploadId" => "two"}
         ]
       }}
    end

    invocation = invocation(%{incomplete: true, summarize: true, output: "ndjson", limit: 1})

    output =
      capture_io(fn ->
        assert :ok =
                 NativeEvents.with_invocation(invocation, fn selected ->
                   NativeListing.run(selected, target(), %Context{}, fetch: fetch)
                 end)
      end)
      |> String.split("\n", trim: true)
      |> Enum.map(&JSON.decode!/1)

    assert Enum.at(output, 1)["data"]["Summary"] == %{
             "Entries" => 1,
             "Bytes" => nil,
             "Complete" => false
           }

    assert List.last(output)["status"] == "ok"
  end

  test "Runner routes plain flagged ls to upload inventory instead of legacy object listing", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200, [],
         "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>"}
      ])

    result =
      capture_io(fn ->
        assert :ok =
                 Runner.run([
                   "--config-dir",
                   root,
                   "--service",
                   "s3",
                   "--endpoint",
                   endpoint,
                   "--no-sign-request",
                   "ls",
                   "s3://bucket/",
                   "--incomplete",
                   "--summarize"
                 ])
      end)
      |> JSON.decode!()

    assert result["Summary"] == %{"Entries" => 0, "Bytes" => nil, "Complete" => true}
    assert_receive {:wire_request, %{method: "GET", target: target}}
    assert URI.decode_query(URI.parse(target).query)["uploads"] == ""
  end
end
