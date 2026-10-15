defmodule ExStorageServiceCli.S3.PaginatorTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, S3.Paginator}

  test "all seven pinned paginators terminate and preserve page limits" do
    for {name, model} <- ExStorageServiceCli.S3.OperationCatalog.paginators() do
      owner = self()
      page = Map.new(List.wrap(model["result_key"]), &{&1, []})
      page = if model["more_results"], do: Map.put(page, model["more_results"], false), else: page

      fetch = fn operation, input ->
        send(owner, {operation, input})
        {:ok, page}
      end

      assert [{:ok, ^page}] = Enum.to_list(Paginator.pages(name, %{}, fetch, page_size: 7))
      assert_receive {^name, input}
      assert input[model["limit_key"]] == 7
    end
  end

  test "multi-token pages change both markers and aggregate result families" do
    fetch = fn "ListObjectVersions", input ->
      case input do
        %{"KeyMarker" => "a", "VersionIdMarker" => "v1"} ->
          {:ok,
           %{
             "IsTruncated" => false,
             "Versions" => [%{"Key" => "b"}],
             "DeleteMarkers" => [%{"Key" => "c"}]
           }}

        %{} ->
          {:ok,
           %{
             "IsTruncated" => true,
             "NextKeyMarker" => "a",
             "NextVersionIdMarker" => "v1",
             "Versions" => [%{"Key" => "a"}]
           }}
      end
    end

    assert {:ok,
            %{
              "Versions" => [%{"Key" => "a"}, %{"Key" => "b"}],
              "DeleteMarkers" => [%{"Key" => "c"}]
            }} = Paginator.collect("ListObjectVersions", %{}, fetch)
  end

  test "truncated responses without tokens, immediate repeats and long cycles fail" do
    for fetch <- [
          fn _, _ -> {:ok, %{"IsTruncated" => true}} end,
          fn _, _ -> {:ok, %{"IsTruncated" => true, "NextContinuationToken" => "same"}} end,
          fn _, input ->
            token =
              case input["ContinuationToken"] do
                nil -> "a"
                "a" -> "b"
                "b" -> "c"
                "c" -> "a"
              end

            {:ok, %{"IsTruncated" => true, "NextContinuationToken" => token}}
          end
        ] do
      assert {:error, %Error{category: :protocol}} =
               Paginator.collect("ListObjectsV2", %{}, fetch)
    end
  end

  test "ListObjects uses its modeled last-key fallback; no-paginate makes one request" do
    fetch = fn _, input ->
      if input["Marker"] == "a",
        do: {:ok, %{"IsTruncated" => false}},
        else: {:ok, %{"IsTruncated" => true, "Contents" => [%{"Key" => "a"}]}}
    end

    assert [{:ok, _}, {:ok, _}] = Enum.to_list(Paginator.pages("ListObjects", %{}, fetch))

    assert [{:ok, _}] =
             Enum.to_list(Paginator.pages("ListObjects", %{}, fetch, no_paginate: true))

    assert {:error, %Error{code: :aggregate_limit}} =
             Paginator.collect("ListObjects", %{}, fetch, max_aggregate_bytes: 1)
  end

  test "CLI tokens truncate within a page, resume secondary lists, and cross exact page boundaries" do
    fetch = fn _, input ->
      case input["ContinuationToken"] do
        nil ->
          {:ok,
           %{
             "IsTruncated" => true,
             "NextContinuationToken" => "page2",
             "Contents" => Enum.map(1..3, &%{"Key" => to_string(&1)}),
             "CommonPrefixes" => [%{"Prefix" => "dir/"}]
           }}

        "page2" ->
          {:ok, %{"IsTruncated" => false, "Contents" => [%{"Key" => "4"}]}}
      end
    end

    assert {:ok, %{"Contents" => [%{"Key" => "1"}], "NextToken" => token}} =
             Paginator.collect("ListObjectsV2", %{}, fetch, max_items: 1)

    assert JSON.decode!(Base.decode64!(token)) == %{
             "ContinuationToken" => nil,
             "boto_truncate_amount" => 1
           }

    assert {:ok,
            %{
              "Contents" => [%{"Key" => "2"}, %{"Key" => "3"}, %{"Key" => "4"}],
              "CommonPrefixes" => []
            } = resumed} = Paginator.collect("ListObjectsV2", %{}, fetch, starting_token: token)

    refute Map.has_key?(resumed, "NextToken")

    assert {:ok, %{"NextToken" => boundary}} =
             Paginator.collect("ListObjectsV2", %{}, fetch, max_items: 3)

    assert JSON.decode!(Base.decode64!(boundary)) == %{"ContinuationToken" => "page2"}

    assert {:ok, %{"Contents" => [%{"Key" => "4"}]}} =
             Paginator.collect("ListObjectsV2", %{}, fetch, starting_token: boundary)
  end

  test "legacy multiple tokens remain usable and invalid pagination fails before a request" do
    owner = self()

    fetch = fn _, input ->
      send(owner, {:input, input})
      {:ok, %{"IsTruncated" => false, "Versions" => [%{"Key" => "1"}, %{"Key" => "2"}]}}
    end

    assert {:ok, %{"Versions" => [%{"Key" => "2"}]}} =
             Paginator.collect("ListObjectVersions", %{}, fetch,
               starting_token: "key___version___1"
             )

    assert_receive {:input, %{"KeyMarker" => "key", "VersionIdMarker" => "version"}}

    for opts <- [[max_items: -1], [page_size: 0], [no_paginate: true, max_items: 1]] do
      assert {:error, %Error{category: :syntax}} =
               Paginator.collect("ListObjectVersions", %{}, fetch, opts)

      refute_receive {:input, _}
    end
  end
end
