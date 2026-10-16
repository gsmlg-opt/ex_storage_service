defmodule ExStorageServiceCli.CLI.NativeInspection do
  @moduledoc "Bounded portable native projections, version metadata and selected raw reads."
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.S3.{Client, Paginator}
  alias ExStorageServiceCli.Transfer.{Coordinator, Engine, Inventory, Manifest, VersionSelection}

  def cat(target, context, options, opts) do
    with :ok <- raw_options(options),
         {:ok, opts} <- source(target, context, options, opts),
         {:ok, input, opts} <- selection(target, context, options, opts),
         {:ok, _} <-
           Engine.download(
             context,
             target.bucket,
             target.key,
             opts[:write] || fn bytes -> IO.binwrite(bytes) end,
             Keyword.put(opts, :input, input)
           ),
         do: :ok
  end

  @doc "Pins historical raw reads before exposing any response bytes."
  def source(target, context, options, opts) do
    with :ok <- rewind_options(options) do
      if options[:rewind] do
        with {:ok, entry} <-
               VersionSelection.single(
                 context,
                 target,
                 options[:rewind],
                 history_opts(context, opts)
               ),
             input =
               Map.merge(opts[:input] || %{}, %{
                 "Bucket" => target.bucket,
                 "Key" => target.key,
                 "VersionId" => entry["version_id"]
               }),
             {:ok, head} <- call(context, "HeadObject", input, opts),
             :ok <- historical_identity(entry, head) do
          {:ok,
           opts
           |> Keyword.put(
             :input,
             input |> Map.drop(["Bucket", "Key"]) |> Map.put("IfMatch", head["ETag"])
           )
           |> Keyword.put(:expected_bytes, entry["size"])
           |> Keyword.put(:verify_selection, true)
           |> Keyword.put(:selected_head, head)}
        end
      else
        input = opts[:input] || %{}

        input =
          if options[:version_id],
            do: Map.put(input, "VersionId", options[:version_id]),
            else: input

        {:ok, Keyword.put(opts, :input, input)}
      end
    end
  end

  def ls(target, context, options, opts) do
    with :ok <- bounds(options),
         :ok <- rewind_options(options),
         :ok <- historical_listing_options(options) do
      with_inventory(target, context, options, opts, fn inventory ->
        limit = options[:limit] || 1000
        rows = checked(inventory, context) |> Enum.take(limit)

        {:ok,
         %{
           "Entries" => rows,
           "Returned" => length(rows),
           "MatchedObjects" => inventory.count,
           "Partial" => inventory.count > limit
         }}
      end)
    end
  end

  defp historical_listing_options(%{rewind: _, delimiter: _}),
    do:
      failure(
        :historical_listing_delimiter,
        "Historical listings use flat snapshot rows; --delimiter is unsupported with --rewind"
      )

  defp historical_listing_options(%{rewind: _} = options) do
    if Enum.any?([:incomplete, :summarize, :storage_class], &Map.has_key?(options, &1)),
      do:
        failure(
          :historical_listing_options,
          "Historical listings do not support incomplete, summary or storage-class options"
        ),
      else: :ok
  end

  defp historical_listing_options(_), do: :ok

  defp raw_options(options) do
    cond do
      Enum.any?([:offset, :tail], &(not is_integer(options[&1] || 0) or (options[&1] || 0) < 0)) ->
        failure(:cat_selection, "Offset and tail must be nonnegative integers")

      (options[:offset] || 0) > 0 and (options[:tail] || 0) > 0 ->
        failure(:cat_selection, "Choose either offset or tail")

      options[:part_number] != nil and options[:part_number] not in 1..10_000 ->
        failure(:cat_selection, "Part number must be between 1 and 10000")

      options[:part_number] != nil and ((options[:offset] || 0) > 0 or (options[:tail] || 0) > 0) ->
        failure(:cat_selection, "Part number conflicts with offset and tail")

      true ->
        :ok
    end
  end

  defp selection(target, context, options, opts) do
    input = opts[:input] || %{}

    input =
      if options[:version_id], do: Map.put(input, "VersionId", options[:version_id]), else: input

    if (options[:offset] || 0) == 0 and (options[:tail] || 0) == 0 and
         options[:part_number] == nil do
      {:ok, input, opts}
    else
      input =
        if options[:part_number],
          do: Map.put(input, "PartNumber", options[:part_number]),
          else: input

      with {:ok, head} <-
             call(
               context,
               "HeadObject",
               Map.merge(input, %{"Bucket" => target.bucket, "Key" => target.key}),
               opts
             ),
           :ok <- selected_version(head, input),
           true <- is_integer(head["ContentLength"]) and head["ContentLength"] >= 0,
           true <- is_binary(head["ETag"]) and head["ETag"] != "" do
        input = Map.put(input, "IfMatch", head["ETag"])

        input =
          if head["VersionId"], do: Map.put(input, "VersionId", head["VersionId"]), else: input

        cond do
          head["ContentLength"] == 0 and options[:part_number] == nil and
              (options[:offset] || 0) == 0 ->
            {:ok, input, Keyword.put(opts, :expected_bytes, 0)}

          options[:part_number] != nil ->
            if is_binary(head["ContentRange"]) and is_integer(head["PartsCount"]) and
                 head["PartsCount"] >= options[:part_number],
               do:
                 {:ok, input,
                  opts
                  |> Keyword.put(:expected_part_range, head["ContentRange"])
                  |> Keyword.put(:expected_bytes, head["ContentLength"])},
               else:
                 failure(
                   :part_identity,
                   "Part selection requires confirmed part range and count metadata"
                 )

          (options[:offset] || 0) >= head["ContentLength"] and (options[:offset] || 0) > 0 ->
            failure(:cat_selection, "Offset must be smaller than the object size")

          true ->
            start =
              if (options[:tail] || 0) > 0,
                do: max(head["ContentLength"] - options[:tail], 0),
                else: options[:offset] || 0

            {:ok, Map.put(input, "Range", "bytes=#{start}-"),
             Keyword.put(opts, :expected_bytes, head["ContentLength"] - start)}
        end
      else
        false ->
          failure(:object_identity, "Selected read requires confirmed object size and identity")

        error ->
          error
      end
    end
  end

  def stat(target, context, options, opts) do
    opts =
      if options[:version_id],
        do:
          Keyword.put(
            opts,
            :input,
            Map.put(opts[:input] || %{}, "VersionId", options[:version_id])
          ),
        else: opts

    with :ok <- bounds(options),
         :ok <- stat_options(target, options),
         :ok <- version_options(target, options),
         :ok <- rewind_options(options) do
      cond do
        options[:no_list] == true and target.key in [nil, ""] ->
          input =
            (opts[:input] || %{})
            |> Map.take(["ExpectedBucketOwner"])
            |> Map.put("Bucket", target.bucket)

          call(context, "HeadBucket", input, opts)

        target.key not in [nil, ""] and
          (not String.ends_with?(target.key, "/") or options[:no_list] == true) and
          options[:recursive] != true and options[:versions] != true ->
          input =
            Map.merge(opts[:input] || %{}, %{"Bucket" => target.bucket, "Key" => target.key})

          result =
            if options[:rewind] do
              with {:ok, selected} <- source(target, context, options, opts),
                   do: {:ok, selected[:selected_head]}
            else
              call(context, "HeadObject", input, opts)
            end

          case result do
            {:ok, head} ->
              with :ok <- selected_version(head, input), do: {:ok, head}

            {:error, %Error{http_status: 404}} when not is_map_key(options, :version_id) ->
              if options[:no_list] == true,
                do: result,
                else: stat_many(target, context, options, opts)

            error ->
              error
          end

        true ->
          stat_many(target, context, options, opts)
      end
    end
  end

  defp stat_options(target, %{no_list: true} = options) do
    cond do
      options[:recursive] == true or options[:versions] == true ->
        failure(:stat_no_list, "Stat --no-list conflicts with --recursive or --versions")

      target.key in [nil, ""] and options[:rewind] != nil ->
        failure(:stat_no_list, "Bucket stat --no-list cannot select a historical object snapshot")

      true ->
        :ok
    end
  end

  defp stat_options(_, _), do: :ok

  defp stat_many(target, context, options, opts) do
    with_inventory(target, context, options, opts, fn inventory ->
      rows = checked(inventory, context) |> Enum.take((options[:limit] || 1000) + 1)
      limit = options[:limit] || 1000

      result =
        Enum.reduce_while(Enum.take(rows, limit), {:ok, []}, fn entry, {:ok, acc} ->
          with :ok <- check(context),
               {:ok, value} <- stat_entry(entry, target, context, opts),
               do: {:cont, {:ok, [value | acc]}},
               else: (error -> {:halt, error})
        end)

      with {:ok, entries} <- result,
           do:
             {:ok,
              %{
                "Entries" => Enum.reverse(entries),
                "Returned" => min(length(rows), limit),
                "MatchedObjects" => inventory.count,
                "Partial" => inventory.count > limit
              }}
    end)
  end

  defp stat_entry(%{"type" => type} = entry, _, _, _) when type in ["folder", "delete-marker"],
    do: {:ok, entry}

  defp stat_entry(entry, target, context, opts) do
    input = Map.merge(opts[:input] || %{}, %{"Bucket" => target.bucket, "Key" => entry["key"]})

    input =
      if entry["version_id"], do: Map.put(input, "VersionId", entry["version_id"]), else: input

    with {:ok, head} <- call(context, "HeadObject", input, opts),
         :ok <- selected_version(head, input),
         :ok <- if(entry["historical"], do: historical_identity(entry, head), else: :ok),
         do: {:ok, Map.merge(entry, head)}
  end

  def tree(target, context, options, opts) do
    with :ok <- bounds(options),
         true <- (options[:depth] || -1) == -1 or (options[:depth] || -1) in 1..10_000 do
      with_inventory(target, context, Map.put(options, :recursive, true), opts, fn inventory ->
        entries =
          checked(inventory, context)
          |> Stream.reject(&(&1["type"] == "delete-marker"))
          |> Stream.flat_map(fn entry ->
            parts = String.split(entry["relative"], "/", trim: false)
            parents = max(length(parts) - 1, 0)
            depth = options[:depth] || -1

            dirs =
              if parents == 0,
                do: [],
                else:
                  for(
                    n <- 1..parents,
                    depth == -1 or n <= depth + 1,
                    do:
                      projection(
                        Enum.take(parts, n) |> Enum.join("/") |> Kernel.<>("/"),
                        "folder",
                        entry
                      )
                  )

            file =
              if options[:files] == true and List.last(parts) != "" and
                   (depth == -1 or parents <= depth),
                 do: [projection(entry["relative"], "file", entry)],
                 else: []

            dirs ++ file
          end)

        grouped(entries, context, opts, options[:limit] || 1000, fn group, _count, _bytes, type ->
          %{"Path" => group, "Type" => type}
        end)
      end)
    else
      false -> failure(:tree_depth, "Tree depth must be -1 or between 1 and 10000")
      error -> error
    end
  end

  def du(target, context, options, opts) do
    with :ok <- bounds(options),
         true <- (options[:depth] || 0) in 0..10_000 do
      options = Map.put(options, :all_rewind_versions, true)

      with_inventory(target, context, Map.put(options, :recursive, true), opts, fn inventory ->
        {objects, bytes} =
          checked(inventory, context)
          |> Enum.reduce({0, 0}, fn entry, {count, total} ->
            if entry["type"] == "file",
              do: {count + 1, total + entry["size"]},
              else: {count, total}
          end)

        entries =
          checked(inventory, context)
          |> Stream.reject(&(&1["type"] != "file"))
          |> Stream.flat_map(fn entry ->
            parts = String.split(entry["relative"], "/", trim: false) |> Enum.drop(-1)

            maximum =
              if options[:recursive] == true or (options[:depth] || 0) > 1,
                do:
                  if((options[:depth] || 0) == 0,
                    do: length(parts),
                    else: min(length(parts), options[:depth] - 1)
                  ),
                else: 0

            for n <- 0..maximum,
                do: projection(Enum.take(parts, n) |> Enum.join("/"), "prefix", entry)
          end)

        entries =
          Stream.concat(
            [
              %{
                "name" => JSON.encode!(["", "prefix", ""]),
                "group" => "",
                "type" => "prefix",
                "size" => 0,
                "objects" => 0
              }
            ],
            entries
          )

        with {:ok, result} <-
               grouped(entries, context, opts, options[:limit] || 1000, fn group,
                                                                           count,
                                                                           bytes,
                                                                           _ ->
                 %{"Prefix" => group, "Objects" => count, "Bytes" => bytes}
               end) do
          {:ok,
           Map.merge(result, %{
             "Objects" => objects,
             "Bytes" => bytes,
             "Versions" => options[:versions] == true
           })}
        end
      end)
    else
      false -> failure(:du_depth, "Summary depth must be between 0 and 10000")
      error -> error
    end
  end

  defp projection(group, type, entry),
    do: %{
      "name" => JSON.encode!([group, type, entry["name"]]),
      "group" => group,
      "type" => type,
      "size" => entry["size"] || 0
    }

  # Sorting derived records on disk makes equal projections adjacent without
  # keeping a map proportional to the number of remote prefixes.
  defp grouped(entries, context, opts, limit, format) do
    with {:ok, projected} <- Manifest.build(entries, opts) do
      try do
        state =
          Enum.reduce(
            checked(projected, context),
            %{group: nil, type: nil, count: 0, bytes: 0, rows: [], total: 0},
            fn entry, acc ->
              acc =
                if entry["group"] != acc.group or entry["type"] != acc.type,
                  do: finish_group(acc, limit, format),
                  else: acc

              %{
                acc
                | group: entry["group"],
                  type: entry["type"],
                  count: acc.count + Map.get(entry, "objects", 1),
                  bytes: acc.bytes + entry["size"]
              }
            end
          )
          |> finish_group(limit, format)

        {:ok,
         %{
           "Entries" => Enum.reverse(state.rows),
           "Returned" => min(state.total, limit),
           "MatchedPrefixes" => state.total,
           "Partial" => state.total > limit
         }}
      after
        Manifest.cleanup(projected)
      end
    end
  end

  defp finish_group(%{group: nil} = acc, _, _), do: acc

  defp finish_group(acc, limit, format) do
    rows =
      if acc.total < limit,
        do: [format.(acc.group, acc.count, acc.bytes, acc.type) | acc.rows],
        else: acc.rows

    %{acc | group: nil, type: nil, count: 0, bytes: 0, rows: rows, total: acc.total + 1}
  end

  defp with_inventory(target, context, options, opts, callback) do
    with {:ok, inventory} <- inventory(target, context, options, opts) do
      try do
        callback.(inventory)
      catch
        {:inspection_cancel, error} -> {:error, error}
      after
        Manifest.cleanup(inventory)
      end
    end
  end

  defp checked(inventory, context),
    do:
      Manifest.stream(inventory)
      |> Stream.map(fn entry ->
        case check(context) do
          :ok -> entry
          {:error, error} -> throw({:inspection_cancel, error})
        end
      end)

  defp inventory(
         %Target{kind: :remote, bucket: bucket} = target,
         context,
         %{rewind: rewind} = options,
         opts
       )
       when is_binary(bucket) do
    prefix = options[:prefix] || target.key || ""
    target = %{target | key: prefix}

    history =
      history_opts(context, opts)
      |> Keyword.put(:literal_prefix, true)
      |> Keyword.put(:page_size, options[:max_keys] || 1000)

    exact =
      options[:versions] == true and options[:recursive] != true and prefix != "" and
        not String.ends_with?(prefix, "/")

    history = Keyword.put(history, :exact, exact)

    with :ok <- rewind_options(options),
         {:ok, cutoff} <- VersionSelection.cutoff(rewind, opts),
         {:ok, versions} <- Inventory.versions(context, target, history) do
      try do
        if options[:versions] == true or options[:all_rewind_versions] == true do
          entries =
            checked(versions, context)
            |> Stream.flat_map(fn entry ->
              if is_integer(entry["modified"]) and entry["modified"] > 0 do
                if entry["modified"] * 1000 < cutoff,
                  do: [historical_entry(entry, prefix)],
                  else: []
              else
                [failure(:rewind_inventory, "Historical selection requires complete timestamps")]
              end
            end)

          Manifest.build(entries, opts)
        else
          with {:ok, selected} <-
                 VersionSelection.select(
                   versions,
                   cutoff,
                   Keyword.put(history, :include_delete_markers, true)
                 ) do
            try do
              Manifest.build(
                checked(selected, context) |> Stream.map(&historical_entry(&1, prefix)),
                opts
              )
            after
              Manifest.cleanup(selected)
            end
          end
        end
      catch
        {:inspection_cancel, error} -> {:error, error}
      after
        Manifest.cleanup(versions)
      end
    end
  end

  defp inventory(%Target{kind: :remote, bucket: bucket} = target, context, options, opts)
       when is_binary(bucket) do
    prefix = options[:prefix] || target.key || ""
    versions = options[:versions] == true
    operation = if versions, do: "ListObjectVersions", else: "ListObjectsV2"
    input = %{"Bucket" => bucket, "Prefix" => prefix}
    input = if options[:recursive] == true, do: input, else: Map.put(input, "Delimiter", "/")
    fetch = fn operation, input -> call(context, operation, input, opts) end

    exact =
      versions and options[:recursive] != true and prefix != "" and
        not String.ends_with?(prefix, "/")

    entries =
      Paginator.pages(operation, input, fetch, page_size: options[:max_keys] || 1000)
      |> Stream.flat_map(fn
        {:error, _} = error ->
          [error]

        {:ok, page} ->
          Enum.flat_map(
            [
              {"Contents", "file"},
              {"Versions", "file"},
              {"DeleteMarkers", "delete-marker"},
              {"CommonPrefixes", "folder"}
            ],
            fn {field, type} ->
              Enum.map(page[field] || [], &entry(&1, type, prefix, versions))
            end
          )
      end)
      |> Stream.filter(fn
        {:error, _} -> true
        {:ok, entry} -> not exact or entry["key"] == prefix
      end)

    Manifest.build(entries, opts)
  end

  defp inventory(_, _, _, _),
    do: failure(:inspection_target, "Inspection requires a remote bucket or prefix")

  defp historical_entry(entry, prefix) do
    if is_integer(entry["size"]) and entry["size"] >= 0 and
         (entry["delete_marker"] or (is_binary(entry["etag"]) and entry["etag"] != "")) do
      Map.merge(entry, %{
        "name" => JSON.encode!([entry["key"], entry["version_id"]]),
        "relative" => String.replace_prefix(entry["key"], prefix, ""),
        "type" => if(entry["delete_marker"], do: "delete-marker", else: "file"),
        "historical" => true
      })
    else
      failure(
        :rewind_inventory,
        "Historical selection requires complete size and entity metadata"
      )
    end
  end

  defp history_opts(context, opts),
    do:
      opts
      |> Keyword.put(:fetch, fn operation, input -> call(context, operation, input, opts) end)
      |> Keyword.put(:coordinator, context.coordinator)

  defp historical_identity(entry, head) do
    if is_binary(entry["etag"]) and entry["etag"] != "" and
         head["VersionId"] == entry["version_id"] and head["ETag"] == entry["etag"] and
         head["ContentLength"] == entry["size"] and
         modified(head["LastModified"]) == entry["modified"],
       do: :ok,
       else:
         failure(
           :source_version_mismatch,
           "Provider did not confirm the listed historical object identity"
         )
  end

  defp modified(%DateTime{} = value), do: DateTime.to_unix(value, :microsecond)

  defp modified(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, time, _} -> modified(time)
      _ -> nil
    end
  end

  defp modified(_), do: nil

  defp rewind_options(options) do
    if options[:rewind] != nil and options[:version_id] != nil,
      do: failure(:version_selection, "Rewind conflicts with an explicit version identifier"),
      else: :ok
  end

  defp entry(record, type, prefix, versions) do
    key = record["Key"] || record["Prefix"]
    version = record["VersionId"]

    if is_binary(key) and String.valid?(key) and String.starts_with?(key, prefix) and
         (not versions or type == "folder" or (is_binary(version) and version != "")) and
         (type != "file" or (is_integer(record["Size"]) and record["Size"] >= 0)) do
      {:ok,
       %{
         "name" => JSON.encode!([key, version, type]),
         "key" => key,
         "version_id" => version,
         "relative" => String.replace_prefix(key, prefix, ""),
         "type" => type,
         "size" => record["Size"] || 0,
         "modified" => record["LastModified"]
       }}
    else
      failure(:inspection_inventory, "Provider returned an invalid inspection entry")
    end
  end

  defp bounds(options) do
    if (options[:limit] || 1000) in 0..10_000 and (options[:max_keys] || 1000) in 1..1000,
      do: :ok,
      else: failure(:inspection_bounds, "Inspection limit must be 0..10000 and page size 1..1000")
  end

  defp version_options(target, options) do
    if options[:version_id] != nil and
         (options[:versions] == true or options[:recursive] == true or target.key in [nil, ""] or
            (String.ends_with?(target.key, "/") and options[:no_list] != true)),
       do:
         failure(
           :version_selection,
           "An explicit version requires one object and conflicts with recursive or all-version inspection"
         ),
       else: :ok
  end

  defp selected_version(head, %{"VersionId" => version}) do
    if head["VersionId"] == version,
      do: :ok,
      else:
        failure(:source_version_mismatch, "Provider did not confirm the selected object version")
  end

  defp selected_version(_, _), do: :ok

  defp call(context, operation, input, opts) do
    with :ok <- check(context),
         do:
           (opts[:call] || opts[:fetch] ||
              fn operation, input -> Client.call(context, operation, input, opts) end).(
             operation,
             input
           )
  end

  defp check(context) do
    receive do
      {:ess_cancel, _} ->
        {:error, Error.new(:interrupted, "Inspection interrupted", code: :cancelled)}
    after
      0 ->
        if context && context.coordinator, do: Coordinator.check(context.coordinator), else: :ok
    end
  end

  defp failure(code, message), do: {:error, Error.new(:syntax, message, code: code)}
end
