defmodule ExStorageServiceCli.CLI.NativeListing do
  @moduledoc "Bounded native inspection with explicit partial-result metadata and streamed NDJSON."
  alias ExStorageServiceCli.{Error, Presenter}
  alias ExStorageServiceCli.CLI.NativeEvents
  alias ExStorageServiceCli.Presentation.Scalar
  alias ExStorageServiceCli.S3.{Client, Paginator}

  def run(invocation, target, context, opts \\ []) do
    limit = invocation.options[:limit] || 1000
    page_size = invocation.options[:max_keys] || 1000

    cond do
      invocation.options[:incomplete] == true and
          (invocation.options[:versions] == true or invocation.options[:rewind] != nil) ->
        {:error,
         Error.new(:syntax, "Incomplete uploads cannot be combined with versions or rewind")}

      Enum.any?([:incomplete, :summarize, :storage_class], &Map.has_key?(invocation.options, &1)) and
          not match?(%{kind: :remote, bucket: bucket} when is_binary(bucket), target) ->
        {:error, Error.new(:syntax, "These listing options require a remote bucket or prefix")}

      limit not in 0..10_000 or page_size not in 1..1000 ->
        {:error, Error.new(:syntax, "Listing limit must be 0..10000 and page size 1..1000")}

      true ->
        list(invocation, target, context, limit, page_size, opts)
    end
  end

  @doc "List bounded immutable versions, delete markers and immediate common prefixes."
  def versions(invocation, target, context, opts \\ []) do
    options = invocation.options
    limit = options[:limit] || 1000
    page_size = options[:max_keys] || 1000

    with :ok <- version_options(target, options, limit, page_size) do
      prefix = options[:prefix] || target.key || ""
      input = %{"Bucket" => target.bucket, "Prefix" => prefix}

      input =
        if options[:recursive] == true,
          do: input,
          else: Map.put(input, "Delimiter", options[:delimiter] || "/")

      fetch =
        opts[:fetch] || fn operation, input -> Client.call(context, operation, input, opts) end

      entries =
        Paginator.pages("ListObjectVersions", input, fetch, page_size: page_size)
        |> Stream.flat_map(fn
          {:error, _} = error -> [error]
          {:ok, page} -> version_rows(page, prefix, options)
        end)

      result =
        Enum.reduce_while(entries, {:ok, [], 0, false, 0}, fn
          {:error, _} = error, _ ->
            {:halt, error}

          {:ok, _}, {:ok, rows, ^limit, _, bytes} ->
            {:halt, {:ok, rows, limit, true, bytes}}

          {:ok, row}, {:ok, rows, count, _, bytes} ->
            {:cont, {:ok, [row | rows], count + 1, false, bytes + (row["size"] || 0)}}
        end)

      with {:ok, rows, count, partial, bytes} <- result do
        data = %{"Entries" => Enum.reverse(rows), "Returned" => count, "Partial" => partial}
        data = summarize(data, options, count, bytes, partial)
        present_versions(data, target, options, opts)
      end
    end
  end

  defp version_options(target, options, limit, page_size) do
    cond do
      options[:incomplete] == true ->
        {:error, Error.new(:syntax, "Incomplete uploads cannot be combined with versions")}

      not match?(%{kind: :remote, bucket: bucket} when is_binary(bucket), target) ->
        {:error, Error.new(:syntax, "Version listing requires a remote bucket or prefix")}

      options[:rewind] != nil ->
        {:error, Error.new(:syntax, "Historical listings require the rewind selection adapter")}

      limit not in 0..10_000 or page_size not in 1..1000 ->
        {:error, Error.new(:syntax, "Listing limit must be 0..10000 and page size 1..1000")}

      true ->
        :ok
    end
  end

  defp version_rows(page, prefix, options) do
    Enum.flat_map([{"Versions", false}, {"DeleteMarkers", true}, {"CommonPrefixes", :prefix}], fn
      {field, marker} ->
        (page[field] || [])
        |> Enum.filter(&selected?(&1, options))
        |> Enum.map(&version_row(&1, prefix, marker))
    end)
  end

  defp version_row(%{"Prefix" => key}, prefix, :prefix) do
    if is_binary(key) and String.valid?(key) and String.starts_with?(key, prefix),
      do: {:ok, %{"prefix" => key, "common_prefix" => true}},
      else: invalid_version()
  end

  defp version_row(record, prefix, marker) when is_boolean(marker) do
    key = record["Key"]
    version = record["VersionId"]
    size = record["Size"] || 0

    if is_binary(key) and String.valid?(key) and String.starts_with?(key, prefix) and
         is_binary(version) and version != "" and byte_size(version) <= 8192 and
         is_integer(size) and size >= 0 do
      {:ok,
       %{
         "name" => JSON.encode!([key, version]),
         "object_name" => String.replace_prefix(key, prefix, ""),
         "key" => key,
         "version_id" => version,
         "delete_marker" => marker,
         "modified" => modified(record["LastModified"]),
         "etag" => record["ETag"],
         "size" => size
       }}
    else
      invalid_version()
    end
  end

  defp version_row(_, _, _), do: invalid_version()

  defp invalid_version,
    do:
      {:error,
       Error.new(:protocol, "Provider returned an invalid version listing entry",
         code: :invalid_version_listing
       )}

  defp modified(%DateTime{} = date), do: DateTime.to_unix(date, :microsecond)

  defp modified(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, date, _} -> DateTime.to_unix(date, :microsecond)
      _ -> 0
    end
  end

  defp modified(_), do: 0

  defp present_versions(data, target, options, opts) do
    cond do
      options[:output] == "ndjson" ->
        with :ok <-
               Enum.reduce_while(data["Entries"], :ok, fn row, :ok ->
                 case NativeEvents.item(row, options) do
                   :ok -> {:cont, :ok}
                   error -> {:halt, error}
                 end
               end),
             do: NativeEvents.item(Map.delete(data, "Entries"), options)

      options[:json] == true or options[:output] not in [nil, "off"] ->
        {:ok, data}

      true ->
        unless options[:quiet] == true or options[:output] == "off" do
          Enum.each(data["Entries"], fn row ->
            path = "s3://" <> target.bucket <> "/" <> (row["key"] || row["prefix"])

            suffix =
              if row["common_prefix"],
                do: "",
                else:
                  " (version " <>
                    Scalar.safe(row["version_id"]) <>
                    ")" <> if(row["delete_marker"], do: " [delete marker]", else: "")

            (opts[:emit] || (&IO.puts/1)).(Scalar.safe(path) <> suffix)
          end)
        end

        if data["Partial"] do
          (opts[:stderr] || fn text -> IO.puts(:stderr, text) end).(
            "Version listing is partial; increase --limit (maximum10000)"
          )
        end

        if summary = data["Summary"] do
          unless options[:quiet] == true or options[:output] == "off" do
            (opts[:emit] || (&IO.puts/1)).(
              "Listed Entries: #{summary["Entries"]}; Listed Bytes: #{summary["Bytes"]}; Complete: #{summary["Complete"]}"
            )
          end
        end

        :ok
    end
  end

  defp list(invocation, target, context, limit, page_size, opts) do
    {operation, input, keys} = request(invocation, target)

    fetch =
      opts[:fetch] || fn operation, input -> Client.call(context, operation, input, opts) end

    entries =
      Paginator.pages(operation, input, fetch, page_size: page_size)
      |> Stream.flat_map(fn
        {:ok, page} -> Enum.flat_map(keys, &Map.get(page, &1, []))
        {:error, _} = error -> [error]
      end)
      |> Stream.map(&validate_entry(&1, input, invocation.options))
      |> Stream.filter(fn
        {:error, _} -> true
        row -> selected?(row, invocation.options)
      end)

    result =
      Enum.reduce_while(entries, {:ok, [], 0, false, 0}, fn
        {:error, _} = error, _ ->
          {:halt, error}

        _entry, {:ok, rows, ^limit, _, bytes} ->
          {:halt, {:ok, rows, limit, true, bytes}}

        entry, {:ok, rows, count, _, bytes} ->
          bytes = bytes + (entry["Size"] || 0)

          if invocation.options[:output] == "ndjson" do
            case NativeEvents.item(entry, invocation.options) do
              :ok -> {:cont, {:ok, rows, count + 1, false, bytes}}
              error -> {:halt, error}
            end
          else
            {:cont, {:ok, [entry | rows], count + 1, false, bytes}}
          end
      end)

    with {:ok, rows, count, partial, bytes} <- result do
      data = %{"Returned" => count, "Partial" => partial}
      data = summarize(data, invocation.options, count, bytes, partial)

      if invocation.options[:output] == "ndjson",
        do: NativeEvents.item(data, invocation.options),
        else: Presenter.write(Map.put(data, "Entries", Enum.reverse(rows)), invocation.options)
    end
  end

  defp request(_invocation, nil), do: {"ListBuckets", %{}, ["Buckets"]}
  defp request(_invocation, %{bucket: nil}), do: {"ListBuckets", %{}, ["Buckets"]}

  defp request(invocation, target) do
    input = %{
      "Bucket" => target.bucket,
      "Prefix" => invocation.options[:prefix] || target.key || ""
    }

    input =
      if invocation.command == ["tree"] or invocation.options[:recursive],
        do: input,
        else: Map.put(input, "Delimiter", invocation.options[:delimiter] || "/")

    if invocation.options[:incomplete] == true,
      do: {"ListMultipartUploads", input, ["Uploads", "CommonPrefixes"]},
      else: {"ListObjectsV2", input, ["Contents", "CommonPrefixes"]}
  end

  defp selected?(row, options) do
    class = row["StorageClass"]
    filter = options[:storage_class]
    class in [nil, ""] or filter in [nil, "", "*"] or class == filter
  end

  defp summarize(data, %{summarize: true} = options, count, bytes, partial) do
    Map.put(data, "Summary", %{
      "Entries" => count,
      "Bytes" => if(options[:incomplete] == true, do: nil, else: bytes),
      "Complete" => not partial
    })
  end

  defp summarize(data, _, _, _, _), do: data

  defp validate_entry({:error, _} = error, _, _), do: error

  defp validate_entry(row, input, %{incomplete: true}) do
    key = row["Key"] || row["Prefix"]
    valid = is_binary(key) and String.valid?(key) and String.starts_with?(key, input["Prefix"])
    upload = row["UploadId"]

    if valid and
         (Map.has_key?(row, "Prefix") or
            (is_binary(upload) and upload != "" and byte_size(upload) <= 8192)),
       do: row,
       else:
         {:error,
          Error.new(:protocol, "Provider returned an invalid incomplete upload entry",
            code: :invalid_upload_listing
          )}
  end

  defp validate_entry(row, _, _), do: row
end
