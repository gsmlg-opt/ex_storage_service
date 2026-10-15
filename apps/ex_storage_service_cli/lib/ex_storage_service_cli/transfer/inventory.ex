defmodule ExStorageServiceCli.Transfer.Inventory do
  @moduledoc "Complete paginated remote and bounded native directory inventories before effects."
  alias ExStorageServiceCli.{Error, Target, S3.Client, S3.Paginator}
  alias ExStorageServiceCli.CLI.Signals.Native
  alias ExStorageServiceCli.Transfer.{Manifest, Sink}

  def remote(context, %Target{} = target, opts \\ []) do
    prefix = prefix(target.key)
    input = Map.merge(opts[:input] || %{}, %{"Bucket" => target.bucket, "Prefix" => prefix})

    fetch =
      opts[:fetch] || fn operation, input -> Client.call(context, operation, input, opts) end

    entries =
      Paginator.pages("ListObjectsV2", input, fetch, page_size: opts[:page_size] || 1000)
      |> Stream.flat_map(fn
        {:error, _} = error -> [error]
        {:ok, page} -> Enum.map(page["Contents"] || [], &remote_entry(&1, prefix))
      end)

    Manifest.build(entries, opts)
  end

  @doc "Complete version and delete-marker inventory; each immutable identifier remains distinct."
  def versions(context, %Target{} = target, opts \\ []) do
    prefix =
      if opts[:exact] == true or opts[:literal_prefix] == true,
        do: target.key || "",
        else: prefix(target.key)

    input = Map.merge(opts[:input] || %{}, %{"Bucket" => target.bucket, "Prefix" => prefix})

    fetch =
      opts[:fetch] || fn operation, input -> Client.call(context, operation, input, opts) end

    entries =
      Paginator.pages("ListObjectVersions", input, fetch, page_size: opts[:page_size] || 1000)
      |> Stream.flat_map(fn
        {:error, _} = error ->
          [error]

        {:ok, page} ->
          Enum.map(page["Versions"] || [], &Map.put(&1, "marker", false)) ++
            Enum.map(page["DeleteMarkers"] || [], &Map.put(&1, "marker", true))
      end)
      |> Stream.filter(fn
        {:error, _} -> true
        record -> opts[:exact] != true or record["Key"] == target.key
      end)
      |> Stream.map(fn
        {:error, _} = error ->
          error

        record ->
          key = record["Key"]
          version = record["VersionId"]

          if is_binary(key) and String.valid?(key) and String.starts_with?(key, prefix) and
               is_binary(version) and version != "" and byte_size(version) <= 8192 do
            {:ok,
             %{
               "name" => JSON.encode!([key, version]),
               "object_name" =>
                 if(opts[:exact],
                   do: key |> String.split("/") |> List.last(),
                   else: String.replace_prefix(key, prefix, "")
                 ),
               "key" => key,
               "version_id" => version,
               "delete_marker" => record["marker"],
               "modified" => modified(record["LastModified"]),
               "etag" => record["ETag"],
               "size" => record["Size"] || 0
             }}
          else
            failure(:invalid_inventory, "Provider returned an invalid version inventory entry")
          end
      end)

    Manifest.build(entries, opts)
  end

  def local(root, opts \\ []) do
    Manifest.build(local_entries(Path.expand(root), opts), opts)
  end

  def local_entries(root, opts \\ []) do
    Stream.resource(fn -> start(root, opts) end, &next/1, &close/1)
  end

  def collision_check(manifest, opts \\ []) do
    entries =
      Manifest.stream(manifest)
      |> Stream.map(fn entry ->
        Map.put(
          entry,
          "collision",
          entry["name"]
          |> String.normalize(:nfc)
          |> String.downcase()
          |> String.replace("/", <<0>>)
        )
      end)

    case Manifest.build(entries, Keyword.put(opts, :key, "collision")) do
      {:ok, collision} ->
        result =
          Manifest.stream(collision)
          |> Enum.reduce_while(nil, fn entry, previous ->
            name = entry["collision"]

            if previous != nil and String.starts_with?(name, previous <> <<0>>),
              do:
                {:halt,
                 failure(
                   :inventory_collision,
                   "Remote keys select a file and its child directory"
                 )},
              else: {:cont, name}
          end)

        Manifest.cleanup(collision)

        case result do
          {:error, _} = error -> error
          _ -> :ok
        end

      error ->
        error
    end
  end

  def local_entry(path, name) do
    case File.stat(path, time: :posix) do
      {:ok, %{type: :regular} = stat} ->
        {:ok,
         %{
           "name" => name,
           "path" => path,
           "size" => stat.size,
           "modified" => stat.mtime * 1_000_000,
           "identity" =>
             Map.new(
               Map.take(stat, [:major_device, :minor_device, :inode, :size, :mtime, :ctime]),
               fn {k, v} -> {Atom.to_string(k), v} end
             )
         }}

      _ ->
        failure(:local_inventory, "Source is not a readable regular file")
    end
  end

  def verify_local(entry) do
    with {:ok, current} <- local_entry(entry["path"], entry["name"]) do
      if current["identity"] == entry["identity"],
        do: :ok,
        else: failure(:inventory_changed, "Local inventory candidate changed")
    end
  end

  def verify_remote(context, target, entry, opts \\ []) do
    input = Map.merge(opts[:input] || %{}, %{"Bucket" => target.bucket, "Key" => entry["key"]})

    input =
      if entry["version_id"], do: Map.put(input, "VersionId", entry["version_id"]), else: input

    with {:ok, head} <- Client.call(context, "HeadObject", input, opts) do
      if head["ContentLength"] == entry["size"] and head["ETag"] == entry["etag"] and
           (entry["version_id"] == nil or
              (head["VersionId"] == entry["version_id"] and
                 modified(head["LastModified"]) == entry["modified"])),
         do: {:ok, head},
         else: failure(:inventory_changed, "Remote inventory candidate changed")
    end
  end

  def prefix(""), do: ""
  def prefix(key), do: String.trim_trailing(key, "/") <> "/"

  defp remote_entry(record, prefix) do
    key = record["Key"]

    if is_binary(key) and String.valid?(key) and String.starts_with?(key, prefix) and
         is_integer(record["Size"]) do
      name = String.replace_prefix(key, prefix, "")
      # Directory marker objects are retained; mapping them to a file must fail
      # explicitly at the local publication boundary rather than silently vanish.
      {:ok,
       %{
         "name" => name,
         "key" => key,
         "size" => record["Size"],
         "modified" => modified(record["LastModified"]),
         "etag" => record["ETag"],
         "storage_class" => record["StorageClass"]
       }}
    else
      failure(:invalid_inventory, "Provider returned an invalid inventory entry")
    end
  end

  defp modified(%DateTime{} = value), do: DateTime.to_unix(value, :microsecond)

  defp modified(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, date, _} -> DateTime.to_unix(date, :microsecond)
      _ -> 0
    end
  end

  defp modified(_), do: 0

  defp start(root, opts) do
    follow = Keyword.get(opts, :follow_symlinks, true)

    case File.stat(root, time: :posix) do
      {:ok, %{type: :directory} = stat} ->
        case Native.open_directory(root, follow) do
          {:ok, resource} ->
            %{
              root: root,
              follow: follow,
              stack: [
                %{
                  path: root,
                  resource: resource,
                  entries: [],
                  ancestors: MapSet.new([inode(stat)])
                }
              ]
            }

          _ ->
            {:error,
             Error.new(:client, "Cannot open local inventory directory", code: :local_inventory)}
        end

      _ ->
        {:error,
         Error.new(:client, "Recursive source must be a readable directory",
           code: :local_inventory
         )}
    end
  end

  defp next({:error, error}), do: {[{:error, error}], :done}
  defp next({:failed, state}), do: {:halt, state}
  defp next(:done), do: {:halt, :done}
  defp next(%{stack: []} = state), do: {:halt, state}

  defp next(%{stack: [%{entries: []} = frame | rest]} = state) do
    case Native.read_directory(frame.resource, 128) do
      {:ok, names} ->
        next(%{state | stack: [%{frame | entries: names} | rest]})

      :eof ->
        Native.close_directory(frame.resource)
        next(%{state | stack: rest})

      _ ->
        fail_stream(state, :local_inventory, "Cannot completely enumerate a source directory")
    end
  end

  defp next(%{stack: [%{entries: [name | names]} = frame | rest]} = state) do
    state = %{state | stack: [%{frame | entries: names} | rest]}
    path = Path.join(frame.path, name)

    if not is_binary(name) or not String.valid?(name) or String.contains?(name, ["/", <<0>>]) do
      fail_stream(state, :local_inventory, "Local directory contains an invalid file name")
    else
      case File.lstat(path, time: :posix) do
        {:ok, %{type: :symlink}} when not state.follow ->
          next(state)

        {:ok, %{type: :symlink}} ->
          follow(state, frame, path)

        {:ok, stat} ->
          visit(state, frame, path, stat)

        _ ->
          fail_stream(state, :local_inventory, "Source entry disappeared or cannot be inspected")
      end
    end
  end

  defp follow(state, frame, path) do
    case File.stat(path, time: :posix) do
      {:ok, stat} -> visit(state, frame, path, stat)
      _ -> fail_stream(state, :local_inventory, "Cannot resolve source symlink")
    end
  end

  defp visit(state, frame, path, %{type: :directory} = stat) do
    if MapSet.member?(frame.ancestors, inode(stat)) or length(state.stack) >= 128 do
      fail_stream(
        state,
        :symlink_loop,
        "Source traversal contains a directory loop or exceeds its depth bound"
      )
    else
      case Native.open_directory(path, state.follow) do
        {:ok, resource} ->
          next(%{
            state
            | stack: [
                %{
                  path: path,
                  resource: resource,
                  entries: [],
                  ancestors: MapSet.put(frame.ancestors, inode(stat))
                }
                | state.stack
              ]
          })

        _ ->
          fail_stream(state, :local_inventory, "Cannot open source directory")
      end
    end
  end

  defp visit(state, _frame, path, %{type: :regular}) do
    relative = Path.relative_to(path, state.root)

    case Sink.map(state.root, relative) do
      {:ok, _} -> {[local_entry(path, relative)], state}
      {:error, _} = error -> {[error], state}
    end
  end

  defp visit(state, _, _, _),
    do: fail_stream(state, :unsupported_local_entry, "Source contains a non-regular file")

  defp fail_stream(state, code, message), do: {[failure(code, message)], {:failed, state}}
  defp close(%{stack: frames}), do: Enum.each(frames, &Native.close_directory(&1.resource))
  defp close({:failed, state}), do: close(state)
  defp close(_), do: :ok
  defp inode(stat), do: {stat.major_device, stat.minor_device, stat.inode}
  defp failure(code, message), do: {:error, Error.new(:client, message, code: code)}
end
