defmodule ExStorageServiceCli.Commands.Tree do
  @moduledoc """
  Render S3 object keys as a directory tree.
  """

  alias ExStorageServiceCli.Output
  alias ExStorageServiceCli.S3Client

  def run(_command, args, ctx) do
    {opts, rest, _} =
      OptionParser.parse(args,
        strict: [
          prefix: :string,
          max_keys: :integer,
          help: :boolean
        ],
        aliases: [h: :help]
      )

    if opts[:help] do
      help("tree")
    else
      client = S3Client.new(ctx)

      case rest do
        [target] ->
          {bucket, prefix} = parse_target(target)
          prefix = opts[:prefix] || prefix
          max_keys = opts[:max_keys] || 1000

          render_tree(client, bucket, prefix, max_keys, ctx)

        [] ->
          Output.error("Missing target. Usage: ess tree s3://bucket[/prefix]")
          System.halt(1)

        _ ->
          Output.error("Too many arguments")
          help("tree")
          System.halt(1)
      end
    end
  end

  def help(_command) do
    IO.puts("""
    #{IO.ANSI.bright()}ess tree#{IO.ANSI.reset()} — Display objects as a directory tree

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess tree <bucket>
        ess tree <bucket>/<prefix>
        ess tree s3://<bucket>/<prefix>

    #{IO.ANSI.bright()}OPTIONS#{IO.ANSI.reset()}
        --prefix <prefix>     Filter by prefix
        --max-keys <n>        Page size for ListObjectsV2 requests (default: 1000)
        --json                Output tree lines and objects in JSON format

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess tree my-bucket
        ess tree my-bucket/images/
        ess tree s3://my-bucket/docs/
    """)
  end

  @doc """
  Formats object keys as printable tree lines.
  """
  def format_tree(bucket, prefix, objects) do
    root = format_root(bucket, prefix)

    tree =
      objects
      |> Enum.map(&object_key/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.map(&relative_key(&1, prefix))
      |> Enum.reject(&(&1 == ""))
      |> Enum.reduce(empty_node(), &put_key/2)

    {lines, directories, files} = render_node(tree, "")

    [root | lines] ++ ["", summary(directories, files)]
  end

  defp render_tree(client, bucket, prefix, max_keys, ctx) do
    case list_all_objects(client, bucket, prefix, max_keys) do
      {:ok, objects} ->
        lines = format_tree(bucket, prefix, objects)
        data = %{bucket: bucket, prefix: prefix, tree: lines, objects: objects}

        Output.render(data, ctx, fn _data ->
          Enum.each(lines, &IO.puts/1)
        end)

      {:error, reason} ->
        Output.error("Failed to list objects in '#{bucket}': #{Output.format_error(reason)}")
        System.halt(1)
    end
  end

  defp list_all_objects(client, bucket, prefix, max_keys) do
    list_all_objects_paginated(client, bucket, prefix, max_keys, nil, [])
  end

  defp list_all_objects_paginated(client, bucket, prefix, max_keys, token, acc) do
    opts = [prefix: prefix, max_keys: max_keys]
    opts = if token, do: opts ++ [continuation_token: token], else: opts

    case S3Client.list_objects(client, bucket, opts) do
      {:ok, %{contents: contents, is_truncated: true, next_continuation_token: next_token}}
      when is_binary(next_token) and next_token != "" ->
        list_all_objects_paginated(client, bucket, prefix, max_keys, next_token, acc ++ contents)

      {:ok, %{contents: contents}} ->
        {:ok, acc ++ contents}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_target(target) do
    target = String.trim_leading(target, "s3://")

    case String.split(target, "/", parts: 2) do
      [bucket] -> {bucket, ""}
      [bucket, prefix] -> {bucket, prefix}
    end
  end

  defp object_key(%{key: key}) when is_binary(key), do: key
  defp object_key(%{"key" => key}) when is_binary(key), do: key
  defp object_key(_object), do: nil

  defp relative_key(key, prefix) when prefix in ["", nil], do: key

  defp relative_key(key, prefix) do
    key
    |> String.trim_leading(prefix)
    |> String.trim_leading("/")
  end

  defp format_root(bucket, prefix) when prefix in ["", nil], do: "s3://#{bucket}/"

  defp format_root(bucket, prefix) do
    suffix = if String.ends_with?(prefix, "/"), do: prefix, else: prefix <> "/"
    "s3://#{bucket}/#{suffix}"
  end

  defp empty_node, do: %{dirs: %{}, files: MapSet.new()}

  defp put_key(key, node) do
    segments = String.split(key, "/", trim: true)

    cond do
      segments == [] ->
        node

      String.ends_with?(key, "/") ->
        put_dir_path(node, segments)

      true ->
        put_file_path(node, segments)
    end
  end

  defp put_file_path(node, [file]) do
    %{node | files: MapSet.put(node.files, file)}
  end

  defp put_file_path(node, [dir | rest]) do
    update_dir(node, dir, &put_file_path(&1, rest))
  end

  defp put_dir_path(node, [dir]) do
    update_dir(node, dir, & &1)
  end

  defp put_dir_path(node, [dir | rest]) do
    update_dir(node, dir, &put_dir_path(&1, rest))
  end

  defp update_dir(node, name, fun) do
    child = Map.get(node.dirs, name, empty_node())
    %{node | dirs: Map.put(node.dirs, name, fun.(child))}
  end

  defp render_node(node, indent) do
    entries =
      Enum.map(node.dirs, fn {name, child} -> {:dir, name, child} end) ++
        Enum.map(node.files, fn name -> {:file, name, nil} end)

    sorted_entries = Enum.sort_by(entries, fn {_type, name, _child} -> name end)
    total = length(sorted_entries)

    sorted_entries
    |> Enum.with_index()
    |> Enum.reduce({[], 0, 0}, fn {{type, name, child}, index}, {lines, dir_count, file_count} ->
      last? = index == total - 1
      connector = if last?, do: "└──", else: "├──"
      child_indent = if last?, do: indent <> "    ", else: indent <> "│   "

      case type do
        :dir ->
          {child_lines, child_dirs, child_files} = render_node(child, child_indent)

          {
            lines ++ ["#{indent}#{connector} #{name}/"] ++ child_lines,
            dir_count + child_dirs + 1,
            file_count + child_files
          }

        :file ->
          {lines ++ ["#{indent}#{connector} #{name}"], dir_count, file_count + 1}
      end
    end)
  end

  defp summary(1, 1), do: "1 directory, 1 file"
  defp summary(1, files), do: "1 directory, #{files} files"
  defp summary(directories, 1), do: "#{directories} directories, 1 file"
  defp summary(directories, files), do: "#{directories} directories, #{files} files"
end
