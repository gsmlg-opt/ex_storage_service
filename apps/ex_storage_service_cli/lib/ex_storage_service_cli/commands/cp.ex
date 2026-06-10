defmodule ExStorageServiceCli.Commands.Cp do
  @moduledoc """
  Copy files between local filesystem and S3.

  Supports:
    - Local → S3 (upload)
    - S3 → Local (download)
    - S3 → S3 (copy)
  """

  alias ExStorageServiceCli.S3Client
  alias ExStorageServiceCli.Output

  def run(_command, args, ctx) do
    {opts, rest, _} =
      OptionParser.parse(args,
        strict: [
          content_type: :string,
          recursive: :boolean,
          help: :boolean
        ],
        aliases: [r: :recursive, h: :help]
      )

    if opts[:help] do
      help("cp")
    else
      case rest do
        [src, dst] ->
          client = S3Client.new(ctx)
          execute_copy(client, src, dst, opts, ctx)

        _ ->
          Output.error("Expected exactly 2 arguments: <source> <destination>")
          help("cp")
          System.halt(1)
      end
    end
  end

  def help(_command) do
    IO.puts("""
    #{IO.ANSI.bright()}ess cp#{IO.ANSI.reset()} — Copy files between local and S3

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess cp <source> <destination> [options]

    #{IO.ANSI.bright()}COPY DIRECTIONS#{IO.ANSI.reset()}
        Local → S3:   ess cp ./file.txt s3://bucket/key
        S3 → Local:   ess cp s3://bucket/key ./file.txt
        S3 → S3:      ess cp s3://bucket1/key1 s3://bucket2/key2

    #{IO.ANSI.bright()}OPTIONS#{IO.ANSI.reset()}
        --content-type <type>   Set content type (upload only)
        -r, --recursive         Copy directories recursively
        -h, --help              Show help

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess cp ./photo.jpg s3://my-bucket/images/photo.jpg
        ess cp s3://my-bucket/data.csv ./data.csv
        ess cp -r ./folder s3://my-bucket/folder
        ess cp -r s3://my-bucket/folder ./folder
        ess cp -r s3://bucket1/folder/ s3://bucket2/dest/
    """)
  end

  defp execute_copy(client, src, dst, opts, ctx) do
    src_s3? = String.starts_with?(src, "s3://")
    dst_s3? = String.starts_with?(dst, "s3://")
    recursive? = Keyword.get(opts, :recursive, false)

    cond do
      !src_s3? && dst_s3? ->
        if File.dir?(src) && !recursive? do
          Output.error("Directory copy requires --recursive flag: #{src}")
          System.halt(1)
        end

        if recursive? do
          upload_recursive(client, src, dst, opts, ctx)
        else
          upload(client, src, dst, opts, ctx)
        end

      src_s3? && !dst_s3? ->
        if recursive? do
          download_recursive(client, src, dst, ctx)
        else
          download(client, src, dst, ctx)
        end

      src_s3? && dst_s3? ->
        if recursive? do
          s3_copy_recursive(client, src, dst, ctx)
        else
          s3_copy(client, src, dst, ctx)
        end

      true ->
        Output.error("At least one argument must be an S3 path (s3://bucket/key)")
        System.halt(1)
    end
  end

  defp upload(client, local_path, s3_path, opts, ctx) do
    unless File.exists?(local_path) do
      Output.error("Local file not found: #{local_path}")
      System.halt(1)
    end

    {bucket, key} = parse_s3_path(s3_path)

    # If the S3 key ends with /, append the filename
    key =
      if String.ends_with?(key, "/") || key == "" do
        key <> Path.basename(local_path)
      else
        key
      end

    body = File.read!(local_path)
    file_size = byte_size(body)

    upload_opts =
      if opts[:content_type] do
        [content_type: opts[:content_type]]
      else
        []
      end

    case S3Client.put_object(client, bucket, key, body, upload_opts) do
      {:ok, result} ->
        if ctx.json do
          IO.puts(
            JSON.encode!(%{
              status: "ok",
              operation: "upload",
              source: local_path,
              destination: s3_path,
              size: file_size,
              etag: result.etag
            })
          )
        else
          Output.success(
            "Uploaded #{local_path} → s3://#{bucket}/#{key} (#{Output.format_bytes(file_size)})"
          )
        end

      {:error, reason} ->
        Output.error("Upload failed: #{Output.format_error(reason)}")
        System.halt(1)
    end
  end

  defp upload_recursive(client, local_path, s3_path, opts, ctx) do
    if File.regular?(local_path) do
      upload(client, local_path, s3_path, opts, ctx)
    else
      files = list_local_files_recursive(local_path)

      if files == [] do
        Output.info("No files to upload in #{local_path}")
      else
        {bucket, dst_prefix} = parse_s3_path(s3_path)

        Enum.each(files, fn file ->
          rel_path = relative_local_path(file, local_path)

          dst_key =
            if dst_prefix == "" do
              rel_path
            else
              prefix = String.trim_trailing(dst_prefix, "/")
              prefix <> "/" <> rel_path
            end

          body = File.read!(file)
          file_size = byte_size(body)

          upload_opts =
            if opts[:content_type] do
              [content_type: opts[:content_type]]
            else
              []
            end

          case S3Client.put_object(client, bucket, dst_key, body, upload_opts) do
            {:ok, result} ->
              if ctx.json do
                IO.puts(
                  JSON.encode!(%{
                    status: "ok",
                    operation: "upload",
                    source: file,
                    destination: "s3://#{bucket}/#{dst_key}",
                    size: file_size,
                    etag: result.etag
                  })
                )
              else
                Output.success(
                  "Uploaded #{file} → s3://#{bucket}/#{dst_key} (#{Output.format_bytes(file_size)})"
                )
              end

            {:error, reason} ->
              Output.error("Upload failed for #{file}: #{Output.format_error(reason)}")
              System.halt(1)
          end
        end)
      end
    end
  end

  defp download(client, s3_path, local_path, ctx) do
    {bucket, key} = parse_s3_path(s3_path)

    # If local path is a directory, append the object key basename
    local_path =
      if File.dir?(local_path) do
        Path.join(local_path, Path.basename(key))
      else
        local_path
      end

    case S3Client.get_object(client, bucket, key) do
      {:ok, result} ->
        # Ensure parent directory exists
        local_path |> Path.dirname() |> File.mkdir_p!()
        File.write!(local_path, result.body)
        file_size = byte_size(result.body)

        if ctx.json do
          IO.puts(
            JSON.encode!(%{
              status: "ok",
              operation: "download",
              source: s3_path,
              destination: local_path,
              size: file_size
            })
          )
        else
          Output.success(
            "Downloaded s3://#{bucket}/#{key} → #{local_path} (#{Output.format_bytes(file_size)})"
          )
        end

      {:error, :not_found} ->
        Output.error("Object not found: s3://#{bucket}/#{key}")
        System.halt(1)

      {:error, reason} ->
        Output.error("Download failed: #{Output.format_error(reason)}")
        System.halt(1)
    end
  end

  defp download_recursive(client, s3_path, local_dir, ctx) do
    {bucket, src_prefix} = parse_s3_path(s3_path)

    case list_all_objects(client, bucket, src_prefix) do
      {:ok, []} ->
        Output.info("No objects found with prefix: s3://#{bucket}/#{src_prefix}")

      {:ok, objects} ->
        Enum.each(objects, fn obj ->
          rel_key = relative_key(obj.key, src_prefix)

          if rel_key != "" do
            local_path = Path.join(local_dir, rel_key)
            local_path |> Path.dirname() |> File.mkdir_p!()

            case S3Client.get_object(client, bucket, obj.key) do
              {:ok, result} ->
                File.write!(local_path, result.body)
                file_size = byte_size(result.body)

                if ctx.json do
                  IO.puts(
                    JSON.encode!(%{
                      status: "ok",
                      operation: "download",
                      source: "s3://#{bucket}/#{obj.key}",
                      destination: local_path,
                      size: file_size
                    })
                  )
                else
                  Output.success(
                    "Downloaded s3://#{bucket}/#{obj.key} → #{local_path} (#{Output.format_bytes(file_size)})"
                  )
                end

              {:error, reason} ->
                Output.error("Download failed for #{obj.key}: #{Output.format_error(reason)}")
                System.halt(1)
            end
          end
        end)

      {:error, reason} ->
        Output.error("Failed to list S3 directory: #{Output.format_error(reason)}")
        System.halt(1)
    end
  end

  defp s3_copy(client, src_path, dst_path, ctx) do
    {src_bucket, src_key} = parse_s3_path(src_path)
    {dst_bucket, dst_key} = parse_s3_path(dst_path)

    # If dst key ends with /, append source basename
    dst_key =
      if String.ends_with?(dst_key, "/") || dst_key == "" do
        dst_key <> Path.basename(src_key)
      else
        dst_key
      end

    case S3Client.copy_object(client, src_bucket, src_key, dst_bucket, dst_key) do
      :ok ->
        if ctx.json do
          IO.puts(
            JSON.encode!(%{
              status: "ok",
              operation: "copy",
              source: src_path,
              destination: "s3://#{dst_bucket}/#{dst_key}"
            })
          )
        else
          Output.success("Copied s3://#{src_bucket}/#{src_key} → s3://#{dst_bucket}/#{dst_key}")
        end

      {:error, reason} ->
        Output.error("Copy failed: #{Output.format_error(reason)}")
        System.halt(1)
    end
  end

  defp s3_copy_recursive(client, src_path, dst_path, ctx) do
    {src_bucket, src_prefix} = parse_s3_path(src_path)
    {dst_bucket, dst_prefix} = parse_s3_path(dst_path)

    case list_all_objects(client, src_bucket, src_prefix) do
      {:ok, []} ->
        Output.info("No objects found to copy: s3://#{src_bucket}/#{src_prefix}")

      {:ok, objects} ->
        Enum.each(objects, fn obj ->
          rel_key = relative_key(obj.key, src_prefix)

          if rel_key != "" do
            dst_key =
              if dst_prefix == "" do
                rel_key
              else
                prefix = String.trim_trailing(dst_prefix, "/")
                prefix <> "/" <> rel_key
              end

            case S3Client.copy_object(client, src_bucket, obj.key, dst_bucket, dst_key) do
              :ok ->
                if ctx.json do
                  IO.puts(
                    JSON.encode!(%{
                      status: "ok",
                      operation: "copy",
                      source: "s3://#{src_bucket}/#{obj.key}",
                      destination: "s3://#{dst_bucket}/#{dst_key}"
                    })
                  )
                else
                  Output.success(
                    "Copied s3://#{src_bucket}/#{obj.key} → s3://#{dst_bucket}/#{dst_key}"
                  )
                end

              {:error, reason} ->
                Output.error("Copy failed for #{obj.key}: #{Output.format_error(reason)}")
                System.halt(1)
            end
          end
        end)

      {:error, reason} ->
        Output.error("Failed to list S3 directory: #{Output.format_error(reason)}")
        System.halt(1)
    end
  end

  # Helpers

  defp list_local_files_recursive(path) do
    cond do
      File.dir?(path) ->
        case File.ls(path) do
          {:ok, children} ->
            Enum.flat_map(children, fn child ->
              list_local_files_recursive(Path.join(path, child))
            end)

          {:error, _} ->
            []
        end

      File.regular?(path) ->
        [path]

      true ->
        []
    end
  end

  def relative_local_path(file_path, base_path) do
    if file_path == base_path do
      Path.basename(file_path)
    else
      base_dir = if String.ends_with?(base_path, "/"), do: base_path, else: base_path <> "/"
      String.trim_leading(file_path, base_dir)
    end
  end

  defp list_all_objects(client, bucket, prefix) do
    list_all_objects_paginated(client, bucket, prefix, nil, [])
  end

  defp list_all_objects_paginated(client, bucket, prefix, token, acc) do
    opts = [prefix: prefix, delimiter: nil]
    opts = if token, do: opts ++ [continuation_token: token], else: opts

    case S3Client.list_objects(client, bucket, opts) do
      {:ok, %{contents: contents, is_truncated: true, next_continuation_token: next_token}}
      when is_binary(next_token) and next_token != "" ->
        list_all_objects_paginated(client, bucket, prefix, next_token, acc ++ contents)

      {:ok, %{contents: contents}} ->
        {:ok, acc ++ contents}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def relative_key(key, src_prefix) do
    cond do
      src_prefix == "" ->
        key

      String.ends_with?(src_prefix, "/") ->
        if String.starts_with?(key, src_prefix) do
          String.trim_leading(key, src_prefix)
        else
          key
        end

      true ->
        if key == src_prefix do
          Path.basename(key)
        else
          prefix_with_slash = src_prefix <> "/"

          if String.starts_with?(key, prefix_with_slash) do
            String.trim_leading(key, prefix_with_slash)
          else
            dir_prefix = Path.dirname(src_prefix)

            if dir_prefix == "." || dir_prefix == "/" do
              key
            else
              String.trim_leading(key, dir_prefix <> "/")
            end
          end
        end
    end
  end

  defp parse_s3_path(path) do
    path = String.trim_leading(path, "s3://")

    case String.split(path, "/", parts: 2) do
      [bucket] -> {bucket, ""}
      [bucket, key] -> {bucket, key}
    end
  end
end
