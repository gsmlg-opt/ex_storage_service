defmodule ExStorageServiceCli.Commands.Mv do
  @moduledoc """
  Move an object within S3 (copy + delete source).
  """

  alias ExStorageServiceCli.S3Client
  alias ExStorageServiceCli.Output

  def run(_command, args, ctx) do
    {opts, rest, _} =
      OptionParser.parse(args,
        strict: [recursive: :boolean, help: :boolean],
        aliases: [r: :recursive, h: :help]
      )

    if opts[:help] do
      help("mv")
    else
      case rest do
        [src, dst] ->
          client = S3Client.new(ctx)

          if opts[:recursive] do
            execute_move_recursive(client, src, dst, ctx)
          else
            execute_move(client, src, dst, ctx)
          end

        _ ->
          Output.error("Expected exactly 2 arguments: <source> <destination>")
          help("mv")
          System.halt(1)
      end
    end
  end

  def help(_command) do
    IO.puts("""
    #{IO.ANSI.bright()}ess mv#{IO.ANSI.reset()} — Move an object or objects within S3

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess mv s3://<bucket>/<key> s3://<bucket>/<key> [options]

    #{IO.ANSI.bright()}DESCRIPTION#{IO.ANSI.reset()}
        Moves objects by copying to the destination and deleting the source.
        Both source and destination must be S3 paths.

    #{IO.ANSI.bright()}OPTIONS#{IO.ANSI.reset()}
        -r, --recursive    Move directories recursively

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess mv s3://bucket/old-name.txt s3://bucket/new-name.txt
        ess mv -r s3://bucket/old-folder/ s3://bucket/new-folder/
    """)
  end

  defp execute_move(client, src, dst, ctx) do
    unless String.starts_with?(src, "s3://") && String.starts_with?(dst, "s3://") do
      Output.error("Both source and destination must be S3 paths (s3://bucket/key)")
      System.halt(1)
    end

    {src_bucket, src_key} = parse_s3_path(src)
    {dst_bucket, dst_key} = parse_s3_path(dst)

    if src_key == "" do
      Output.error("Source must include an object key")
      System.halt(1)
    end

    dst_key =
      if String.ends_with?(dst_key, "/") || dst_key == "" do
        dst_key <> Path.basename(src_key)
      else
        dst_key
      end

    # Step 1: Copy
    case S3Client.copy_object(client, src_bucket, src_key, dst_bucket, dst_key) do
      :ok ->
        # Step 2: Delete source
        case S3Client.delete_object(client, src_bucket, src_key) do
          :ok ->
            if ctx.json do
              IO.puts(
                JSON.encode!(%{
                  status: "ok",
                  operation: "move",
                  source: src,
                  destination: "s3://#{dst_bucket}/#{dst_key}"
                })
              )
            else
              Output.success(
                "Moved s3://#{src_bucket}/#{src_key} → s3://#{dst_bucket}/#{dst_key}"
              )
            end

          {:error, reason} ->
            Output.warn(
              "Object copied but source deletion failed: #{Output.format_error(reason)}"
            )

            Output.warn("Source: s3://#{src_bucket}/#{src_key}")
            Output.warn("Destination: s3://#{dst_bucket}/#{dst_key}")
            System.halt(1)
        end

      {:error, reason} ->
        Output.error("Move failed (copy step): #{Output.format_error(reason)}")
        System.halt(1)
    end
  end

  defp execute_move_recursive(client, src, dst, ctx) do
    unless String.starts_with?(src, "s3://") && String.starts_with?(dst, "s3://") do
      Output.error("Both source and destination must be S3 paths (s3://bucket/key)")
      System.halt(1)
    end

    {src_bucket, src_prefix} = parse_s3_path(src)
    {dst_bucket, dst_prefix} = parse_s3_path(dst)

    case list_all_objects(client, src_bucket, src_prefix) do
      {:ok, []} ->
        unless ctx.json,
          do: Output.info("No objects found to move: s3://#{src_bucket}/#{src_prefix}")

        :ok

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
                case S3Client.delete_object(client, src_bucket, obj.key) do
                  :ok ->
                    if ctx.json do
                      IO.puts(
                        JSON.encode!(%{
                          status: "ok",
                          operation: "move",
                          source: "s3://#{src_bucket}/#{obj.key}",
                          destination: "s3://#{dst_bucket}/#{dst_key}"
                        })
                      )
                    else
                      Output.success(
                        "Moved s3://#{src_bucket}/#{obj.key} → s3://#{dst_bucket}/#{dst_key}"
                      )
                    end

                  {:error, reason} ->
                    Output.warn(
                      "Object copied but source deletion failed for #{obj.key}: #{Output.format_error(reason)}"
                    )
                end

              {:error, reason} ->
                Output.error(
                  "Move failed for #{obj.key} (copy step): #{Output.format_error(reason)}"
                )

                System.halt(1)
            end
          end
        end)

      {:error, reason} ->
        Output.error("Failed to list S3 directory: #{Output.format_error(reason)}")
        System.halt(1)
    end
  end

  defp parse_s3_path(path) do
    path = String.trim_leading(path, "s3://")

    case String.split(path, "/", parts: 2) do
      [bucket] -> {bucket, ""}
      [bucket, key] -> {bucket, key}
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
end
