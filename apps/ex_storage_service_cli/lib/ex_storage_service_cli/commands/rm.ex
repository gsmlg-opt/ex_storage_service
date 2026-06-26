defmodule ExStorageServiceCli.Commands.Rm do
  @moduledoc """
  Remove an object from S3.
  """

  alias ExStorageServiceCli.S3Client
  alias ExStorageServiceCli.Output

  def run(_command, args, ctx) do
    {opts, rest, _} =
      OptionParser.parse(args,
        strict: [recursive: :boolean, force: :boolean, help: :boolean],
        aliases: [r: :recursive, f: :force, h: :help]
      )

    if opts[:help] do
      help("rm")
    else
      case rest do
        [target] ->
          {bucket, key} = parse_s3_path(target)

          client = S3Client.new(ctx)

          if opts[:recursive] do
            if key == "" and not opts[:force] do
              Output.error(
                "Bailing out: to delete all objects in the bucket recursively, you must specify the --force flag."
              )

              System.halt(1)
            end

            case delete_recursive(client, bucket, key, opts[:force], ctx) do
              :ok ->
                if ctx.json do
                  IO.puts(JSON.encode!(%{status: "ok", bucket: bucket, prefix: key}))
                end

              {:error, reason} ->
                Output.error("Failed to delete recursively: #{Output.format_error(reason)}")
                System.halt(1)
            end
          else
            if key == "" do
              Output.error(
                "Missing object key. Use 'ess rb' to remove a bucket, or 'ess rm -r' to delete recursively."
              )

              System.halt(1)
            end

            case S3Client.delete_object(client, bucket, key) do
              :ok ->
                if ctx.json do
                  IO.puts(JSON.encode!(%{status: "ok", bucket: bucket, key: key}))
                else
                  Output.success("Deleted s3://#{bucket}/#{key}")
                end

              {:error, reason} ->
                Output.error("Failed to delete: #{Output.format_error(reason)}")
                System.halt(1)
            end
          end

        [] ->
          Output.error("Missing target. Usage: ess rm s3://bucket/key")
          System.halt(1)

        _ ->
          Output.error("Too many arguments")
          help("rm")
          System.halt(1)
      end
    end
  end

  def help(_command) do
    IO.puts("""
    #{IO.ANSI.bright()}ess rm#{IO.ANSI.reset()} — Remove an object or objects from S3

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess rm s3://<bucket>/<key> [options]

    #{IO.ANSI.bright()}OPTIONS#{IO.ANSI.reset()}
        -r, --recursive    Remove recursively
        -f, --force        Force removal (required to delete entire bucket contents recursively)

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess rm s3://my-bucket/file.txt
        ess rm -r s3://my-bucket/images/
        ess rm -rf s3://my-bucket/
    """)
  end

  def parse_s3_path(path) do
    path = String.trim_leading(path, "s3://")

    case String.split(path, "/", parts: 2) do
      [bucket] -> {bucket, ""}
      [bucket, key] -> {bucket, key}
    end
  end

  defp delete_recursive(client, bucket, prefix, _force?, ctx) do
    case list_all_objects(client, bucket, prefix) do
      {:ok, []} ->
        unless ctx.json, do: Output.info("No objects found with prefix: s3://#{bucket}/#{prefix}")
        :ok

      {:ok, objects} ->
        Enum.reduce_while(objects, :ok, fn obj, :ok ->
          case S3Client.delete_object(client, bucket, obj.key) do
            :ok ->
              unless ctx.json, do: Output.success("Deleted s3://#{bucket}/#{obj.key}")
              {:cont, :ok}

            {:error, reason} ->
              {:halt, {:error, reason}}
          end
        end)

      {:error, reason} ->
        {:error, reason}
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
end
