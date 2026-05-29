defmodule ExStorageServiceCli.Commands.Mv do
  @moduledoc """
  Move an object within S3 (copy + delete source).
  """

  alias ExStorageServiceCli.S3Client
  alias ExStorageServiceCli.Output

  def run(_command, args, ctx) do
    {opts, rest, _} =
      OptionParser.parse(args,
        strict: [help: :boolean],
        aliases: [h: :help]
      )

    if opts[:help] do
      help("mv")
    else
      case rest do
        [src, dst] ->
          client = S3Client.new(ctx)
          execute_move(client, src, dst, ctx)

        _ ->
          Output.error("Expected exactly 2 arguments: <source> <destination>")
          help("mv")
          System.halt(1)
      end
    end
  end

  def help(_command) do
    IO.puts("""
    #{IO.ANSI.bright()}ess mv#{IO.ANSI.reset()} — Move an object within S3

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess mv s3://<bucket>/<key> s3://<bucket>/<key>

    #{IO.ANSI.bright()}DESCRIPTION#{IO.ANSI.reset()}
        Moves an object by copying to the destination and deleting the source.
        Both source and destination must be S3 paths.

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess mv s3://bucket/old-name.txt s3://bucket/new-name.txt
        ess mv s3://bucket1/file.txt s3://bucket2/file.txt
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
                Jason.encode!(%{
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

  defp parse_s3_path(path) do
    path = String.trim_leading(path, "s3://")

    case String.split(path, "/", parts: 2) do
      [bucket] -> {bucket, ""}
      [bucket, key] -> {bucket, key}
    end
  end
end
