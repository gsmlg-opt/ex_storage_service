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
        ess cp <source> <destination>

    #{IO.ANSI.bright()}COPY DIRECTIONS#{IO.ANSI.reset()}
        Local → S3:   ess cp ./file.txt s3://bucket/key
        S3 → Local:   ess cp s3://bucket/key ./file.txt
        S3 → S3:      ess cp s3://bucket1/key1 s3://bucket2/key2

    #{IO.ANSI.bright()}OPTIONS#{IO.ANSI.reset()}
        --content-type <type>   Set content type (upload only)
        -h, --help              Show help

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess cp ./photo.jpg s3://my-bucket/images/photo.jpg
        ess cp s3://my-bucket/data.csv ./data.csv
        ess cp s3://bucket1/key s3://bucket2/key
    """)
  end

  defp execute_copy(client, src, dst, opts, ctx) do
    src_s3? = String.starts_with?(src, "s3://")
    dst_s3? = String.starts_with?(dst, "s3://")

    cond do
      !src_s3? && dst_s3? ->
        upload(client, src, dst, opts, ctx)

      src_s3? && !dst_s3? ->
        download(client, src, dst, ctx)

      src_s3? && dst_s3? ->
        s3_copy(client, src, dst, ctx)

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
            Jason.encode!(%{
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
        Output.error("Upload failed: #{reason}")
        System.halt(1)
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
            Jason.encode!(%{
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
        Output.error("Download failed: #{reason}")
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
            Jason.encode!(%{
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
        Output.error("Copy failed: #{reason}")
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
