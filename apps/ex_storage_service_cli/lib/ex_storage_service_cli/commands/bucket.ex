defmodule ExStorageServiceCli.Commands.Bucket do
  @moduledoc """
  Bucket management commands: `mb` (make bucket) and `rb` (remove bucket).
  """

  alias ExStorageServiceCli.S3Client
  alias ExStorageServiceCli.Output

  def run("mb", args, ctx) do
    {opts, rest, _} =
      OptionParser.parse(args,
        strict: [help: :boolean],
        aliases: [h: :help]
      )

    if opts[:help] do
      help("mb")
    else
      case rest do
        [bucket_arg] ->
          bucket = parse_bucket_name(bucket_arg)
          client = S3Client.new(ctx)

          case S3Client.create_bucket(client, bucket) do
            :ok ->
              if ctx.json do
                IO.puts(JSON.encode!(%{status: "ok", bucket: bucket}))
              else
                Output.success("Bucket '#{bucket}' created")
              end

            {:error, reason} ->
              Output.error("Failed to create bucket '#{bucket}': #{Output.format_error(reason)}")
              System.halt(1)
          end

        [] ->
          Output.error("Missing bucket name")
          help("mb")
          System.halt(1)

        _ ->
          Output.error("Too many arguments")
          help("mb")
          System.halt(1)
      end
    end
  end

  def run("rb", args, ctx) do
    {opts, rest, _} =
      OptionParser.parse(args,
        strict: [force: :boolean, help: :boolean],
        aliases: [f: :force, h: :help]
      )

    if opts[:help] do
      help("rb")
    else
      case rest do
        [bucket_arg] ->
          bucket = parse_bucket_name(bucket_arg)
          client = S3Client.new(ctx)

          case S3Client.delete_bucket(client, bucket) do
            :ok ->
              if ctx.json do
                IO.puts(JSON.encode!(%{status: "ok", bucket: bucket}))
              else
                Output.success("Bucket '#{bucket}' deleted")
              end

            {:error, reason} ->
              Output.error("Failed to delete bucket '#{bucket}': #{Output.format_error(reason)}")
              System.halt(1)
          end

        [] ->
          Output.error("Missing bucket name")
          help("rb")
          System.halt(1)

        _ ->
          Output.error("Too many arguments")
          help("rb")
          System.halt(1)
      end
    end
  end

  def help("mb") do
    IO.puts("""
    #{IO.ANSI.bright()}ess mb#{IO.ANSI.reset()} — Make (create) a bucket

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess mb <bucket-name>
        ess mb s3://<bucket-name>

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess mb my-bucket
        ess mb s3://my-bucket
    """)
  end

  def help("rb") do
    IO.puts("""
    #{IO.ANSI.bright()}ess rb#{IO.ANSI.reset()} — Remove (delete) a bucket

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess rb <bucket-name>
        ess rb s3://<bucket-name>

    #{IO.ANSI.bright()}DESCRIPTION#{IO.ANSI.reset()}
        The bucket must be empty before it can be deleted.

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess rb my-bucket
        ess rb s3://my-bucket
    """)
  end

  defp parse_bucket_name(name) do
    name
    |> String.trim_leading("s3://")
    |> String.trim_trailing("/")
  end
end
