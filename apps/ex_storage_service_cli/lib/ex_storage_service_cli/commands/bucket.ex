defmodule ExStorageServiceCli.Commands.Bucket do
  @moduledoc """
  Bucket management commands: `mb` (make bucket) and `rb` (remove bucket).
  """

  alias ExStorageServiceCli.S3Client
  alias ExStorageServiceCli.Output
  alias ExStorageServiceCli.Commands.Failure

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
              Failure.reported()
          end

        [] ->
          Output.error("Missing bucket name")
          help("mb")
          Failure.reported()

        _ ->
          Output.error("Too many arguments")
          help("mb")
          Failure.reported()
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
              Failure.reported()
          end

        [] ->
          Output.error("Missing bucket name")
          help("rb")
          Failure.reported()

        _ ->
          Output.error("Too many arguments")
          help("rb")
          Failure.reported()
      end
    end
  end

  def help("mb") do
    IO.puts("""
    #{Output.color(IO.ANSI.bright())}ess mb#{Output.color(IO.ANSI.reset())} — Make (create) a bucket

    #{Output.color(IO.ANSI.bright())}USAGE#{Output.color(IO.ANSI.reset())}
        ess mb <bucket-name>
        ess mb s3://<bucket-name>

    #{Output.color(IO.ANSI.bright())}EXAMPLES#{Output.color(IO.ANSI.reset())}
        ess mb my-bucket
        ess mb s3://my-bucket
    """)
  end

  def help("rb") do
    IO.puts("""
    #{Output.color(IO.ANSI.bright())}ess rb#{Output.color(IO.ANSI.reset())} — Remove (delete) a bucket

    #{Output.color(IO.ANSI.bright())}USAGE#{Output.color(IO.ANSI.reset())}
        ess rb <bucket-name>
        ess rb s3://<bucket-name>

    #{Output.color(IO.ANSI.bright())}DESCRIPTION#{Output.color(IO.ANSI.reset())}
        The bucket must be empty before it can be deleted.

    #{Output.color(IO.ANSI.bright())}EXAMPLES#{Output.color(IO.ANSI.reset())}
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
