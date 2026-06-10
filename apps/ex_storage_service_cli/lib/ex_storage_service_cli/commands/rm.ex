defmodule ExStorageServiceCli.Commands.Rm do
  @moduledoc """
  Remove an object from S3.
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
      help("rm")
    else
      case rest do
        [target] ->
          {bucket, key} = parse_s3_path(target)

          if key == "" do
            Output.error("Missing object key. Use 'ess rb' to remove a bucket.")
            System.halt(1)
          end

          client = S3Client.new(ctx)

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
    #{IO.ANSI.bright()}ess rm#{IO.ANSI.reset()} — Remove an object from S3

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess rm s3://<bucket>/<key>

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess rm s3://my-bucket/file.txt
        ess rm s3://my-bucket/images/photo.jpg
    """)
  end

  defp parse_s3_path(path) do
    path = String.trim_leading(path, "s3://")

    case String.split(path, "/", parts: 2) do
      [bucket] -> {bucket, ""}
      [bucket, key] -> {bucket, key}
    end
  end
end
