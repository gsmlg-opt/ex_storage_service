defmodule ExStorageServiceCli.Commands.Rm do
  @moduledoc """
  Remove an object from S3.
  """

  alias ExStorageServiceCli.S3Client
  alias ExStorageServiceCli.Output
  alias ExStorageServiceCli.Commands.Failure

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
            Failure.reported()
          else
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
                Failure.reported()
            end
          end

        [] ->
          Output.error("Missing target. Usage: ess rm s3://bucket/key")
          Failure.reported()

        _ ->
          Output.error("Too many arguments")
          help("rm")
          Failure.reported()
      end
    end
  end

  def help(_command) do
    IO.puts("""
    #{Output.color(IO.ANSI.bright())}ess rm#{Output.color(IO.ANSI.reset())} — Remove an object from S3

    #{Output.color(IO.ANSI.bright())}USAGE#{Output.color(IO.ANSI.reset())}
        ess rm s3://<bucket>/<key>

    #{Output.color(IO.ANSI.bright())}EXAMPLES#{Output.color(IO.ANSI.reset())}
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
