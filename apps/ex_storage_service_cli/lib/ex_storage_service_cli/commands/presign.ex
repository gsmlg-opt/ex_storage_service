defmodule ExStorageServiceCli.Commands.Presign do
  @moduledoc """
  Generate presigned URLs for S3 objects.
  """

  alias ExStorageServiceCli.SigV4
  alias ExStorageServiceCli.Output

  def run(_command, args, ctx) do
    {opts, rest, _} =
      OptionParser.parse(args,
        strict: [
          expires: :integer,
          method: :string,
          help: :boolean
        ],
        aliases: [e: :expires, h: :help]
      )

    if opts[:help] do
      help("presign")
    else
      case rest do
        [target] ->
          generate_presigned_url(target, opts, ctx)

        [] ->
          Output.error("Missing target. Usage: ess presign s3://bucket/key")
          System.halt(1)

        _ ->
          Output.error("Too many arguments")
          help("presign")
          System.halt(1)
      end
    end
  end

  def help(_command) do
    IO.puts("""
    #{IO.ANSI.bright()}ess presign#{IO.ANSI.reset()} — Generate a presigned URL

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess presign s3://<bucket>/<key> [options]

    #{IO.ANSI.bright()}OPTIONS#{IO.ANSI.reset()}
        --expires <seconds>   Expiration time (default: 3600, max: 604800)
        --method <method>     HTTP method: GET or PUT (default: GET)
        -h, --help            Show help

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess presign s3://my-bucket/file.txt
        ess presign s3://my-bucket/file.txt --expires 86400
        ess presign s3://my-bucket/upload.txt --method PUT
    """)
  end

  defp generate_presigned_url(target, opts, ctx) do
    unless ctx.access_key_id && ctx.secret_access_key do
      Output.error("Access key and secret key are required for presigned URLs")
      Output.error("Run 'ess configure' or pass --access-key and --secret-key")
      System.halt(1)
    end

    {bucket, key} = parse_s3_path(target)

    if key == "" do
      Output.error("Object key is required. Usage: ess presign s3://bucket/key")
      System.halt(1)
    end

    method = String.upcase(opts[:method] || "GET")
    expires = opts[:expires] || 3600

    unless method in ["GET", "PUT", "HEAD", "DELETE"] do
      Output.error("Invalid method '#{method}'. Use GET, PUT, HEAD, or DELETE.")
      System.halt(1)
    end

    url = "#{ctx.endpoint}/#{bucket}/#{key}"

    presigned_url =
      SigV4.presign_url(method, url,
        access_key_id: ctx.access_key_id,
        secret_access_key: ctx.secret_access_key,
        region: ctx.region,
        expires: expires
      )

    if ctx.json do
      IO.puts(
        JSON.encode!(%{
          url: presigned_url,
          method: method,
          expires: expires,
          bucket: bucket,
          key: key
        })
      )
    else
      IO.puts(presigned_url)
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
