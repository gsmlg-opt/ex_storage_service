defmodule ExStorageServiceCli.Commands.Ls do
  @moduledoc """
  List buckets or objects command.
  """

  alias ExStorageServiceCli.S3Client
  alias ExStorageServiceCli.Output

  def run(_command, args, ctx) do
    {opts, rest, _} =
      OptionParser.parse(args,
        strict: [
          prefix: :string,
          delimiter: :string,
          max_keys: :integer,
          recursive: :boolean,
          help: :boolean
        ],
        aliases: [r: :recursive, h: :help]
      )

    if opts[:help] do
      help("ls")
    else
      client = S3Client.new(ctx)

      case rest do
        [] ->
          list_buckets(client, ctx)

        [target] ->
          {bucket, prefix} = parse_target(target)
          prefix = opts[:prefix] || prefix
          delimiter = if opts[:recursive], do: nil, else: opts[:delimiter] || "/"
          max_keys = opts[:max_keys] || 1000

          list_objects(client, bucket, prefix, delimiter, max_keys, ctx)

        _ ->
          Output.error("Too many arguments")
          help("ls")
          System.halt(1)
      end
    end
  end

  def help(_command) do
    IO.puts("""
    #{IO.ANSI.bright()}ess ls#{IO.ANSI.reset()} — List buckets or objects

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess ls                       List all buckets
        ess ls <bucket>              List objects in bucket (folder view)
        ess ls <bucket>/<prefix>     List objects with prefix
        ess ls s3://<bucket>/<prefix>

    #{IO.ANSI.bright()}OPTIONS#{IO.ANSI.reset()}
        --prefix <prefix>     Filter by prefix
        --delimiter <delim>   Delimiter (default: "/")
        -r, --recursive       List all objects recursively (no delimiter)
        --max-keys <n>        Maximum keys to return (default: 1000)
        --json                Output in JSON format

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess ls
        ess ls my-bucket
        ess ls my-bucket/images/
        ess ls s3://my-bucket --recursive
        ess ls my-bucket --json
    """)
  end

  defp list_buckets(client, ctx) do
    case S3Client.list_buckets(client) do
      {:ok, buckets} ->
        Output.render(buckets, ctx, fn data ->
          if data == [] do
            Output.info("No buckets found")
          else
            rows =
              Enum.map(data, fn b ->
                [b.name, Output.format_datetime(b.creation_date)]
              end)

            Output.table(["BUCKET", "CREATED"], rows)
            Output.info("\n#{length(data)} bucket(s)")
          end
        end)

      {:error, reason} ->
        Output.error("Failed to list buckets: #{Output.format_error(reason)}")
        System.halt(1)
    end
  end

  defp list_objects(client, bucket, prefix, delimiter, max_keys, ctx) do
    case S3Client.list_objects(client, bucket,
           prefix: prefix,
           delimiter: delimiter,
           max_keys: max_keys
         ) do
      {:ok, result} ->
        Output.render(result, ctx, fn data ->
          # Sort files and common prefixes (directories) alphabetically
          sorted_contents = Enum.sort_by(data.contents, fn obj -> obj.key end)
          sorted_prefixes = Enum.sort(data.common_prefixes)

          # Show objects (files) first
          Enum.each(sorted_contents, fn obj ->
            datetime_str = Output.format_datetime_local(obj.last_modified)
            size_str = Output.format_bytes_short(obj.size) |> String.pad_leading(6)
            storage_class = if obj.storage_class == "", do: "STANDARD", else: obj.storage_class
            name = strip_prefix(obj.key, prefix)

            IO.puts("#{datetime_str} #{size_str} #{storage_class} #{name}")
          end)

          # Show common prefixes (directories) next
          Enum.each(sorted_prefixes, fn cp ->
            datetime_str = Output.current_datetime_local()
            size_str = "0B" |> String.pad_leading(6)
            name = strip_prefix(cp, prefix)

            IO.puts("#{datetime_str} #{size_str} #{name}")
          end)
        end)

      {:error, reason} ->
        Output.error("Failed to list objects in '#{bucket}': #{Output.format_error(reason)}")
        System.halt(1)
    end
  end

  defp parse_target(target) do
    target = String.trim_leading(target, "s3://")

    case String.split(target, "/", parts: 2) do
      [bucket] -> {bucket, ""}
      [bucket, prefix] -> {bucket, prefix}
    end
  end

  defp strip_prefix(path, ""), do: path
  defp strip_prefix(path, nil), do: path

  defp strip_prefix(path, prefix) do
    case String.trim_leading(path, prefix) do
      "" -> path
      relative -> relative
    end
  end
end
