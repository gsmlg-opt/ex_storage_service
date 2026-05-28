defmodule ExStorageServiceCli.Commands.Info do
  @moduledoc """
  Server health and information command.
  """

  alias ExStorageServiceCli.S3Client
  alias ExStorageServiceCli.Output

  def run(_command, args, ctx) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [help: :boolean],
        aliases: [h: :help]
      )

    if opts[:help] do
      help("info")
    else
      client = S3Client.new(ctx)

      case S3Client.health(client) do
        {:ok, health} ->
          Output.render(health, ctx, fn data ->
            IO.puts("#{IO.ANSI.bright()}Server Information#{IO.ANSI.reset()}")
            IO.puts("")
            IO.puts("  Endpoint:  #{IO.ANSI.cyan()}#{ctx.endpoint}#{IO.ANSI.reset()}")
            IO.puts("  Profile:   #{ctx.profile}")
            IO.puts("  Region:    #{ctx.region}")
            IO.puts("")

            status_color =
              if data["status"] == "ok", do: IO.ANSI.green(), else: IO.ANSI.red()

            IO.puts("  Status:    #{status_color}#{data["status"]}#{IO.ANSI.reset()}")

            # Print any additional health data
            data
            |> Map.delete("status")
            |> Enum.each(fn {k, v} ->
              IO.puts("  #{k}:  #{inspect(v)}")
            end)
          end)

        {:error, reason} ->
          Output.error("Server unreachable: #{inspect(reason)}")
          IO.puts("")
          IO.puts("  Endpoint: #{ctx.endpoint}")
          IO.puts("  Profile:  #{ctx.profile}")
          System.halt(1)
      end
    end
  end

  def help(_command) do
    IO.puts("""
    #{IO.ANSI.bright()}ess info#{IO.ANSI.reset()} — Show server health information

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess info

    #{IO.ANSI.bright()}DESCRIPTION#{IO.ANSI.reset()}
        Checks the S3 endpoint health and displays server information.

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess info
        ess info --endpoint http://s3.example.com:9000
        ess info --json
    """)
  end
end
