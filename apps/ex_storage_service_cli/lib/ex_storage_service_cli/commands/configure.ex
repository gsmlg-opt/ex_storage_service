defmodule ExStorageServiceCli.Commands.Configure do
  @moduledoc """
  Interactive profile configuration command.

  Sets up access credentials and endpoint in `~/.config/ess/config.toml`.
  """

  alias ExStorageServiceCli.Config
  alias ExStorageServiceCli.Output

  def run(_command, args, _ctx) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [profile: :string],
        aliases: []
      )

    profile_name = opts[:profile] || "default"

    IO.puts("#{IO.ANSI.bright()}ESS CLI Configuration#{IO.ANSI.reset()}")
    IO.puts("Profile: #{IO.ANSI.cyan()}#{profile_name}#{IO.ANSI.reset()}")
    IO.puts("")

    existing = Config.load_profile(profile_name)

    endpoint = prompt("S3 Endpoint", existing[:endpoint] || "http://localhost:9000")
    access_key_id = prompt("Access Key ID", existing[:access_key_id] || "")
    secret_access_key = prompt_secret("Secret Access Key", existing[:secret_access_key])
    region = prompt("Region", existing[:region] || "us-east-1")

    profile_data = %{
      endpoint: endpoint,
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region
    }

    case Config.save_profile(profile_name, profile_data) do
      :ok ->
        Output.success("Profile '#{profile_name}' saved to #{Config.config_path()}")

      {:error, reason} ->
        Output.error("Failed to save profile: #{inspect(reason)}")
        System.halt(1)
    end
  end

  def help(_command) do
    IO.puts("""
    #{IO.ANSI.bright()}ess configure#{IO.ANSI.reset()} — Set up access credentials

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess configure [--profile <name>]

    #{IO.ANSI.bright()}OPTIONS#{IO.ANSI.reset()}
        --profile <name>    Profile name (default: "default")

    #{IO.ANSI.bright()}DESCRIPTION#{IO.ANSI.reset()}
        Interactively configures access credentials, endpoint, and region.
        Settings are saved to ~/.config/ess/config.toml.

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess configure
        ess configure --profile production
    """)
  end

  defp prompt(label, default) do
    default_display = if default != "", do: " [#{default}]", else: ""
    input = IO.gets("#{label}#{default_display}: ") |> String.trim()

    if input == "" do
      default
    else
      input
    end
  end

  defp prompt_secret(label, existing) do
    has_existing = existing != nil && existing != ""
    mask = if has_existing, do: " [****]", else: ""
    input = IO.gets("#{label}#{mask}: ") |> String.trim()

    if input == "" && has_existing do
      existing
    else
      input
    end
  end
end
