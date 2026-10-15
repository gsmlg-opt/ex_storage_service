defmodule ExStorageServiceCli.Commands.Configure do
  @moduledoc """
  Interactive profile configuration command.

  Sets up access credentials and endpoint in `~/.config/ess/config.toml`.
  """

  alias ExStorageServiceCli.Config
  alias ExStorageServiceCli.Output
  alias ExStorageServiceCli.Error

  def run(_command, args, ctx) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [profile: :string],
        aliases: []
      )

    profile_name = opts[:profile] || "default"
    config_opts = ExStorageServiceCli.CLI.NativeGlobals.config_options(ctx[:options] || %{})

    with {:ok, existing} <- existing_profile(profile_name, config_opts) do
      IO.puts(
        "#{Output.color(IO.ANSI.bright())}ESS CLI Configuration#{Output.color(IO.ANSI.reset())}"
      )

      IO.puts(
        "Profile: #{Output.color(IO.ANSI.cyan())}#{profile_name}#{Output.color(IO.ANSI.reset())}"
      )

      IO.puts("")

      with {:ok, endpoint} <-
             prompt("S3 Endpoint", existing[:endpoint] || "http://localhost:9000"),
           {:ok, access_key_id} <- prompt("Access Key ID", existing[:access_key_id] || ""),
           {:ok, secret_access_key} <-
             prompt_secret("Secret Access Key", existing[:secret_access_key]),
           {:ok, region} <- prompt("Region", existing[:region] || "us-east-1"),
           :ok <-
             Config.save_profile(
               profile_name,
               %{
                 endpoint: endpoint,
                 access_key_id: access_key_id,
                 secret_access_key: secret_access_key,
                 region: region
               },
               config_opts
             ) do
        Output.success(
          "Profile '#{profile_name}' saved to #{config_opts[:path] || Config.config_path()}"
        )
      end
    end
  end

  defp existing_profile(name, opts) do
    case Config.read_profile(name, opts) do
      {:error, %Error{code: :profile_missing}} -> {:ok, %{}}
      result -> result
    end
  end

  def help(_command) do
    IO.puts("""
    #{Output.color(IO.ANSI.bright())}ess configure#{Output.color(IO.ANSI.reset())} — Set up access credentials

    #{Output.color(IO.ANSI.bright())}USAGE#{Output.color(IO.ANSI.reset())}
        ess configure [--profile <name>]

    #{Output.color(IO.ANSI.bright())}OPTIONS#{Output.color(IO.ANSI.reset())}
        --profile <name>    Profile name (default: "default")

    #{Output.color(IO.ANSI.bright())}DESCRIPTION#{Output.color(IO.ANSI.reset())}
        Interactively configures access credentials, endpoint, and region.
        Settings are saved to ~/.config/ess/config.toml.

    #{Output.color(IO.ANSI.bright())}EXAMPLES#{Output.color(IO.ANSI.reset())}
        ess configure
        ess configure --profile production
    """)
  end

  defp prompt(label, default) do
    default_display = if default != "", do: " [#{default}]", else: ""

    with {:ok, input} <- read_input("#{label}#{default_display}: ") do
      {:ok, if(input == "", do: default, else: input)}
    end
  end

  defp prompt_secret(label, existing) do
    has_existing = existing != nil && existing != ""
    mask = if has_existing, do: " [****]", else: ""

    with {:ok, input} <- read_input("#{label}#{mask}: ") do
      {:ok, if(input == "" && has_existing, do: existing, else: input)}
    end
  end

  defp read_input(prompt) do
    case IO.gets(prompt) do
      input when is_binary(input) -> {:ok, String.trim(input)}
      _ -> {:error, Error.new(:config, "Configuration input ended before the profile was saved")}
    end
  end
end
