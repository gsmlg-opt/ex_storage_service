# Docker's dependency stage initially copies only umbrella project descriptors.
# Compilation still requires the task once the complete package is available.
if File.regular?(Path.join(__DIR__, "mix/compile.ess_signals.exs")) do
  Code.require_file("mix/compile.ess_signals.exs", __DIR__)
end

defmodule ExStorageServiceCli.MixProject do
  use Mix.Project

  @version "0.7.1"
  @source_url "https://github.com/gsmlg-opt/ex_storage_service"

  def project do
    [
      app: :ex_storage_service_cli,
      version: @version,
      build_path: "../../_build",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: ">= 1.18.0",
      start_permanent: false,
      erlc_paths: ["lib/ex_storage_service_cli/query/src", "lib/ex_storage_service_cli/yaml/src"],
      erlc_options: [{:i, "lib/ex_storage_service_cli/yaml/include"}],
      compilers: [:ess_signals, :yecc, :leex] ++ Mix.compilers(),
      deps: deps(),
      escript: escript(),
      package: package(),
      description: "CLI tool for ExStorageService S3-compatible object storage",
      source_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto, :public_key, :ssl, :xmerl]
    ]
  end

  defp escript do
    [
      main_module: ExStorageServiceCli,
      name: "ess",
      include_priv_for: [:ex_storage_service_cli]
    ]
  end

  defp deps do
    [
      sdk_dep(),
      {:http_fetch, "~> 1.0.0-rc.10"},
      {:jason, "~> 1.4"},
      {:gsmlg_toml, "~> 1.0"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  # Inside the umbrella the CLI builds against the sibling SDK. Hex packages can
  # only depend on Hex packages, so `mix hex.build`/`hex.publish` run with
  # EX_STORAGE_SERVICE_SDK=hex. Package verification before the SDK is on Hex
  # points EX_STORAGE_SERVICE_SDK at an extracted SDK package directory.
  defp sdk_dep do
    case System.get_env("EX_STORAGE_SERVICE_SDK") do
      nil ->
        if in_umbrella?(),
          do: {:ex_storage_service_sdk, in_umbrella: true},
          else: {:ex_storage_service_sdk, "~> " <> @version}

      "hex" ->
        {:ex_storage_service_sdk, "~> " <> @version}

      path ->
        {:ex_storage_service_sdk, path: path}
    end
  end

  defp in_umbrella?, do: Path.basename(Path.dirname(__DIR__)) == "apps"

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files:
        ~w(lib mix.exs mix/compile.ess_signals.exs c_src README.md LICENSE priv/compatibility)
    ]
  end
end
