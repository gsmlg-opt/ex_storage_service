# Docker's dependency stage initially copies only umbrella project descriptors.
# Compilation still requires the task once the complete package is available.
if File.regular?(Path.join(__DIR__, "mix/compile.ess_signals.exs")) do
  Code.require_file("mix/compile.ess_signals.exs", __DIR__)
end

defmodule ExStorageServiceCli.MixProject do
  use Mix.Project

  @version "0.6.7"
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
      extra_applications: [:logger, :crypto, :xmerl]
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
      {:http_fetch, "~> 0.18.1"},
      {:jason, "~> 1.4"},
      {:gsmlg_toml, "~> 1.0"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files:
        ~w(lib mix.exs mix/compile.ess_signals.exs c_src README.md LICENSE priv/compatibility)
    ]
  end
end
