defmodule ExStorageServiceCli.MixProject do
  use Mix.Project

  @version "0.5.0"
  @source_url "https://github.com/gsmlg-dev/ex_storage_service"

  def project do
    [
      app: :ex_storage_service_cli,
      version: @version,
      build_path: "../../_build",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: ">= 1.18.0",
      start_permanent: false,
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
      name: "ess"
    ]
  end

  defp deps do
    [
      {:req, "~> 0.5"},
      {:jason, "~> 1.4"},
      {:gsmlg_toml, "~> 1.0"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE)
    ]
  end
end
