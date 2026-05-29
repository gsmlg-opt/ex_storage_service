defmodule ExStorageServiceCli.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/gsmlg-dev/ex_storage_service"

  def project do
    [
      app: :ex_storage_service_cli,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: false,
      deps: deps(),
      escript: escript(),
      package: package(),
      description: "CLI tool for ExStorageService S3-compatible object storage",
      source_url: @source_url
    ] ++ umbrella_paths()
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
      {:toml, "~> 0.7"},
      {:ex_doc, "~> 0.35", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE)
    ]
  end

  # Only set umbrella paths when running inside the umbrella project.
  # When installed standalone from hex.pm, these paths don't exist.
  defp umbrella_paths do
    if File.exists?(Path.expand("../../mix.exs", __DIR__)) do
      [
        build_path: "../../_build",
        config_path: "../../config/config.exs",
        deps_path: "../../deps",
        lockfile: "../../mix.lock"
      ]
    else
      []
    end
  end
end

