defmodule ExStorageService.MixProject do
  use Mix.Project

  @source_url "https://github.com/gsmlg-opt/ex_storage_service"

  def project do
    [
      app: :ex_storage_service,
      version: "0.6.4",
      elixir: ">= 1.18.0",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      description: "Embeddable content-addressed object storage for Elixir applications",
      source_url: @source_url
    ] ++ umbrella_paths()
  end

  def application do
    [
      mod: {ExStorageService.Application, []},
      extra_applications: [:logger, :runtime_tools, :crypto, :xmerl]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp umbrella_paths do
    if Path.basename(Path.dirname(__DIR__)) == "apps" do
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

  defp deps do
    [
      {:concord, "~> 3.0"},
      {:req, "~> 0.5"},
      {:jason, "~> 1.4"},
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_poller, "~> 1.1"},
      {:dns_cluster, "~> 0.1"},
      {:phoenix_pubsub, "~> 2.1"}
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
