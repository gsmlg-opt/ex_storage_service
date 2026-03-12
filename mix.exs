defmodule ExStorageService.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_storage_service,
      version: "0.1.0",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      mod: {ExStorageService.Application, []},
      extra_applications: [:logger, :runtime_tools, :crypto, :xmerl]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:bandit, "~> 1.6"},
      {:plug, "~> 1.16"},
      {:phoenix, "~> 1.8"},
      {:phoenix_live_view, "~> 1.0"},
      {:phoenix_html, "~> 4.2"},
      {:phoenix_live_dashboard, "~> 0.8"},
      {:concord, "~> 1.0"},
      {:req, "~> 0.5"},
      {:jason, "~> 1.4"},
      {:telemetry_metrics, "~> 0.6"},
      {:telemetry_poller, "~> 1.1"},
      {:dns_cluster, "~> 0.1"},
      {:phoenix_live_reload, "~> 1.2", only: :dev},
      {:esbuild, "~> 0.8", runtime: Mix.env() == :dev},
      {:tailwind, "~> 0.2", runtime: Mix.env() == :dev},
      {:heroicons, "~> 0.5",
       github: "tailwindlabs/heroicons",
       tag: "v2.2.0",
       sparse: "optimized",
       app: false,
       compile: false,
       depth: 1}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "assets.setup", "assets.build"],
      "assets.setup": ["tailwind.install --if-missing", "esbuild.install --if-missing"],
      "assets.build": ["tailwind ex_storage_service", "esbuild ex_storage_service"],
      "assets.deploy": [
        "tailwind ex_storage_service --minify",
        "esbuild ex_storage_service --minify",
        "phx.digest"
      ]
    ]
  end
end
