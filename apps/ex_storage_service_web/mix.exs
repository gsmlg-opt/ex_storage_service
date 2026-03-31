defmodule ExStorageServiceWeb.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_storage_service_web,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      mod: {ExStorageServiceWeb.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:ex_storage_service, in_umbrella: true},
      {:ex_storage_service_s3, in_umbrella: true},
      {:bandit, "~> 1.6"},
      {:phoenix, "~> 1.8"},
      {:phoenix_live_view, "~> 1.0"},
      {:phoenix_html, "~> 4.2"},
      {:phoenix_live_dashboard, "~> 0.8"},
      {:jason, "~> 1.4"},
      {:phoenix_live_reload, "~> 1.2", only: :dev},
      {:bun, "~> 1.4", runtime: Mix.env() == :dev},
      {:tailwind, "~> 0.2", runtime: Mix.env() == :dev},
      {:phoenix_duskmoon,
       github: "duskmoon-dev/phoenix-duskmoon-ui",
       tag: "v9.0.0-rc.3",
       sparse: "apps/phoenix_duskmoon"}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "bun.install", "assets.setup", "assets.build"],
      "assets.setup": ["tailwind.install --if-missing"],
      "assets.build": ["tailwind ex_storage_service_web", "bun ex_storage_service_web"],
      "assets.deploy": [
        "tailwind ex_storage_service_web --minify",
        "bun ex_storage_service_web",
        "phx.digest"
      ]
    ]
  end
end
