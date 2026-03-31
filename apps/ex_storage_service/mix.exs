defmodule ExStorageService.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_storage_service,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
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
      {:concord, "~> 1.0"},
      {:req, "~> 0.5"},
      {:jason, "~> 1.4"},
      {:telemetry_metrics, "~> 0.6"},
      {:telemetry_poller, "~> 1.1"},
      {:dns_cluster, "~> 0.1"},
      {:phoenix_pubsub, "~> 2.1"}
    ]
  end
end
