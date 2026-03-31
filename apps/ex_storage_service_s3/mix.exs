defmodule ExStorageServiceS3.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_storage_service_s3,
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
      mod: {ExStorageServiceS3.Application, []},
      extra_applications: [:logger, :xmerl]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:ex_storage_service, in_umbrella: true},
      {:bandit, "~> 1.6"},
      {:plug, "~> 1.16"}
    ]
  end
end
