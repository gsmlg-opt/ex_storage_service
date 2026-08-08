defmodule ExStorageServiceS3.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_storage_service_s3,
      version: "0.6.2",
      elixir: ">= 1.18.0",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ] ++ umbrella_paths()
  end

  def application do
    [
      mod: {ExStorageServiceS3.Application, []},
      extra_applications: [:logger, :xmerl]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp umbrella_paths do
    if in_umbrella?() do
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
      ex_storage_service_dep(),
      {:bandit, "~> 1.6"},
      {:plug, "~> 1.16"}
    ]
  end

  defp ex_storage_service_dep do
    if in_umbrella?() do
      {:ex_storage_service, in_umbrella: true}
    else
      {:ex_storage_service, "~> 0.6"}
    end
  end

  defp in_umbrella?, do: Path.basename(Path.dirname(__DIR__)) == "apps"
end
