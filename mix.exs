defmodule ExStorageService.Umbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.1.0",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  defp deps do
    []
  end

  defp aliases do
    [
      setup: ["deps.get", "cmd --app ex_storage_service_web mix setup"],
      "assets.deploy": ["cmd --app ex_storage_service_web mix assets.deploy"]
    ]
  end
end
