defmodule ExStorageService.Umbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.1.10",
      start_permanent: Mix.env() == :prod,
      listeners: [Phoenix.CodeReloader],
      deps: deps(),
      aliases: aliases(),
      releases: releases()
    ]
  end

  defp deps do
    []
  end

  defp releases do
    [
      ess: [
        applications: [
          ex_storage_service: :permanent,
          ex_storage_service_s3: :permanent,
          ex_storage_service_web: :permanent
        ]
      ]
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "cmd --app ex_storage_service_web mix setup"],
      "assets.deploy": ["cmd --app ex_storage_service_web mix assets.deploy"]
    ]
  end
end
