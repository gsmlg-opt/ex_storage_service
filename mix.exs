defmodule ExStorageService.Umbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.5.0",
      elixir: ">= 1.18.0",
      start_permanent: Mix.env() == :prod,
      listeners: [Phoenix.CodeReloader],
      deps: deps(),
      aliases: aliases(),
      releases: releases()
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.40", runtime: false, override: true}
    ]
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
      setup: ["deps.get", "do --app ex_storage_service_web cmd mix setup"],
      "assets.deploy": ["do --app ex_storage_service_web cmd mix assets.deploy"],
      test: [&clean_test_data/1, "test"]
    ]
  end

  # Wipe test state (storage + Ra + Concord live under the test data root)
  # BEFORE the apps boot. Cleaning inside a test_helper.exs is too late: mix
  # starts the apps first, so stale Raft state from a previous run of another
  # umbrella app breaks Concord recovery for the current one.
  defp clean_test_data(_args) do
    if Mix.env() == :test do
      data_root = System.get_env("ESS_DATA_ROOT", "/tmp/ex_storage_service/test_data")
      File.rm_rf!(data_root)
    end
  end
end
