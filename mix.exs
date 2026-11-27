defmodule ExStorageService.Umbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.8.1",
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
          ex_storage_service_cluster: :permanent,
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
      test: [&isolate_test_data/1, "test"]
    ]
  end

  # Mix starts the apps before test_helper.exs. Own the root before startup,
  # and let child processes that select a different data root derive their own paths.
  defp isolate_test_data(_args) do
    if Mix.env() == :test do
      id = Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
      root = Path.join(System.tmp_dir!(), "ess-mix-test-" <> id)
      File.mkdir!(root)
      System.at_exit(fn _ -> File.rm_rf!(root) end)
      File.chmod!(root, 0o700)
      System.put_env("ESS_DATA_ROOT", root)

      Enum.each(
        ~w(ESS_BLOB_ROOT ESS_TMP_ROOT ESS_RA_ROOT ESS_METADATA_ROOT),
        &System.delete_env/1
      )

      Mix.Tasks.Loadconfig.load_runtime(Path.join(__DIR__, "config/runtime.exs"))
    end
  end
end
