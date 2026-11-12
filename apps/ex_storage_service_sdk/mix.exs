defmodule ExStorageServiceSdk.MixProject do
  use Mix.Project

  @version "0.7.0"
  @source_url "https://github.com/gsmlg-opt/ex_storage_service"

  def project do
    [
      app: :ex_storage_service_sdk,
      version: @version,
      elixir: ">= 1.18.0",
      start_permanent: false,
      deps: deps(),
      package: package(),
      docs: docs(),
      description: "S3 client SDK for ExStorageService and other S3-compatible object storage",
      source_url: @source_url
    ] ++ umbrella_paths()
  end

  # Shares the umbrella build inside apps/; a standalone checkout (Hex consumers,
  # package verification) uses its own paths.
  defp umbrella_paths do
    if Path.basename(Path.dirname(__DIR__)) == "apps",
      do: [build_path: "../../_build", deps_path: "../../deps", lockfile: "../../mix.lock"],
      else: []
  end

  def application do
    [extra_applications: [:logger, :crypto]]
  end

  defp deps do
    [
      {:http_fetch, "~> 1.0.0-rc.10"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE priv/compatibility)
    ]
  end

  defp docs do
    [main: "readme", extras: ["README.md"], source_ref: "v#{@version}"]
  end
end
