import Config

config :ex_storage_service_web, ExStorageServiceWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: ExStorageServiceWeb.ErrorHTML, json: ExStorageServiceWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: ExStorageService.PubSub,
  live_view: [signing_salt: "ess_live_view"]

config :ex_storage_service, :json_library, Jason

config :volt,
  entry: Path.expand("../apps/ex_storage_service_web/assets/js/app.js", __DIR__),
  root: Path.expand("../apps/ex_storage_service_web/assets", __DIR__),
  outdir: Path.expand("../apps/ex_storage_service_web/priv/static/assets", __DIR__),
  resolve_dirs: [Path.expand("../deps", __DIR__), Path.expand("../node_modules", __DIR__)],
  target: :es2020,
  tailwind: [
    css: Path.expand("../apps/ex_storage_service_web/assets/css/app.css", __DIR__),
    sources: [
      %{base: Path.expand("../apps/ex_storage_service_web/lib/", __DIR__), pattern: "**/*.{ex,heex}"},
      %{base: Path.expand("../apps/ex_storage_service_web/assets/", __DIR__), pattern: "**/*.{js,ts,jsx,tsx}"},
      %{base: Path.expand("../deps/phoenix_duskmoon/", __DIR__), pattern: "**/*.{ex,heex}"}
    ]
  ]

# Ra configuration (Raft consensus used by Concord)
config :ra,
  data_dir: ~c"/tmp/ex_storage_service/ra"

# Concord configuration
config :concord,
  data_dir: "/tmp/ex_storage_service/concord",
  http: [enabled: false],
  prometheus_enabled: false

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

config :phoenix, :json_library, Jason

import_config "#{config_env()}.exs"
