import Config

config :ex_storage_service, ExStorageServiceWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: ExStorageServiceWeb.ErrorHTML, json: ExStorageServiceWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: ExStorageService.PubSub,
  live_view: [signing_salt: "ess_live_view"]

config :ex_storage_service, :json_library, Jason

config :esbuild,
  version: "0.21.5",
  ex_storage_service: [
    args: ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets --external:/fonts/* --external:/images/*),
    cd: Path.expand("../assets", __DIR__),
    env: %{"NODE_PATH" => Path.expand("../deps", __DIR__)}
  ]

config :tailwind,
  version: "3.4.17",
  ex_storage_service: [
    args: ~w(
      --config=tailwind.config.js
      --input=css/app.css
      --output=../priv/static/assets/app.css
    ),
    cd: Path.expand("../assets", __DIR__)
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
