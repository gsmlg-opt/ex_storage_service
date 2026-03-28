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

config :bun,
  version: "1.3.4",
  ex_storage_service: [
    args:
      ~w(build js/app.js --outdir=../priv/static/assets --external=/fonts/* --external=/images/*),
    cd: Path.expand("../assets", __DIR__),
    env: %{"NODE_PATH" => Path.expand("../deps", __DIR__)}
  ]

config :tailwind,
  version: "4.1.11",
  ex_storage_service: [
    args: ~w(
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
