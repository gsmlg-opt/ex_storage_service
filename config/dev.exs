import Config

config :ex_storage_service_web, ExStorageServiceWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4900],
  check_origin: false,
  code_reloader: true,
  debug_errors: true,
  secret_key_base:
    "dev_secret_key_base_that_is_at_least_64_bytes_long_for_development_only_000000000",
  watchers: [
    bun: {Bun, :install_and_run, [:ex_storage_service_web, ~w(--watch)]},
    tailwind: {Tailwind, :install_and_run, [:ex_storage_service_web, ~w(--watch)]}
  ]

config :ex_storage_service_web, ExStorageServiceWeb.Endpoint,
  live_reload: [
    patterns: [
      ~r"apps/ex_storage_service_web/priv/static/(?!uploads/).*(js|css|png|jpeg|jpg|gif|svg)$",
      ~r"apps/ex_storage_service_web/lib/ex_storage_service_web/(controllers|live|components)/.*(ex|heex)$"
    ]
  ]

config :ex_storage_service_web, dev_routes: true

config :logger, :console, level: :debug

config :phoenix, :plug_init_mode, :runtime

# Enable sourcemaps in dev for easier debugging
config :bun,
  ex_storage_service_web: [
    args:
      ~w(build js/app.js --outdir=../priv/static/assets --external=/fonts/* --external=/images/* --sourcemap=linked),
    cd: Path.expand("../apps/ex_storage_service_web/assets", __DIR__),
    env: %{"NODE_PATH" => Path.expand("../deps", __DIR__)}
  ]

config :phoenix_live_view,
  debug_heex_annotations: true,
  enable_expensive_runtime_checks: true
