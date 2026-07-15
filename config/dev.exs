import Config

config :ex_storage_service_web, ExStorageServiceWeb.Endpoint,
  http: [ip: {0, 0, 0, 0}, port: 4900],
  check_origin: false,
  code_reloader: true,
  debug_errors: true,
  secret_key_base:
    "dev_secret_key_base_that_is_at_least_64_bytes_long_for_development_only_000000000",
  watchers: [
    duskmoon_bundler:
      {Mix.Tasks.DuskmoonBundler.Dev, :run,
       [
         ~w(--tailwind --tailwind-outdir) ++
           [Path.expand("../apps/ex_storage_service_web/priv/static/assets/css", __DIR__)]
       ]}
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

config :duskmoon_bundler, :server,
  prefix: "/assets",
  watch_dirs: [Path.expand("../apps/ex_storage_service_web/lib/", __DIR__)]

config :duskmoon_bundler,
  sourcemap: :linked

config :phoenix_live_view,
  debug_heex_annotations: true,
  enable_expensive_runtime_checks: true
