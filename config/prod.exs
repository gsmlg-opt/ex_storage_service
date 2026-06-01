import Config

config :logger, level: :info

config :ex_storage_service_web, ExStorageServiceWeb.Endpoint,
  cache_static_manifest: "priv/static/cache_manifest.json"
