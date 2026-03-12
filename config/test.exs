import Config

config :ex_storage_service, ExStorageServiceWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "test_secret_key_base_that_is_at_least_64_bytes_long_for_testing_only_00000000000",
  server: true

config :ex_storage_service,
  s3_port: 9001,
  rate_limit: [enabled: false]

config :logger, level: :warning

config :phoenix, :plug_init_mode, :runtime
