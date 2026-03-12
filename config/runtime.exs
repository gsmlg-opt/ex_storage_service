import Config

data_root = System.get_env("ESS_DATA_ROOT", "/tmp/ex_storage_service/data")

# Configure Ra and Concord data directories
config :ra, data_dir: ~c"#{Path.join(data_root, "ra")}"
config :concord, data_dir: Path.join(data_root, "concord")

config :ex_storage_service,
  data_root: data_root,
  s3_port: String.to_integer(System.get_env("ESS_S3_PORT", "9000")),
  admin_port: String.to_integer(System.get_env("ESS_ADMIN_PORT", "4000")),
  root_admin_user: System.get_env("ESS_ADMIN_USER", "admin"),
  root_admin_password_hash:
    System.get_env(
      "ESS_ADMIN_PASSWORD_HASH",
      Base.encode16(:crypto.hash(:sha256, "admin"), case: :lower)
    ),
  master_key:
    System.get_env("ESS_MASTER_KEY") ||
      if(config_env() != :prod,
        do: Base.encode64(:crypto.strong_rand_bytes(32)),
        else: nil
      ),
  multipart_gc_interval: :timer.hours(1),
  multipart_max_age: :timer.hours(24),
  sync_interval: :timer.seconds(30),
  max_object_size: 5 * 1024 * 1024 * 1024,
  max_part_size: 5 * 1024 * 1024 * 1024,
  min_part_size: 5 * 1024 * 1024

if config_env() == :prod do
  master_key =
    System.get_env("ESS_MASTER_KEY") ||
      raise """
      environment variable ESS_MASTER_KEY is missing.
      You can generate one by calling: mix phx.gen.secret 32
      """

  config :ex_storage_service, master_key: master_key

  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  host = System.get_env("PHX_HOST", "localhost")
  port = String.to_integer(System.get_env("ESS_ADMIN_PORT", "4000"))

  config :ex_storage_service, ExStorageServiceWeb.Endpoint,
    url: [host: host, port: 443, scheme: "https"],
    http: [
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: port
    ],
    secret_key_base: secret_key_base,
    server: true
end
