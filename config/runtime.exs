import Config

data_root =
  System.get_env(
    "ESS_DATA_ROOT",
    if(config_env() == :test,
      do: "/tmp/ex_storage_service/test_data",
      else: "/tmp/ex_storage_service/data"
    )
  )

s3_auth_enabled_value = System.get_env("ESS_S3_AUTH_ENABLED", "false") |> String.downcase()
s3_auth_enabled? = s3_auth_enabled_value in ["1", "true", "yes", "on"]

parse_boolean = fn variable, default ->
  case System.get_env(variable, default) |> String.downcase() do
    value when value in ["1", "true", "yes", "on"] -> true
    value when value in ["0", "false", "no", "off"] -> false
    value -> raise "#{variable} must be a boolean, got: #{inspect(value)}"
  end
end

parse_positive_integer = fn variable, default ->
  value = System.get_env(variable, default)

  case Integer.parse(value) do
    {integer, ""} when integer > 0 -> integer
    _ -> raise "#{variable} must be an integer greater than or equal to 1, got: #{inspect(value)}"
  end
end

mode =
  case System.get_env("ESS_MODE", "standalone") |> String.downcase() do
    "standalone" -> :standalone
    "cluster" -> :cluster
    value -> raise "ESS_MODE must be standalone or cluster, got: #{inspect(value)}"
  end

metadata_schema =
  case System.get_env("ESS_METADATA_SCHEMA", "v2") |> String.downcase() do
    "v1" -> :v1
    "v2" -> :v2
    value -> raise "ESS_METADATA_SCHEMA must be v1 or v2, got: #{inspect(value)}"
  end

instance_config = [
  mode: mode,
  replication_factor: parse_positive_integer.("ESS_REPLICATION_FACTOR", "1"),
  write_quorum: parse_positive_integer.("ESS_WRITE_QUORUM", "1"),
  allow_degraded_writes: parse_boolean.("ESS_ALLOW_DEGRADED_WRITES", "false"),
  cluster_data_plane_enabled: parse_boolean.("ESS_CLUSTER_DATA_PLANE_ENABLED", "false"),
  public_s3_enabled: parse_boolean.("ESS_PUBLIC_S3_ENABLED", "true"),
  metadata_schema: metadata_schema
]

# Compute the well-known default admin password hash once at the top so it can
# be referenced both in the config block and in the prod guard below.
default_admin_hash = Base.encode16(:crypto.hash(:sha256, "admin"), case: :lower)

# Configure Ra and Concord data directories
config :ra, data_dir: ~c"#{Path.join(data_root, "ra")}"
config :concord, data_dir: Path.join(data_root, "concord")

# Disable libcluster
config :libcluster, topologies: []

config :ex_storage_service,
  data_root: data_root,
  instance_config: instance_config,
  s3_port:
    String.to_integer(
      System.get_env("ESS_S3_PORT", if(config_env() == :test, do: "9001", else: "9000"))
    ),
  admin_port:
    String.to_integer(
      System.get_env("ESS_ADMIN_PORT", if(config_env() == :test, do: "4002", else: "4900"))
    ),
  s3_auth_enabled: s3_auth_enabled?,
  root_admin_user: System.get_env("ESS_ADMIN_USER", "admin"),
  root_admin_password_hash: System.get_env("ESS_ADMIN_PASSWORD_HASH", default_admin_hash),
  master_key:
    System.get_env("ESS_MASTER_KEY") ||
      if(config_env() != :prod,
        # Fixed dev/test key so encrypted secrets in Concord survive restarts.
        # NEVER use this value in production — set ESS_MASTER_KEY instead.
        do: Base.encode64("ex_storage_service_dev_master_key!!"),
        else: nil
      ),
  multipart_gc_interval: :timer.hours(1),
  multipart_max_age: :timer.hours(24),
  sync_interval: :timer.seconds(30),
  max_object_size: 5 * 1024 * 1024 * 1024,
  max_part_size: 5 * 1024 * 1024 * 1024,
  # The S3 minimum part size (5 MiB) applies to every part but the last. It is
  # disabled in the test env so multipart mechanics tests can use tiny parts.
  min_part_size: if(config_env() == :test, do: 0, else: 5 * 1024 * 1024)

if config_env() == :prod do
  # ── Security guardrail 1: S3 auth must be explicitly enabled ───────────────
  unless s3_auth_enabled? do
    raise """
    ESS_S3_AUTH_ENABLED must be set to "true" in production.

    Allowing unauthenticated S3 access in production is a critical security risk.
    Set ESS_S3_AUTH_ENABLED=true in your environment or Docker compose file.
    """
  end

  # ── Security guardrail 2: reject the well-known default admin password ──────
  configured_admin_hash = System.get_env("ESS_ADMIN_PASSWORD_HASH", default_admin_hash)

  if configured_admin_hash == default_admin_hash do
    raise """
    ESS_ADMIN_PASSWORD_HASH must be explicitly set in production.

    Do not use the default "admin" password. Generate a hash with:

        echo -n "your-secure-password" | sha256sum | awk '{print $1}'

    Then set ESS_ADMIN_PASSWORD_HASH to that value.
    """
  end

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
  port = String.to_integer(System.get_env("ESS_ADMIN_PORT", "4900"))

  config :ex_storage_service_web, ExStorageServiceWeb.Endpoint,
    url: [host: host, port: 443, scheme: "https"],
    http: [
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: port
    ],
    secret_key_base: secret_key_base,
    server: true
end
