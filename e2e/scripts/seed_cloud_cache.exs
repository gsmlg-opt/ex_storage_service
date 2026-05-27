alias ExStorageService.CloudCache.Config, as: CloudConfig
alias ExStorageService.Metadata

Application.ensure_all_started(:ex_storage_service)

defmodule CloudCacheE2ESeed do
  @moduledoc false

  @doc """
  Seeds a local bucket with cloud cache configuration pointing at the
  upstream MinIO instance for E2E testing.

  Expected environment variables:
    - MINIO_ENDPOINT      — upstream MinIO URL (e.g. http://localhost:9100)
    - MINIO_ACCESS_KEY    — upstream MinIO root user
    - MINIO_SECRET_KEY    — upstream MinIO root password
    - MINIO_BUCKET        — upstream bucket name (default: upstream-e2e)
    - ESS_CLOUD_E2E_BUCKET — local ESS bucket name (default: cloud-e2e)
  """
  def run do
    endpoint = System.get_env("MINIO_ENDPOINT", "http://localhost:9100")
    access_key = System.get_env("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = System.get_env("MINIO_SECRET_KEY", "minioadmin")
    remote_bucket = System.get_env("MINIO_BUCKET", "upstream-e2e")
    local_bucket = System.get_env("ESS_CLOUD_E2E_BUCKET", "cloud-e2e")

    # 1. Create local ESS bucket
    case Metadata.create_bucket(local_bucket) do
      :ok ->
        IO.puts("Created local bucket: #{local_bucket}")

      {:error, :already_exists} ->
        IO.puts("Local bucket already exists: #{local_bucket}")

      {:error, reason} ->
        IO.puts(:stderr, "Failed to create bucket #{local_bucket}: #{inspect(reason)}")
        System.halt(1)
    end

    # 2. Set cloud cache config pointing at upstream MinIO
    case CloudConfig.set_config(local_bucket, %{
           enabled: true,
           provider: :minio,
           endpoint: endpoint,
           region: "us-east-1",
           bucket: remote_bucket,
           secret_access_key: secret_key,
           access_key_id: access_key,
           cache_max_bytes: 10_737_418_240,
           cache_enabled: true
         }) do
      :ok ->
        IO.puts("Cloud cache configured: #{local_bucket} → #{endpoint}/#{remote_bucket}")

      {:error, reason} ->
        IO.puts(:stderr, "Failed to set cloud cache config: #{inspect(reason)}")
        System.halt(1)
    end

    # 3. Verify config was saved
    case CloudConfig.get_active_config(local_bucket) do
      {:ok, config} ->
        IO.puts(
          "Verified cloud cache active: provider=#{config.provider} bucket=#{config.bucket}"
        )

      :disabled ->
        IO.puts(:stderr, "Cloud cache config was saved but reads back as disabled!")
        System.halt(1)
    end

    # 4. Stop storage apps cleanly
    Enum.each([:ex_storage_service, :concord, :ra], fn app ->
      Application.stop(app)
    end)

    IO.puts("Cloud cache E2E seed complete.")
  end
end

CloudCacheE2ESeed.run()
