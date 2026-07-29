defmodule ExStorageService.Cluster.Scrubber do
  @moduledoc """
  Streams one local blob representation through SHA-256 verification.

  The scrubber is deliberately a plain module. Durable scheduling, leases, and
  repair dispatch belong to the outbox worker; this module only performs one
  bounded, optionally rate-limited verification.
  """

  alias ExStorageService.BlobStore.{LocalCAS, Source}

  @telemetry_event [:ex_storage_service, :cluster, :scrub, :stop]

  @type report :: %{hash: binary(), size: non_neg_integer(), bytes: non_neg_integer()}

  @spec scrub(binary(), keyword()) :: {:ok, report()} | {:error, term()}
  def scrub(hash, opts \\ [])

  def scrub(hash, opts) when is_binary(hash) do
    clock = Keyword.get(opts, :clock, fn -> System.monotonic_time(:millisecond) end)
    started_at = clock.()

    result = do_scrub(hash, opts)
    finished_at = clock.()
    emit(result, hash, max(finished_at - started_at, 0))

    case result do
      {:ok, report} -> {:ok, report}
      {:error, reason, _bytes} -> {:error, reason}
    end
  end

  def scrub(_hash, _opts), do: {:error, :invalid_hash}

  defp do_scrub(hash, opts) do
    blob_store = Keyword.get(opts, :blob_store, LocalCAS)
    blob_opts = Keyword.get(opts, :blob_store_opts, [])

    with {:ok, rate} <- rate(opts),
         {:ok, %{size: expected_size}} <- blob_store.stat(hash, blob_opts),
         {:ok, source} <- blob_store.open(hash, nil, blob_opts) do
      initial = {:crypto.hash_init(:sha256), 0}

      case Source.reduce(source, initial, fn chunk, {digest, bytes} ->
             throttle(chunk, rate, opts)
             {:cont, {:crypto.hash_update(digest, chunk), bytes + byte_size(chunk)}}
           end) do
        {:ok, {digest, ^expected_size}} ->
          actual_hash =
            digest
            |> :crypto.hash_final()
            |> Base.encode16(case: :lower)

          if actual_hash == String.downcase(hash) do
            {:ok, %{hash: hash, size: expected_size, bytes: expected_size}}
          else
            {:error, :checksum_mismatch, expected_size}
          end

        {:ok, {_digest, actual_size}} ->
          {:error, {:size_mismatch, expected_size, actual_size}, actual_size}

        {:error, reason, {_digest, bytes}} ->
          {:error, reason, bytes}
      end
    else
      {:error, reason} -> {:error, reason, 0}
    end
  end

  defp rate(opts) do
    case Keyword.get(opts, :bytes_per_second, :infinity) do
      :infinity -> {:ok, :infinity}
      rate when is_integer(rate) and rate > 0 -> {:ok, rate}
      _other -> {:error, :invalid_rate_limit}
    end
  end

  defp throttle(_chunk, :infinity, _opts), do: :ok

  defp throttle(chunk, bytes_per_second, opts) do
    delay_ms = ceil(byte_size(chunk) * 1_000 / bytes_per_second)
    Keyword.get(opts, :sleep, &Process.sleep/1).(delay_ms)
  end

  defp emit(result, hash, duration) do
    {status, bytes, reason} =
      case result do
        {:ok, %{bytes: bytes}} -> {:ok, bytes, nil}
        {:error, reason, bytes} -> {:error, bytes, reason}
      end

    :telemetry.execute(
      @telemetry_event,
      %{bytes: bytes, duration: duration, count: 1},
      %{hash: hash, status: status, reason: reason}
    )
  end
end
