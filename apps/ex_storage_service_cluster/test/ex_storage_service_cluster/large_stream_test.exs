defmodule ExStorageServiceCluster.LargeStreamTest do
  use ExUnit.Case, async: false

  alias ExStorageService.BlobStore.Source
  alias ExStorageService.Cluster.{BlobDescriptor, ReplicaAck}
  alias ExStorageService.{Context, InstanceConfig}
  alias ExStorageServiceCluster.Router
  alias ExStorageServiceCluster.Transport.HTTP

  @secret "phase5-test-secret-at-least-32-bytes"
  @chunk_size 256 * 1_024
  @logical_size 2 * 1_024 * 1_024 * 1_024
  @memory_limit 64 * 1_024 * 1_024

  defmodule ProbeStore do
    @moduledoc false

    alias ExStorageService.BlobStore.{ReadyBlob, Source, StagedBlob}

    def stage_from_reader(reader, state, opts) do
      digests = {:crypto.hash_init(:sha256), :crypto.hash_init(:md5), 0}
      consume(reader, state, digests, Keyword.fetch!(opts, :memory_probe))
    end

    def commit(%StagedBlob{} = staged, _opts) do
      {:ok,
       %ReadyBlob{
         path: "memory-probe",
         hash: staged.hash,
         etag: staged.etag,
         size: staged.size,
         source: Source.file("/dev/null", 0, staged.size)
       }}
    end

    def discard(_staged, _opts), do: :ok

    defp consume(reader, state, digests, probe) do
      sample(probe)

      case reader.(state) do
        {:more, chunk, next_state} ->
          consume(reader, next_state, update(digests, chunk), probe)

        {:ok, chunk, final_state} ->
          {hash, etag, size} = finalize(update(digests, chunk))

          {:ok, %StagedBlob{path: "memory-probe", hash: hash, etag: etag, size: size},
           final_state}

        {:done, final_state} ->
          {hash, etag, size} = finalize(digests)

          {:ok, %StagedBlob{path: "memory-probe", hash: hash, etag: etag, size: size},
           final_state}

        {:error, reason, final_state} ->
          {:error, reason, final_state}
      end
    end

    defp update({sha, md5, size}, chunk) do
      {
        :crypto.hash_update(sha, chunk),
        :crypto.hash_update(md5, chunk),
        size + byte_size(chunk)
      }
    end

    defp finalize({sha, md5, size}) do
      {
        sha |> :crypto.hash_final() |> Base.encode16(case: :lower),
        md5 |> :crypto.hash_final() |> Base.encode16(case: :lower),
        size
      }
    end

    defp sample({probe, index}) do
      {:memory, bytes} = Process.info(self(), :memory)
      ExStorageServiceCluster.LargeStreamTest.record_sample(probe, index, bytes)
    end
  end

  @tag large_stream: true
  @tag timeout: 600_000
  test "a two-gibibyte HTTP upload keeps sender and receiver process memory bounded" do
    probe = :atomics.new(6, signed: false)
    chunk = :binary.copy(<<0>>, @chunk_size)
    chunk_count = div(@logical_size, @chunk_size)
    hash = repeated_hash(chunk, chunk_count)
    %{url: url, context: context} = start_transport(probe, @logical_size)

    source =
      1..chunk_count
      |> Stream.map(fn _index ->
        sample(probe, 1)
        chunk
      end)
      |> Source.stream(@logical_size)

    descriptor = %BlobDescriptor{
      schema: 2,
      hash: hash,
      algorithm: :sha256,
      size: @logical_size,
      desired_replication_factor: 2,
      created_at: DateTime.utc_now()
    }

    assert {:ok, %ReplicaAck{hash: ^hash, size: @logical_size}} =
             HTTP.put_blob(context, url, source, descriptor,
               secret: @secret,
               timeout: 600_000
             )

    sender = memory_samples(probe, 1)
    receiver = memory_samples(probe, 4)

    IO.puts(
      "2 GiB stream process memory bytes: sender=#{inspect(sender)} receiver=#{inspect(receiver)}"
    )

    assert sender.peak < @memory_limit
    assert receiver.peak < @memory_limit
  end

  @doc false
  def record_sample(probe, index, candidate) do
    _first = :atomics.compare_exchange(probe, index, 0, candidate)
    record_max(probe, index + 1, candidate)
    :atomics.put(probe, index + 2, candidate)
  end

  defp record_max(probe, index, candidate) do
    current = :atomics.get(probe, index)

    if candidate > current do
      case :atomics.compare_exchange(probe, index, current, candidate) do
        ^current -> :ok
        _other -> record_max(probe, index, candidate)
      end
    else
      :ok
    end
  end

  defp sample(probe, index) do
    {:memory, bytes} = Process.info(self(), :memory)
    record_sample(probe, index, bytes)
  end

  defp memory_samples(probe, index) do
    %{
      before: :atomics.get(probe, index),
      peak: :atomics.get(probe, index + 1),
      after: :atomics.get(probe, index + 2)
    }
  end

  defp repeated_hash(chunk, count) do
    1..count
    |> Enum.reduce(:crypto.hash_init(:sha256), fn _index, context ->
      :crypto.hash_update(context, chunk)
    end)
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  defp start_transport(probe, maximum) do
    replay_table = :ets.new(:phase5_large_stream_replay, [:set, :public])

    router_opts = [
      secret: @secret,
      replay_table: replay_table,
      auth_skew_seconds: 600,
      node_id: "large-stream-target",
      blob_store: ProbeStore,
      blob_store_opts: [memory_probe: {probe, 4}],
      max_blob_size: maximum,
      read_timeout: 600_000
    ]

    server =
      start_supervised!(
        {Bandit, plug: {Router, router_opts}, ip: {127, 0, 0, 1}, port: 0, startup_log: false}
      )

    assert {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    {:ok, config} = InstanceConfig.new(internal_secret: @secret)

    %{context: Context.new(config), url: "http://127.0.0.1:#{port}"}
  end
end
