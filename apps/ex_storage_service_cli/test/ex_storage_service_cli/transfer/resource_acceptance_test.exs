Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transfer.ResourceAcceptanceTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.Context
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transfer.{Coordinator, Engine}
  @moduletag :tmp_dir
  @moduletag :resource
  @moduletag timeout: 600_000
  @chunk 65_536
  @budget 1_048_576
  @overhead 67_108_864

  test "throttled wire transfers keep the same measured resource envelope as object size grows",
       %{tmp_dir: root} do
    large = System.get_env("ESS_RESOURCE_LARGE_BYTES", "67108864") |> String.to_integer()
    assert large >= 64 * @budget
    # Load models, transport and crypto before measuring the two comparable runs.
    measure(root, 262_144)
    small = measure(root, @budget)
    big = measure(root, large)

    for measurement <- [small, big] do
      assert measurement.samples > 5
      assert measurement.peak_total - measurement.baseline_total <= @budget + @overhead
      assert measurement.peak_binary - measurement.baseline_binary <= @budget + @overhead
      assert measurement.peak_processes - measurement.baseline_processes <= 48
      assert measurement.peak_mailbox <= 128
      assert measurement.requests.high_requests == 1
      assert measurement.requests.high_bytes <= @budget
      assert measurement.requests.active == 0
      assert measurement.requests.bytes == 0
      assert measurement.peak_staging <= measurement.size
      assert measurement.peak_staging > 0
    end

    assert big.peak_total - big.baseline_total <=
             small.peak_total - small.baseline_total + @overhead

    IO.puts(
      "ESS_RESOURCE_EVIDENCE " <>
        JSON.encode!(%{small: small, large: big, budget: @budget, fixed_overhead: @overhead})
    )
  end

  defp measure(root, size) do
    source = Path.join(root, "source")
    destination = Path.join(root, "destination")
    {:ok, file} = File.open(source, [:write, :binary, :raw])
    {:ok, _} = :file.position(file, size)
    :ok = :file.truncate(file)
    :ok = File.close(file)
    File.rm(destination)

    coordinator =
      start_supervised!({Coordinator, max_requests: 1, memory_budget: @budget}, id: make_ref())

    {endpoint, _peer} = streaming_peer(size)

    context = %Context{
      endpoint: endpoint,
      unsigned: true,
      coordinator: coordinator,
      timeout: 300_000,
      options: %{request_checksum_calculation: "when_required"}
    }

    :erlang.garbage_collect()
    initial = sample(root)
    parent = self()
    sampler = spawn_link(fn -> monitor_resources(parent, root, initial) end)

    assert {:ok, %{bytes: ^size}} =
             Engine.upload_file(context, "bucket", "key", source, multipart_threshold: size + 1)

    assert_receive {:upload_streamed, ^size}, 5000
    assert {:ok, %{bytes: ^size}} = Engine.download_file(context, "bucket", "key", destination)
    assert_receive {:download_streamed, ^size}, 5000
    send(sampler, :finish)
    assert_receive {:resource_samples, metrics}, 5000
    assert File.stat!(source).size == size
    assert File.stat!(destination).size == size
    assert Path.wildcard(Path.join(root, ".ess-download-*"), match_dot: true) == []
    assert {:ok, file} = File.open(destination, [:read, :binary, :raw])
    assert {:ok, <<0::64>>} = :file.pread(file, size - 8, 8)
    File.close(file)
    metrics |> Map.put(:size, size) |> Map.put(:requests, Coordinator.stats(coordinator))
  end

  defp streaming_peer(size) do
    WireServer.start(fn listener, owner ->
      {:ok, upload} = :gen_tcp.accept(listener, 60_000)
      {head, rest} = WireServer.head(upload)
      assert head =~ "PUT /bucket/key "
      assert String.downcase(head) =~ "content-length: #{size}"
      assert byte_size(rest) <= @chunk * 2
      assert drain(upload, size - byte_size(rest), byte_size(rest)) == size
      WireServer.response(upload, 200, [{"ETag", "\"uploaded\""}], "")
      :gen_tcp.close(upload)
      send(owner, {:upload_streamed, size})
      {:ok, download} = :gen_tcp.accept(listener, 60_000)
      {head, ""} = WireServer.head(download)
      assert head =~ "GET /bucket/key "

      :ok =
        :gen_tcp.send(
          download,
          "HTTP/1.1 200 OK\r\nContent-Length: #{size}\r\nConnection: close\r\n\r\n"
        )

      send_zeros(download, size, :binary.copy(<<0>>, @chunk))
      :gen_tcp.close(download)
      send(owner, {:download_streamed, size})
    end)
  end

  defp drain(_socket, 0, total), do: total

  defp drain(socket, remaining, total) do
    {:ok, bytes} = :gen_tcp.recv(socket, min(remaining, @chunk), 60_000)
    # The peer retains one chunk and applies real socket backpressure.
    Process.sleep(1)
    drain(socket, remaining - byte_size(bytes), total + byte_size(bytes))
  end

  defp send_zeros(_socket, 0, _chunk), do: :ok

  defp send_zeros(socket, remaining, chunk) do
    length = min(remaining, byte_size(chunk))
    :ok = :gen_tcp.send(socket, binary_part(chunk, 0, length))
    Process.sleep(1)
    send_zeros(socket, remaining - length, chunk)
  end

  defp sample(root) do
    memory = :erlang.memory([:total, :binary]) |> Map.new()
    pids = Process.list()

    mailbox =
      Enum.reduce(pids, 0, fn pid, maximum ->
        case Process.info(pid, :message_queue_len) do
          {:message_queue_len, count} -> max(count, maximum)
          nil -> maximum
        end
      end)

    staging =
      Path.wildcard(Path.join(root, ".ess-download-*"), match_dot: true)
      |> Enum.reduce(0, fn path, total ->
        case File.stat(path) do
          {:ok, stat} -> total + stat.size
          _ -> total
        end
      end)

    %{
      total: memory.total,
      binary: memory.binary,
      processes: length(pids),
      mailbox: mailbox,
      staging: staging
    }
  end

  defp monitor_resources(owner, root, initial) do
    metrics = %{
      baseline_total: initial.total,
      baseline_binary: initial.binary,
      baseline_processes: initial.processes,
      peak_total: initial.total,
      peak_binary: initial.binary,
      peak_processes: initial.processes,
      peak_mailbox: initial.mailbox,
      peak_staging: initial.staging,
      samples: 0
    }

    monitor_loop(owner, root, metrics)
  end

  defp monitor_loop(owner, root, metrics) do
    latest = sample(root)

    metrics = %{
      metrics
      | peak_total: max(metrics.peak_total, latest.total),
        peak_binary: max(metrics.peak_binary, latest.binary),
        peak_processes: max(metrics.peak_processes, latest.processes),
        peak_mailbox: max(metrics.peak_mailbox, latest.mailbox),
        peak_staging: max(metrics.peak_staging, latest.staging),
        samples: metrics.samples + 1
    }

    receive do
      :finish -> send(owner, {:resource_samples, metrics})
    after
      10 -> monitor_loop(owner, root, metrics)
    end
  end
end
