Code.require_file("../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.ExecutableSignalTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transfer.Journal

  @moduletag :tmp_dir

  setup_all do
    root = Path.expand("../..", __DIR__)

    temporary =
      Path.join(System.tmp_dir!(), "ess-signal-executable-#{System.unique_integer([:positive])}")

    File.mkdir_p!(temporary)

    {output, status} =
      System.cmd("mix", ["escript.build", "--no-compile"],
        cd: root,
        env: [{"MIX_ENV", "test"}, {"PAGER", "cat"}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    executable = Path.join(temporary, "ess")
    File.cp!(Path.join(root, "ess"), executable)
    on_exit(fn -> File.rm_rf!(temporary) end)
    %{executable: executable}
  end

  test "standalone artifact includes the native bridge, service models and legal notices",
       context do
    {:ok, sections} = :escript.extract(String.to_charlist(context.executable), [])
    {:ok, directory} = :zip.list_dir(Keyword.fetch!(sections, :archive))

    names =
      for entry <- directory, elem(entry, 0) == :zip_file, do: entry |> elem(1) |> to_string()

    for name <- [
          "native/ess_signals.so",
          "native/ess_signals.json",
          "native/ess_pager",
          "native/ess_pager.json",
          "compatibility/NOTICE.txt",
          "compatibility/NOTICE-QUERY.txt",
          "compatibility/NOTICE-YAML.txt",
          "compatibility/aws/service-2.json",
          "compatibility/aws/cli.json"
        ] do
      assert ("ex_storage_service_cli/priv/" <> name) in names
    end
  end

  for {signal, status} <- [{"INT", 130}, {"TERM", 143}] do
    test "real SIG#{signal} interrupts a stalled download and preserves the prior destination",
         context do
      destination = Path.join(context.tmp_dir, "destination")
      File.write!(destination, "prior contents")
      {endpoint, _} = download_peer()
      {port, pid} = start(context, ["--endpoint", endpoint, "cp", "s3://bucket/key", destination])
      assert_receive {:download_stalled, request}, 5000
      assert request.method == "GET"
      assert request.headers["if-match"] == "\"source\""
      assert [_] = await_staging(context.tmp_dir, 200)
      assert {"", 0} = System.cmd("kill", ["-" <> unquote(signal), to_string(pid)])
      assert {output, unquote(status)} = collect(port, "")
      assert_receive {:download_closed, {:error, :closed}}, 2000
      assert File.read!(destination) == "prior contents"
      assert Path.wildcard(Path.join(context.tmp_dir, ".ess-download-*"), match_dot: true) == []
      assert Path.wildcard(Path.join(context.tmp_dir, "ess-native-*")) == []
      refute output =~ "fixture-secret"
    end

    test "real SIG#{signal} after a multipart checkpoint aborts only the owned upload and releases its journal",
         context do
      source = Path.join(context.tmp_dir, "source")
      File.write!(source, String.duplicate("x", 9 * 1_048_576))
      {endpoint, _} = multipart_peer()
      {port, pid} = start(context, ["--endpoint", endpoint, "cp", source, "s3://bucket/key"])
      assert_receive {:multipart_stalled, request}, 30_000
      assert request.method == "PUT"
      assert request.target == "/bucket/key?partNumber=2&uploadId=owned-signal"
      assert byte_size(request.body) == 4 * 1_048_576
      root = Path.join(context.tmp_dir, "state/transfers")
      assert {:ok, %{entries: [%{"id" => id}], next: nil}} = Journal.list(root)
      assert {:ok, before} = Journal.get(root, id)
      assert before["phase"] == "transferring"
      assert [first, second] = before["parts"]
      assert first["status"] == "completed"
      assert first["etag"] == "\"first\""
      assert is_binary(first["checksums"]["CRC64NVME"])
      assert second["status"] != "completed"
      assert {"", 0} = System.cmd("kill", ["-" <> unquote(signal), to_string(pid)])
      assert {output, unquote(status)} = collect(port, "")
      assert_receive {:part_closed, {:error, :closed}}, 2000

      assert_receive {:owned_abort,
                      %{method: "DELETE", target: "/bucket/key?uploadId=owned-signal"}},
                     2000

      assert {:ok, record} = Journal.get(root, id)
      assert record["cleanup"] == "aborted"
      assert record["phase"] == "failed"
      assert record["revision"] > before["revision"]
      assert record["upload_id"] == "owned-signal"
      assert {:ok, %{entries: events}} = Journal.events(root, id)
      assert Enum.any?(events, &(&1["phase"] == "aborting"))
      assert List.last(events)["phase"] == "failed"
      assert {:ok, claim} = Journal.claim(root, id)
      assert :ok = Journal.release(claim)
      assert File.read!(source) == String.duplicate("x", 9 * 1_048_576)
      refute File.read!(Path.join(root, id <> ".json")) =~ "fixture-secret"
      refute output =~ "fixture-secret"
      refute_receive {:wire_request, %{method: "POST"}}, 50
    end
  end

  defp download_peer do
    WireServer.start(fn listener, owner ->
      {:ok, head} = :gen_tcp.accept(listener, 15_000)
      %{method: "HEAD"} = WireServer.request(head)
      WireServer.response(head, 200, [{"Content-Length", "100000"}, {"ETag", "\"source\""}], "")
      :gen_tcp.close(head)
      {:ok, socket} = :gen_tcp.accept(listener, 15_000)
      request = WireServer.request(socket)

      :ok =
        :gen_tcp.send(
          socket,
          "HTTP/1.1 200 Fixture\r\nContent-Length: 100000\r\nETag: \"source\"\r\nConnection: close\r\n\r\nx"
        )

      send(owner, {:download_stalled, request})
      send(owner, {:download_closed, :gen_tcp.recv(socket, 0, 5000)})
      :gen_tcp.close(socket)
    end)
  end

  defp multipart_peer do
    WireServer.start(fn listener, owner ->
      {:ok, initiation} = :gen_tcp.accept(listener, 15_000)
      %{method: "POST", target: "/bucket/key?uploads="} = WireServer.request(initiation)

      WireServer.response(
        initiation,
        200,
        [],
        "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>owned-signal</UploadId></InitiateMultipartUploadResult>"
      )

      :gen_tcp.close(initiation)
      {:ok, first} = :gen_tcp.accept(listener, 15_000)

      %{method: "PUT", target: "/bucket/key?partNumber=1&uploadId=owned-signal", body: bytes} =
        WireServer.request(first)

      true = byte_size(bytes) == 5 * 1_048_576
      WireServer.response(first, 200, [{"ETag", "\"first\""}], "")
      :gen_tcp.close(first)
      {:ok, second} = :gen_tcp.accept(listener, 15_000)
      send(owner, {:multipart_stalled, WireServer.request(second)})
      send(owner, {:part_closed, :gen_tcp.recv(second, 0, 5000)})
      :gen_tcp.close(second)
      {:ok, abort} = :gen_tcp.accept(listener, 5000)
      send(owner, {:owned_abort, WireServer.request(abort)})
      WireServer.response(abort, 204, [], "")
      :gen_tcp.close(abort)
    end)
  end

  defp start(context, args) do
    env = [
      {~c"HOME", String.to_charlist(context.tmp_dir)},
      {~c"TMPDIR", String.to_charlist(context.tmp_dir)},
      {~c"ESS_STATE_DIR", String.to_charlist(Path.join(context.tmp_dir, "state"))},
      {~c"ERL_FLAGS", ~c"+S 2:2"},
      {~c"ESS_CONFIG_DIR", false},
      {~c"XDG_CONFIG_HOME", false}
    ]

    port =
      Port.open(
        {:spawn_executable, String.to_charlist(System.find_executable("escript"))},
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: [
            context.executable,
            "--access-key",
            "fixture-access",
            "--secret-key",
            "fixture-secret" | args
          ],
          env: env
        ]
      )

    {:os_pid, pid} = Port.info(port, :os_pid)

    on_exit(fn ->
      if Port.info(port) do
        System.cmd("kill", ["-KILL", to_string(pid)], stderr_to_stdout: true)

        try do
          Port.close(port)
        catch
          :error, :badarg -> :ok
        end
      end
    end)

    {port, pid}
  end

  defp collect(port, output) do
    receive do
      {^port, {:data, bytes}} -> collect(port, output <> bytes)
      {^port, {:exit_status, status}} -> {output, status}
    after
      10_000 -> flunk("Signalled escript did not finish within ten seconds")
    end
  end

  defp await_staging(directory, remaining) do
    case Path.wildcard(Path.join(directory, ".ess-download-*"), match_dot: true) do
      [] when remaining > 0 ->
        Process.sleep(10)
        await_staging(directory, remaining - 1)

      paths ->
        paths
    end
  end
end
