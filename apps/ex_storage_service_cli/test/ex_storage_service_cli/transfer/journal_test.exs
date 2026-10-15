defmodule ExStorageServiceCli.Transfer.JournalTest do
  # The deterministic mutex ports share the host namespace with wire-test peers.
  # Exercise ownership concurrency inside this suite, without concurrent HTTP fixtures.
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Error, Transfer.Journal, Transfer.Source}
  @moduletag :tmp_dir

  setup %{tmp_dir: root} do
    path = Path.join(root, "source")
    File.write!(path, "abcdefghij")
    {:ok, source} = Source.file(path)

    record = %{
      "version" => 1,
      "endpoint" => %{
        "url" => "https://s3.example.com",
        "region" => "us-east-1",
        "service" => "s3",
        "addressing" => "path"
      },
      "credential_provider" => %{"kind" => "profile", "name" => "default"},
      "target" => %{"bucket" => "bucket", "key" => "dir/key"},
      "source" => Journal.source_record(source),
      "upload_id" => "owned-upload",
      "options" => %{"part_size" => 5, "checksum_algorithm" => "SHA256"},
      "parts" => [
        %{
          "number" => 1,
          "offset" => 0,
          "length" => 5,
          "status" => "completed",
          "etag" => "first",
          "checksums" => %{"SHA256" => "known"}
        },
        %{"number" => 2, "offset" => 5, "length" => 5, "status" => "pending", "etag" => nil}
      ],
      "phase" => "transferring",
      "cleanup" => "none"
    }

    %{record: record, root: Path.join(root, "journals")}
  end

  test "strict versioned schema refuses secret-bearing or arbitrary fields", %{record: record} do
    assert :ok = Journal.validate(record)

    for invalid <- [
          Map.put(record, "version", 2),
          Map.put(record, "secret_access_key", "private"),
          put_in(record, ["credential_provider", "session_token"], "private"),
          put_in(record, ["options", "sse_customer_key"], "private"),
          put_in(record, ["options", "headers"], %{"Authorization" => "private"}),
          put_in(record, ["endpoint", "url"], "https://user:password@example.com"),
          put_in(record, ["endpoint", "url"], "https://example.com?X-Amz-Signature=private"),
          put_in(record, ["source", "identity", "extra"], "private"),
          Map.put(record, "parts", [Map.put(hd(record["parts"]), "number", 2)])
        ] do
      assert {:error, %Error{code: :journal_schema} = error} = Journal.validate(invalid)
      refute inspect(error) =~ "private"
    end
  end

  test "resume rejects endpoint, key, file identity, hash, and option changes", %{record: record} do
    assert :ok = Journal.resume_matches(record, record)

    for changed <- [
          put_in(record, ["endpoint", "region"], "eu-west-1"),
          put_in(record, ["target", "key"], "other"),
          put_in(record, ["source", "identity", "inode"], 0),
          put_in(record, ["source", "payload_hash"], String.duplicate("0", 64)),
          put_in(record, ["options", "part_size"], 6),
          put_in(record, ["credential_provider", "name"], "different")
        ] do
      assert {:error, %Error{code: :resume_conflict}} = Journal.resume_matches(record, changed)
    end
  end

  test "reconcile keeps only acknowledged or independently checksum-matched progress", %{
    record: record
  } do
    remote = [
      %{"number" => 2, "size" => 5, "etag" => "unacknowledged"},
      %{"number" => 1, "size" => 5, "etag" => "first", "checksums" => %{"SHA256" => "known"}}
    ]

    assert {:ok, updated} = Journal.reconcile(record, remote, complete: true)
    assert Enum.at(updated["parts"], 0)["status"] == "completed"
    assert Enum.at(updated["parts"], 1)["status"] == "pending"
    assert Enum.at(updated["parts"], 1)["etag"] == nil

    with_checksum =
      update_in(record, ["parts"], fn [first, second] ->
        [first, Map.put(second, "checksums", %{"SHA256" => "second"})]
      end)

    remote = [
      %{"number" => 2, "size" => 5, "etag" => "verified", "checksums" => %{"SHA256" => "second"}}
    ]

    assert {:ok, updated} = Journal.reconcile(with_checksum, remote, complete: true)
    assert Enum.at(updated["parts"], 0)["status"] == "pending"
    assert Enum.at(updated["parts"], 1)["status"] == "completed"
  end

  test "partial, duplicate, unexpected, wrong-size and conflicting acknowledged parts fail", %{
    record: record
  } do
    assert {:error, %Error{code: :incomplete_parts_inventory}} = Journal.reconcile(record, [])
    valid = %{"number" => 1, "size" => 5, "etag" => "first"}

    for remote <- [
          [valid, valid],
          [Map.put(valid, "number", 3)],
          [Map.put(valid, "size", 6)],
          [Map.put(valid, "etag", "different")]
        ] do
      assert {:error, %Error{code: :remote_parts_conflict}} =
               Journal.reconcile(record, remote, complete: true)
    end
  end

  test "private atomic checkpoints are revision fenced and indexed with bounded event pages", %{
    root: root,
    record: record
  } do
    ids =
      for _ <- 1..3 do
        assert {:ok, id} = Journal.create(root, record)
        id
      end

    [id | _] = ids
    assert {:ok, current} = Journal.get(root, id)
    assert current["revision"] == 0
    assert {:ok, %{entries: first, next: cursor}} = Journal.list(root, limit: 2)
    assert Enum.map(first, & &1["id"]) == Enum.take(ids, 2)
    assert {:ok, %{entries: [last], next: nil}} = Journal.list(root, cursor: cursor, limit: 2)
    assert last["id"] == List.last(ids)
    assert {:ok, claim} = Journal.claim(root, id)
    assert :ok = Journal.checkpoint(claim, Map.put(current, "phase", "retained"))
    assert {:error, %Error{code: :journal_revision}} = Journal.checkpoint(claim, current)
    assert :ok = Journal.release(claim)
    assert {:ok, restored} = Journal.get(root, id)
    assert restored["revision"] == 1
    assert restored["phase"] == "retained"

    assert {:ok, %{entries: [%{"revision" => 0}], next: cursor}} =
             Journal.events(root, id, limit: 1)

    assert {:ok, %{entries: [%{"revision" => 1}], next: nil}} =
             Journal.events(root, id, cursor: cursor, limit: 2)

    assert Bitwise.band(File.stat!(root).mode, 0o777) == 0o700
    assert Bitwise.band(File.stat!(Path.join(root, id <> ".json")).mode, 0o777) == 0o600
    refute File.read!(Path.join(root, id <> ".json")) =~ "secret_access_key"
    refute Enum.any?(File.ls!(root), &String.contains?(&1, ".tmp."))
  end

  test "cross-process ownership and equivalent paths exclude simultaneous resumers", %{
    root: root,
    record: record
  } do
    {:ok, id} = Journal.create(root, record)
    {:ok, claim} = Journal.claim(root, id)
    alternative = Path.join(root, "../" <> Path.basename(root))
    result = Task.async(fn -> Journal.claim(alternative, id) end) |> Task.await()
    assert {:error, %Error{code: :journal_busy}} = result

    assert {:error, %Error{code: :journal_owner}} =
             Task.async(fn -> Journal.release(claim) end) |> Task.await()

    assert :ok = Journal.release(claim)
    assert :ok = Journal.with_claim(alternative, id, fn _claim -> :ok end)
  end

  test "abrupt owner death releases the kernel mutex and fences the old claim", %{
    root: root,
    record: record
  } do
    {:ok, id} = Journal.create(root, record)
    {:ok, claim} = Journal.claim(root, id)
    monitor = Process.monitor(claim.owner)
    Process.exit(claim.owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, _, :killed}
    assert {:error, %Error{code: :journal_owner}} = Journal.checkpoint(claim, record)
    assert {:ok, replacement} = Journal.claim(root, id)
    assert :ok = Journal.release(replacement)
    assert {:error, %Error{code: :journal_owner}} = Journal.release(claim)
    assert {:ok, %{"revision" => 0}} = Journal.get(root, id)
  end

  test "claimant death closes the dedicated socket owner and releases ownership", %{
    root: root,
    record: record
  } do
    {:ok, id} = Journal.create(root, record)
    parent = self()

    holder =
      spawn(fn ->
        {:ok, claim} = Journal.claim(root, id)
        send(parent, {:owner, claim.owner})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:owner, owner}
    monitor = Process.monitor(owner)
    Process.exit(holder, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, _}
    assert :ok = Journal.with_claim(root, id, fn _ -> :ok end)
  end

  test "unrelated listeners return busy without stealing or closing their sockets", %{
    root: root,
    record: record
  } do
    {:ok, id} = Journal.create(root, record)
    {:ok, port} = Journal.lock_port(root, id)

    {:ok, socket} =
      :gen_tcp.listen(port, [:binary, active: false, reuseaddr: false, ip: {127, 0, 0, 1}])

    try do
      assert {:error, %Error{code: :journal_busy}} = Journal.claim(root, id)
      assert {:ok, {{127, 0, 0, 1}, ^port}} = :inet.sockname(socket)
    after
      :gen_tcp.close(socket)
    end

    assert :ok = Journal.with_claim(root, id, fn _ -> :ok end)
  end

  test "a separate VM excludes resumers and abrupt VM halt permits crash resume", %{
    root: root,
    record: record
  } do
    {:ok, id} = Journal.create(root, record)

    paths =
      :code.get_path()
      |> Enum.map(&List.to_string/1)
      |> Enum.filter(&String.contains?(&1, "_build/test/lib/"))

    code =
      "{:ok, _claim} = ExStorageServiceCli.Transfer.Journal.claim(Enum.at(System.argv(), 0), Enum.at(System.argv(), 1)); IO.puts(\"CLAIMED\"); IO.gets(\"\"); System.halt(7)"

    port =
      Port.open(
        {:spawn_executable, System.find_executable("elixir")},
        [
          :binary,
          :exit_status,
          :use_stdio,
          :stderr_to_stdout,
          args:
            Enum.flat_map(paths, &["-pa", &1]) ++ ["--erl", "+S 2:2", "-e", code, "--", root, id]
        ]
      )

    try do
      assert receive_marker(port, "", "CLAIMED") =~ "CLAIMED"
      assert {:error, %Error{code: :journal_busy}} = Journal.claim(root, id)
      Port.command(port, "halt\n")
      assert_receive {^port, {:exit_status, 7}}, 5_000
      assert :ok = Journal.with_claim(root, id, fn _ -> :ok end)
      assert {:ok, %{"revision" => 0}} = Journal.get(root, id)
    after
      if Port.info(port), do: Port.close(port)
    end
  end

  test "exception cleanup, corrupt/oversized records, symlinks, and malformed inventory fail safely",
       %{root: root, record: record, tmp_dir: tmp_dir} do
    {:ok, id} = Journal.create(root, record)

    assert_raise RuntimeError, "callback", fn ->
      Journal.with_claim(root, id, fn _ -> raise "callback" end)
    end

    assert :ok = Journal.with_claim(root, id, fn _ -> :ok end)
    alias_root = Path.join(tmp_dir, "alias")
    File.ln_s!(root, alias_root)
    assert {:error, %Error{code: :journal_path}} = Journal.claim(alias_root, id)
    File.write!(Path.join(root, "index.jsonl"), String.duplicate("x", 4097))
    assert {:error, %Error{code: :journal_index}} = Journal.list(root)
    path = Path.join(root, id <> ".json")
    File.write!(path, "{broken")
    assert {:error, %Error{code: :journal_malformed}} = Journal.get(root, id)
    assert File.read!(path) == "{broken"
    File.write!(path, String.duplicate("x", 8_388_609))
    assert {:error, %Error{code: :journal_read}} = Journal.get(root, id)
    assert {:error, %Error{code: :journal_id}} = Journal.get(root, "../escape")
    assert {:error, %Error{code: :journal_page}} = Journal.events(root, id, limit: 101)
  end

  defp receive_marker(port, accumulated, marker) do
    receive do
      {^port, {:data, chunk}} ->
        accumulated = accumulated <> chunk

        if String.contains?(accumulated, marker),
          do: accumulated,
          else: receive_marker(port, accumulated, marker)

      {^port, {:exit_status, status}} ->
        flunk("VM exited before ownership marker: #{status}, #{accumulated}")
    after
      5_000 -> flunk("VM did not acknowledge ownership: #{accumulated}")
    end
  end
end
