defmodule ExStorageServiceCli.Transfer.Journal do
  @moduledoc """
  Versioned secret-free multipart checkpoints and conservative resume reconciliation.

  Ownership requires an available loopback TCP port. The port is a kernel mutex,
  not a service: no connections are accepted. A dedicated process owns the
  socket and monitors the claimant, so process/VM death releases ownership.
  Hash collisions or unrelated listeners safely return journal_busy. No stale
  lock is deleted and no timeout is used to steal a live owner's lock.

  Checkpoints are bounded JSON files synced before atomic rename. A bounded
  append-only index enumerates transfers; separate events record checkpoint
  revisions and digests. File sync plus rename proves process-crash recovery;
  it does not claim filesystem-independent power-loss durability.
  """
  alias ExStorageServiceCli.{Error, Transfer.Source}
  alias ExStorageServiceCli.Transfer.RemoteSource

  @phases ~w(planned initiating transferring completing verifying completed failed cancelled aborting abort_failed retained indeterminate)
  @root_keys ~w(version id revision endpoint credential_provider target source upload_id options parts phase completion cleanup)
  @context_keys ~w(endpoint credential_provider target source options)
  @checksums ~w(SHA256 SHA1 CRC32 CRC32C CRC64NVME MD5)
  @max_bytes 8_388_608
  @max_line 4096

  defmodule Claim do
    @moduledoc false
    defstruct [:root, :id, :owner, :holder, :monitor, :token, :port, :identity]
  end

  @doc "Creates an indexed v1 record under a private, symlink-free state directory."
  def create(root, record) do
    with :ok <- validate(record),
         {:ok, root, identity} <- prepare_root(root, true),
         {:ok, claim} <- acquire(root, "index", identity) do
      try do
        call(claim, {:create, record})
      after
        release(claim)
      end
    end
  end

  @doc "Reads one bounded atomic checkpoint."
  def get(root, id) do
    with :ok <- valid_id(id),
         {:ok, root, _identity} <- prepare_root(root, false),
         {:ok, document} <- read_json(record_path(root, id)),
         :ok <- validate(document) do
      if document["id"] == id,
        do: {:ok, document},
        else: failure(:journal_schema, "Transfer journal identity is inconsistent")
    end
  end

  @doc "Acquires exclusive resumer ownership; live owners and port collisions fail busy."
  def claim(root, id) do
    with {:ok, _record} <- get(root, id),
         {:ok, root, identity} <- prepare_root(root, false),
         do: acquire(root, id, identity)
  end

  @doc "Writes an immutable-context, revision-fenced progress checkpoint under the claim."
  def checkpoint(%Claim{} = claim, record), do: call(claim, {:checkpoint, record})

  @doc "Always releases ownership after the callback, including callback exceptions."
  def with_claim(root, id, function) when is_function(function, 1) do
    with {:ok, claim} <- claim(root, id) do
      try do
        function.(claim)
      after
        release(claim)
      end
    end
  end

  def release(%Claim{} = claim) do
    if claim.holder == self() do
      result = call(claim, :release)

      if result == :ok do
        # A release is acknowledged only once the kernel bind is gone.
        receive do
          {:DOWN, monitor, :process, owner, _}
          when monitor == claim.monitor and owner == claim.owner ->
            :ok
        end
      end

      Process.demonitor(claim.monitor, [:flush])
      result
    else
      failure(:journal_owner, "Transfer journal claim belongs to another process")
    end
  end

  @doc "Reads at most 100 index entries from an opaque byte-offset cursor."
  def list(root, opts \\ []) do
    with {:ok, root, _} <- prepare_root(root, false),
         {:ok, page} <- read_page(Path.join(root, "index.jsonl"), opts) do
      entries =
        Enum.map(page.entries, fn entry ->
          id = entry["id"]

          case get(root, id) do
            {:ok, record} ->
              %{
                "id" => id,
                "phase" => record["phase"],
                "target" => record["target"],
                "revision" => record["revision"]
              }

            {:error, error} ->
              %{"id" => id, "phase" => "unreadable", "code" => Atom.to_string(error.code)}
          end
        end)

      {:ok, %{page | entries: entries}}
    end
  end

  @doc "Reads a bounded page of append-only checkpoint events."
  def events(root, id, opts \\ []) do
    with :ok <- valid_id(id),
         {:ok, root, _} <- prepare_root(root, false),
         do: read_page(event_path(root, id), opts)
  end

  @doc false
  def lock_port(root, id) do
    with {:ok, _root, identity} <- prepare_root(root, false),
         do: {:ok, port(identity, id)}
  end

  defp acquire(root, id, identity) do
    holder = self()
    token = make_ref()
    port = port(identity, id)

    {owner, monitor} =
      spawn_monitor(fn ->
        holder_monitor = Process.monitor(holder)

        case :gen_tcp.listen(port, [:binary, active: false, ip: {127, 0, 0, 1}, reuseaddr: false]) do
          {:ok, socket} ->
            send(holder, {:journal_acquired, token})

            try do
              owner_loop(root, id, identity, holder, holder_monitor, token)
            after
              :gen_tcp.close(socket)
            end

          {:error, reason} ->
            send(holder, {:journal_busy, token, reason})
        end
      end)

    receive do
      {:journal_acquired, ^token} ->
        {:ok,
         %Claim{
           root: root,
           id: id,
           owner: owner,
           holder: holder,
           monitor: monitor,
           token: token,
           port: port,
           identity: identity
         }}

      {:journal_busy, ^token, reason} ->
        Process.demonitor(monitor, [:flush])

        {:error,
         Error.new(
           :client,
           "Transfer journal ownership is busy or its loopback mutex is unavailable",
           code: :journal_busy,
           details: %{mutex_port: port, socket_reason: reason}
         )}

      {:DOWN, ^monitor, :process, ^owner, _} ->
        failure(:journal_owner, "Transfer journal owner could not start")
    end
  end

  defp owner_loop(root, id, identity, holder, monitor, token) do
    receive do
      {:journal_call, ^holder, reference, ^token, :release} ->
        send(holder, {:journal_reply, reference, :ok})

      {:journal_call, ^holder, reference, ^token, operation} ->
        result =
          with {:ok, _root, ^identity} <- prepare_root(root, false) do
            owner_operation(root, id, operation)
          else
            _ ->
              failure(
                :journal_root_changed,
                "Transfer journal directory changed during ownership"
              )
          end

        send(holder, {:journal_reply, reference, result})
        owner_loop(root, id, identity, holder, monitor, token)

      {:DOWN, ^monitor, :process, ^holder, _} ->
        :ok

      _message ->
        owner_loop(root, id, identity, holder, monitor, token)
    end
  end

  defp call(%Claim{holder: holder} = claim, operation) when holder == self() do
    reference = make_ref()
    monitor = Process.monitor(claim.owner)
    send(claim.owner, {:journal_call, self(), reference, claim.token, operation})

    try do
      receive do
        {:journal_reply, ^reference, result} ->
          result

        {:DOWN, ^monitor, :process, owner, _} when owner == claim.owner ->
          failure(
            :journal_owner,
            "Transfer journal owner ended before acknowledging the operation"
          )
      end
    after
      Process.demonitor(monitor, [:flush])
    end
  end

  defp call(_claim, _operation),
    do: failure(:journal_owner, "Transfer journal claim belongs to another process")

  defp owner_operation(root, "index", {:create, record}) do
    id = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
    record = record |> Map.put("id", id) |> Map.put("revision", 0)
    # Index first: a crash before the first checkpoint is visible as unreadable,
    # rather than silently omitting an interrupted creation from enumeration.
    with :ok <- append_json(Path.join(root, "index.jsonl"), %{"id" => id}),
         :ok <- persist(root, id, record),
         do: {:ok, id}
  end

  defp owner_operation(root, id, {:checkpoint, record}) do
    with :ok <- validate(record),
         {:ok, current} <- get(root, id),
         :ok <- resume_matches(current, record),
         :ok <- revision_matches(current, record) do
      persist(root, id, Map.put(record, "revision", current["revision"] + 1))
    end
  end

  defp owner_operation(_root, _id, _operation),
    do: failure(:journal_owner, "Invalid transfer journal ownership operation")

  defp revision_matches(current, record) do
    if current["id"] == record["id"] and current["revision"] == record["revision"] and
         (is_nil(current["upload_id"]) or current["upload_id"] == record["upload_id"]),
       do: :ok,
       else: failure(:journal_revision, "Transfer journal revision or upload identity changed")
  end

  defp persist(root, id, record) do
    with {:ok, bytes} <- encode(record),
         :ok <- atomic_write(record_path(root, id), bytes),
         do:
           append_json(event_path(root, id), %{
             "id" => id,
             "revision" => record["revision"],
             "phase" => record["phase"],
             "checkpoint_sha256" => :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
           })
  end

  defp atomic_write(path, bytes) do
    temporary = path <> ".tmp." <> Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)

    with :ok <- safe_file(path),
         {:ok, file} <- File.open(temporary, [:write, :binary, :raw, :exclusive]) do
      try do
        written =
          try do
            with :ok <- File.chmod(temporary, 0o600),
                 :ok <- :file.write(file, bytes),
                 do: :file.sync(file)
          after
            File.close(file)
          end

        with :ok <- written, :ok <- safe_path(path), do: File.rename(temporary, path)
      after
        File.rm(temporary)
      end
    end
    |> io_result()
  end

  defp append_json(path, entry) do
    with :ok <- safe_file(path), {:ok, file} <- File.open(path, [:append, :binary, :raw]) do
      try do
        with :ok <- File.chmod(path, 0o600),
             :ok <- :file.write(file, JSON.encode!(entry) <> "\n"),
             do: :file.sync(file)
      after
        File.close(file)
      end
    end
    |> io_result()
  end

  defp read_json(path) do
    with :ok <- safe_path(path),
         {:ok, %{type: :regular, size: size}} when size <= @max_bytes <- File.lstat(path),
         {:ok, file} <- File.open(path, [:read, :binary, :raw]) do
      try do
        case :file.read(file, @max_bytes + 1) do
          {:ok, bytes} when byte_size(bytes) <= @max_bytes ->
            case JSON.decode(bytes) do
              {:ok, record} ->
                {:ok, record}

              _ ->
                failure(
                  :journal_malformed,
                  "Transfer journal is malformed; original file was preserved"
                )
            end

          _ ->
            failure(:journal_size, "Transfer journal exceeds its bounded format")
        end
      after
        File.close(file)
      end
    else
      {:error, %Error{}} = error -> error
      _ -> failure(:journal_read, "Transfer journal could not be read within its size limit")
    end
  end

  defp read_page(path, opts) do
    offset = opts[:cursor] || 0
    limit = opts[:limit] || 50

    if is_integer(offset) and offset >= 0 and is_integer(limit) and limit in 1..100 do
      with :ok <- safe_file(path), {:ok, file} <- File.open(path, [:read, :binary, :raw]) do
        try do
          with {:ok, _} <- :file.position(file, offset), do: read_entries(file, limit, [], offset)
        after
          File.close(file)
        end
      else
        {:error, :enoent} -> {:ok, %{entries: [], next: nil}}
        {:error, %Error{}} = error -> error
        _ -> failure(:journal_read, "Transfer journal inventory could not be read")
      end
    else
      failure(:journal_page, "Journal cursor and page size are invalid")
    end
  end

  defp read_entries(_file, 0, entries, offset),
    do: {:ok, %{entries: Enum.reverse(entries), next: offset}}

  defp read_entries(file, count, entries, offset) do
    # Chunk-bounded line reads avoid an unbounded malformed JSONL line.
    case read_line(file, [], 0) do
      :eof ->
        {:ok, %{entries: Enum.reverse(entries), next: nil}}

      {:ok, bytes} ->
        case JSON.decode(bytes) do
          {:ok, %{"id" => id} = entry} ->
            if id?(id),
              do: read_entries(file, count - 1, [entry | entries], offset + byte_size(bytes)),
              else:
                failure(:journal_index, "Transfer journal inventory contains an invalid identity")

          _ ->
            failure(:journal_index, "Transfer journal inventory is malformed")
        end

      _ ->
        failure(
          :journal_index,
          "Transfer journal inventory exceeds its line limit or is truncated"
        )
    end
  end

  defp read_line(_file, _chunks, count) when count >= @max_line, do: :error

  defp read_line(file, chunks, count) do
    case :file.read(file, 1) do
      {:ok, "\n"} -> {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary() |> Kernel.<>("\n")}
      {:ok, byte} -> read_line(file, [byte | chunks], count + 1)
      :eof when count == 0 -> :eof
      _ -> :error
    end
  end

  defp encode(record) do
    bytes = JSON.encode!(record)

    if byte_size(bytes) <= @max_bytes,
      do: {:ok, bytes},
      else: failure(:journal_size, "Transfer journal exceeds its bounded format")
  rescue
    _ -> failure(:journal_schema, "Transfer journal cannot be encoded safely")
  end

  defp prepare_root(root, create?) do
    root = Path.expand(root)

    with :ok <- safe_path(root),
         :ok <- maybe_create(root, create?),
         {:ok, %{type: :directory} = stat} <- File.lstat(root) do
      if Bitwise.band(stat.mode, 0o077) == 0,
        do: {:ok, root, {stat.major_device, stat.minor_device, stat.inode}},
        else: failure(:journal_root, "Transfer journal directory must have private permissions")
    else
      {:error, %Error{}} = error -> error
      _ -> failure(:journal_root, "Transfer journal directory is unavailable or unsafe")
    end
  end

  defp maybe_create(root, true) do
    with :ok <- File.mkdir_p(root), do: File.chmod(root, 0o700)
  end

  defp maybe_create(_root, false), do: :ok

  defp safe_path(path) do
    [root | segments] = path |> Path.expand() |> Path.split()

    Enum.reduce_while(segments, root, fn segment, parent ->
      current = Path.join(parent, segment)

      case File.lstat(current) do
        {:ok, %{type: :symlink}} ->
          {:halt, failure(:journal_path, "Transfer journal paths must not contain symlinks")}

        {:ok, _} ->
          {:cont, current}

        {:error, :enoent} ->
          {:cont, current}

        _ ->
          {:halt, failure(:journal_path, "Transfer journal path could not be inspected")}
      end
    end)
    |> case do
      {:error, _} = error -> error
      _ -> :ok
    end
  end

  defp safe_file(path) do
    with :ok <- safe_path(path) do
      case File.lstat(path) do
        {:ok, %{type: :regular}} -> :ok
        {:error, :enoent} -> :ok
        _ -> failure(:journal_path, "Transfer journal files must be regular files")
      end
    end
  end

  defp port(identity, id) do
    <<value::unsigned-32, _::binary>> =
      :crypto.hash(:sha256, :erlang.term_to_binary({identity, id}))

    49_152 + rem(value, 16_384)
  end

  defp valid_id(id),
    do: if(id?(id), do: :ok, else: failure(:journal_id, "Transfer journal identity is invalid"))

  defp record_path(root, id), do: Path.join(root, id <> ".json")
  defp event_path(root, id), do: Path.join(root, id <> ".events.jsonl")
  defp io_result(:ok), do: :ok
  defp io_result({:error, %Error{}} = error), do: error

  defp io_result(_),
    do: failure(:journal_write, "Transfer journal checkpoint could not be written")

  @doc "Serializes only the fixed, non-secret source descriptor fields."
  def source_record(%RemoteSource{} = source), do: RemoteSource.record(source)

  def source_record(%Source{} = source) do
    identity =
      Map.new(source.identity, fn {key, value} ->
        {Atom.to_string(key), if(is_atom(value), do: Atom.to_string(value), else: value)}
      end)

    %{
      "path" => source.path,
      "offset" => source.offset,
      "length" => source.length,
      "identity" => identity,
      "payload_hash" => source.payload_hash
    }
  end

  @doc "Rejects unknown fields at every level; credentials and arbitrary headers are not journal fields."
  def validate(record) do
    valid? =
      fields?(
        record,
        @root_keys,
        ~w(version endpoint credential_provider target source options parts phase)
      ) and
        record["version"] == 1 and valid_endpoint?(record["endpoint"]) and
        fields?(record["credential_provider"], ~w(kind name), ~w(kind name)) and
        record["credential_provider"]["kind"] in ~w(profile alias environment role web_identity sso anonymous explicit) and
        text?(record["credential_provider"]["name"], 1024) and valid_target?(record["target"]) and
        valid_source?(record["source"]) and valid_options?(record["options"]) and
        valid_parts?(record["parts"], record["source"]) and record["phase"] in @phases and
        optional?(record, "id", &id?/1) and optional?(record, "revision", &nonnegative?/1) and
        optional?(record, "upload_id", &(is_nil(&1) or text?(&1, 4096))) and
        optional?(record, "completion", &valid_completion?/1) and
        optional?(
          record,
          "cleanup",
          &(&1 in ~w(none abort_requested aborted abort_failed retained))
        )

    if valid?,
      do: :ok,
      else: failure(:journal_schema, "Transfer journal contains invalid or unsupported fields")
  end

  @doc "Compares immutable endpoint, provider reference, target, source, and transfer options."
  def resume_matches(record, expected) do
    with :ok <- validate(record) do
      if is_map(expected) and
           Map.take(record, @context_keys) == Map.take(expected, @context_keys),
         do: :ok,
         else:
           failure(:resume_conflict, "Transfer context changed since the journal was recorded")
    end
  end

  @doc "Reconciles a complete ListParts inventory; size alone never proves an unacknowledged part."
  def reconcile(record, remote_parts, opts \\ []) do
    with :ok <- validate(record),
         :ok <- complete_inventory(opts),
         {:ok, remote} <- remote_parts(remote_parts, record["parts"]),
         {:ok, parts} <- reconcile_parts(record["parts"], remote) do
      {:ok, Map.put(record, "parts", parts)}
    end
  end

  defp reconcile_parts(parts, remote) do
    Enum.reduce_while(parts, {:ok, []}, fn part, {:ok, reconciled} ->
      case Map.get(remote, part["number"]) do
        nil ->
          {:cont, {:ok, [pending(part) | reconciled]}}

        remote_part ->
          acknowledged = part["status"] == "completed" and part["etag"] == remote_part["etag"]

          checksum_matches =
            map_size(part["checksums"] || %{}) > 0 and
              Enum.all?(part["checksums"], fn {key, value} ->
                remote_part["checksums"][key] == value
              end)

          cond do
            part["status"] == "completed" and not acknowledged ->
              {:halt,
               failure(
                 :remote_parts_conflict,
                 "Remote multipart state differs from acknowledged progress"
               )}

            acknowledged or checksum_matches ->
              {:cont,
               {:ok,
                [
                  part |> Map.put("etag", remote_part["etag"]) |> Map.put("status", "completed")
                  | reconciled
                ]}}

            true ->
              {:cont, {:ok, [pending(part) | reconciled]}}
          end
      end
    end)
    |> case do
      {:ok, parts} -> {:ok, Enum.reverse(parts)}
      error -> error
    end
  end

  defp pending(part), do: part |> Map.put("status", "pending") |> Map.put("etag", nil)

  defp complete_inventory(opts) do
    if opts[:complete] == true,
      do: :ok,
      else:
        failure(
          :incomplete_parts_inventory,
          "Resume requires a complete multipart part inventory"
        )
  end

  defp remote_parts(parts, planned) when is_list(parts) and length(parts) <= 10_000 do
    ranges = Map.new(planned, &{&1["number"], &1})

    Enum.reduce_while(parts, {:ok, %{}}, fn part, {:ok, remote} ->
      valid =
        fields?(part, ~w(number size etag checksums), ~w(number size etag)) and
          is_integer(part["number"]) and text?(part["etag"], 1024) and
          valid_checksums?(Map.get(part, "checksums", %{}))

      number = if valid, do: part["number"]
      range = if valid, do: ranges[number]

      if valid and not is_nil(range) and part["size"] == range["length"] and
           not Map.has_key?(remote, number),
         do: {:cont, {:ok, Map.put(remote, number, Map.put_new(part, "checksums", %{}))}},
         else:
           {:halt,
            failure(
              :remote_parts_conflict,
              "Remote multipart inventory is invalid or conflicts with planned ranges"
            )}
    end)
  end

  defp remote_parts(_, _),
    do: failure(:remote_parts_conflict, "Remote multipart inventory exceeds its bounded format")

  defp valid_endpoint?(endpoint) do
    if fields?(endpoint, ~w(url region addressing service), ~w(url region addressing service)) and
         text?(endpoint["url"], 4096) and text?(endpoint["region"], 256) and
         text?(endpoint["service"], 256) and endpoint["addressing"] in ~w(path virtual) do
      uri = URI.parse(endpoint["url"])

      uri.scheme in ~w(http https) and is_binary(uri.host) and uri.host != "" and
        is_nil(uri.userinfo) and is_nil(uri.query) and is_nil(uri.fragment)
    else
      false
    end
  end

  defp valid_target?(target),
    do:
      fields?(target, ~w(bucket key), ~w(bucket key)) and text?(target["bucket"], 1024) and
        text?(target["key"], 4096)

  defp valid_source?(%{"kind" => "remote"} = source), do: RemoteSource.valid_record?(source)

  defp valid_source?(source) do
    fields?(
      source,
      ~w(path offset length identity payload_hash),
      ~w(path offset length identity payload_hash)
    ) and
      text?(source["path"], 4096) and Path.type(source["path"]) == :absolute and
      nonnegative?(source["offset"]) and nonnegative?(source["length"]) and
      is_binary(source["payload_hash"]) and
      Regex.match?(~r/\A[0-9a-f]{64}\z/, source["payload_hash"]) and
      fields?(
        source["identity"],
        ~w(major_device minor_device inode size mtime ctime type),
        ~w(major_device minor_device inode size mtime ctime type)
      ) and
      source["identity"]["type"] == "regular" and
      Enum.all?(
        ~w(major_device minor_device inode size mtime ctime),
        &is_integer(source["identity"][&1])
      ) and
      source["offset"] + source["length"] <= source["identity"]["size"]
  end

  defp valid_options?(options) do
    fields?(
      options,
      ~w(part_size checksum_algorithm checksum_type keep_incomplete storage_class content_type input_digest copy),
      ~w(part_size)
    ) and
      is_integer(options["part_size"]) and options["part_size"] > 0 and
      optional?(options, "checksum_algorithm", &(&1 in @checksums)) and
      optional?(options, "checksum_type", &(&1 in ~w(FULL_OBJECT COMPOSITE))) and
      optional?(options, "keep_incomplete", &is_boolean/1) and
      optional?(options, "copy", &is_boolean/1) and
      optional?(options, "storage_class", &text?(&1, 256)) and
      optional?(options, "content_type", &text?(&1, 1024)) and
      optional?(
        options,
        "input_digest",
        &(is_binary(&1) and Regex.match?(~r/\A[0-9a-f]{64}\z/, &1))
      )
  end

  defp valid_parts?(parts, source) when is_list(parts) and length(parts) in 1..10_000 do
    Enum.reduce_while(parts, {source["offset"], 1}, fn part, {offset, number} ->
      valid =
        fields?(
          part,
          ~w(number offset length etag checksums status),
          ~w(number offset length status)
        ) and
          part["number"] == number and part["offset"] == offset and is_integer(part["length"]) and
          part["length"] > 0 and
          part["status"] in ~w(pending completed) and
          optional?(part, "checksums", &valid_checksums?/1) and
          optional?(part, "etag", &(is_nil(&1) or text?(&1, 1024))) and
          (part["status"] != "completed" or text?(part["etag"], 1024))

      if valid, do: {:cont, {offset + part["length"], number + 1}}, else: {:halt, false}
    end)
    |> case do
      {offset, _} -> offset == source["offset"] + source["length"]
      false -> false
    end
  end

  defp valid_parts?(_, _), do: false

  defp valid_checksums?(checksums),
    do:
      fields?(checksums, @checksums, []) and
        Enum.all?(checksums, fn {_key, value} -> text?(value, 256) end)

  defp valid_completion?(nil), do: true

  defp valid_completion?(value),
    do:
      fields?(value, ~w(etag version_id checksum), ~w(etag)) and
        text?(value["etag"], 1024) and optional?(value, "version_id", &text?(&1, 4096)) and
        optional?(value, "checksum", &valid_checksums?/1)

  defp fields?(map, allowed, required),
    do:
      is_map(map) and not is_struct(map) and
        Enum.all?(Map.keys(map), &(&1 in allowed)) and Enum.all?(required, &Map.has_key?(map, &1))

  defp optional?(map, key, validator), do: not Map.has_key?(map, key) or validator.(map[key])

  defp text?(value, max),
    do: is_binary(value) and byte_size(value) in 1..max and String.valid?(value)

  defp nonnegative?(value), do: is_integer(value) and value >= 0
  defp id?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{32}\z/, value)
  defp failure(code, message), do: {:error, Error.new(:client, message, code: code)}
end
