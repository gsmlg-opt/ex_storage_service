defmodule ExStorageService.Storage.StagingRecovery do
  @moduledoc false

  # Anonymous names are issued only before any metadata submission is possible.
  # Protected names are deliberately retained even when an exact read is absent:
  # an earlier metadata submission may still linearize after that read.
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.Backend.Concord

  @chunk 262_144
  @turn_bytes 4 * 1024 * 1024
  @anonymous ~r/\Aess1-a-([a-f0-9]{64})-([1-9][0-9]{0,19})-([A-Za-z0-9_-]{32})\z/
  @protected ~r/\Aess1-[hg]-/
  @helper ~S"""
  printf '%s\n' "${1##*/}"
  while IFS= read -r -t 15 reply; do
    case "$reply" in
      next) exit 0 ;;
      pulse) ;;
      *) exit 1 ;;
    esac
  done
  exit 1
  """

  def staging_name(opts) do
    nonce = Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)

    case owner_scope(opts) do
      {:ok, scope} -> "ess1-a-#{scope}-#{System.pid()}-#{nonce}"
      {:error, _} -> "upload-#{nonce}"
    end
  end

  def protected?(path), do: Regex.match?(@protected, Path.basename(path))

  def protect(staged, kind, opts) do
    with true <- Keyword.has_key?(opts, :managed_staging),
         [_, scope, pid, nonce] <- Regex.run(@anonymous, Path.basename(staged.path)),
         {:ok, ^scope} <- owner_scope(opts),
         true <- pid == System.pid(),
         true <- Path.dirname(staged.path) == staging_root(opts) do
      fs = Keyword.get(opts, :fs_module, ExStorageService.BlobStore.LocalCAS.FileSystem)

      destination =
        Path.join(Path.dirname(staged.path), protected_name(kind, scope, pid, nonce, staged.hash))

      protect_path(staged, destination, fs, opts)
    else
      # Unmanaged callers keep the existing BlobStore contract. A managed name
      # with uncertain ownership is never submitted through a different scope.
      false -> unmanaged_or_error(staged, opts)
      nil -> {:ok, staged}
      {:error, _} -> {:error, :staging_owner_unavailable}
      _ -> {:error, :staging_owner_mismatch}
    end
  end

  defp unmanaged_or_error(staged, opts) do
    if Keyword.has_key?(opts, :managed_staging),
      do: {:error, :staging_owner_mismatch},
      else: {:ok, staged}
  end

  defp protected_name({:hosted, _id}, scope, pid, nonce, hash),
    do: "ess1-h-#{scope}-#{pid}-#{nonce}-#{hash}"

  defp protected_name({:gateway, id}, scope, pid, nonce, hash) do
    prefix = "ess1-g-#{scope}-#{pid}-#{nonce}-#{hash}-"
    encoded = Base.url_encode64(id, padding: false)
    suffix = if byte_size(prefix) + byte_size(encoded) <= 254, do: "i" <> encoded, else: "opaque"
    prefix <> suffix
  end

  defp protect_path(staged, destination, fs, opts) do
    # Flush completed bytes before changing the staging directory's namespace.
    # Reserve exclusively. Only a reservation successfully created by this call
    # can be replaced; failed creation never removes another owner's file.
    with :ok <- sync_data(staged.path, fs, opts),
         {:ok, reservation} <- fs.open(destination, [:write, :raw, :binary, :exclusive]) do
      with :ok <- fs.close(reservation),
           :ok <- protection_fault(opts, :protection_rename),
           :ok <- fs.rename(staged.path, destination),
           :ok <- protection_fault(opts, :protection_directory_sync),
           :ok <- sync_directory(fs, Path.dirname(destination)) do
        {:ok, %{staged | path: destination}}
      else
        {:error, reason} -> {:error, {:staging_protection, reason}}
      end
    else
      {:error, reason} -> {:error, {:staging_protection, reason}}
    end
  end

  defp sync_data(path, fs, opts) do
    with {:ok, io} <- fs.open(path, [:read, :raw, :binary]) do
      result =
        with :ok <- protection_fault(opts, :protection_data_sync),
             do: fs.sync(io)

      closed = fs.close(io)
      if result == :ok, do: closed, else: result
    end
  end

  defp protection_fault(opts, boundary) do
    case Keyword.get(opts, :faults, %{}) |> Map.get(boundary) do
      nil -> :ok
      fun when is_function(fun, 0) -> fun.()
      reason -> {:error, reason}
    end
  end

  defp sync_directory(fs, root) do
    with {:ok, io} <- fs.open_directory(root) do
      result = fs.sync(io)
      closed = fs.close(io)
      if result == :ok, do: closed, else: result
    end
  end

  def new(opts) do
    with {:ok, scope} <- owner_scope(opts),
         {:ok, %{type: :directory} = root_stat} <- File.lstat(staging_root(opts), time: :posix),
         find when is_binary(find) <- System.find_executable("find"),
         bash when is_binary(bash) <- System.find_executable("bash") do
      port =
        Port.open({:spawn_executable, find}, [
          :binary,
          :exit_status,
          :use_stdio,
          {:line, 512},
          args: [
            staging_root(opts),
            "-mindepth",
            "1",
            "-maxdepth",
            "1",
            "-name",
            "ess1-*",
            "(",
            "-exec",
            bash,
            "-c",
            @helper,
            "ess-staging-cursor",
            "{}",
            ";",
            "-o",
            "-quit",
            ")"
          ]
        ])

      {:ok,
       %{
         opts: opts,
         scope: scope,
         root_identity: identity(root_stat),
         port: port,
         candidate: nil,
         deleted: 0,
         retained: 0,
         decisions: 0,
         last_turn_bytes: 0
       }}
    else
      {:error, :owner_scope_unavailable} -> {:disabled, :owner_scope_unavailable}
      _ -> {:disabled, :staging_cursor_unavailable}
    end
  end

  def message(%{port: port} = state, {port, {:data, {:eol, name}}}) do
    if state.candidate == nil and byte_size(name) <= 255 do
      {:ok, candidate(state, name)}
    else
      {:error, :invalid_cursor_protocol, close(state)}
    end
  end

  def message(%{port: port} = state, {port, {:exit_status, 0}}),
    do: {:done, close(state)}

  def message(%{port: port} = state, {port, _}),
    do: {:error, :staging_cursor_failed, close(state)}

  def message(state, _), do: {:ignore, state}

  def step(%{candidate: nil} = state), do: state

  def step(state) do
    command(state.port, "pulse\n")
    hash_turn(%{state | last_turn_bytes: 0}, @turn_bytes)
  end

  def close(state) do
    close_candidate(state.candidate)

    if is_port(state.port) and Port.info(state.port) != nil do
      command(state.port, "stop\n")
      port = state.port

      receive do
        {^port, {:exit_status, _}} -> :ok
      after
        1000 -> if Port.info(port) != nil, do: Port.close(port)
      end
    end

    %{state | port: nil, candidate: nil}
  end

  defp candidate(state, name) do
    with [_, scope, pid, _] <- Regex.run(@anonymous, name),
         true <- scope == state.scope,
         true <- owner_absent?(pid, state.opts),
         true <- root_unchanged?(state),
         path = Path.join(staging_root(state.opts), name),
         {:ok, %{type: :regular} = stat} <- File.lstat(path, time: :posix),
         {:ok, io} <- File.open(path, [:read, :raw, :binary]) do
      %{
        state
        | candidate: %{
            path: path,
            pid: pid,
            io: io,
            identity: identity(stat),
            hash: :crypto.hash_init(:sha256)
          }
      }
    else
      _ -> decided(state, :retained)
    end
  end

  defp hash_turn(state, 0), do: state

  defp hash_turn(%{candidate: candidate} = state, remaining) do
    case :file.read(candidate.io, min(@chunk, remaining)) do
      {:ok, bytes} ->
        next = %{candidate | hash: :crypto.hash_update(candidate.hash, bytes)}

        hash_turn(
          %{state | candidate: next, last_turn_bytes: state.last_turn_bytes + byte_size(bytes)},
          remaining - byte_size(bytes)
        )

      :eof ->
        hash = candidate.hash |> :crypto.hash_final() |> Base.encode16(case: :lower)
        backend = Keyword.get(state.opts, :backend, Concord)
        result = backend.get(Keys.gc_guard(hash), consistency: :linearizable)

        outcome =
          if result == {:ok, nil} and safe_to_unlink?(state) and File.rm(candidate.path) == :ok,
            do: :deleted,
            else: :retained

        close_candidate(candidate)
        decided(%{state | candidate: nil}, outcome)

      {:error, _} ->
        close_candidate(candidate)
        decided(%{state | candidate: nil}, :retained)
    end
  end

  defp safe_to_unlink?(state) do
    with {:ok, scope} <- owner_scope(state.opts),
         true <- scope == state.scope,
         true <- root_unchanged?(state),
         true <- owner_absent?(state.candidate.pid, state.opts),
         {:ok, %{type: :regular} = stat} <- File.lstat(state.candidate.path, time: :posix) do
      identity(stat) == state.candidate.identity
    else
      _ -> false
    end
  end

  defp decided(state, outcome) do
    command(state.port, "next\n")
    state |> Map.update!(outcome, &(&1 + 1)) |> Map.update!(:decisions, &(&1 + 1))
  end

  defp close_candidate(nil), do: :ok
  defp close_candidate(candidate), do: File.close(candidate.io)

  defp command(port, bytes) do
    if is_port(port) and Port.info(port) != nil, do: Port.command(port, bytes)
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp root_unchanged?(state) do
    case File.lstat(staging_root(state.opts), time: :posix) do
      {:ok, %{type: :directory} = stat} ->
        root_identity(stat) == root_identity(state.root_identity)

      _ ->
        false
    end
  end

  defp root_identity(stat), do: Map.take(stat, [:inode, :major_device, :minor_device, :type])

  defp identity(stat),
    do: Map.take(stat, [:inode, :major_device, :minor_device, :type, :size, :mtime, :ctime])

  defp owner_absent?(pid, opts),
    do: File.lstat(Path.join(proc_root(opts), pid)) == {:error, :enoent}

  defp proc_root(opts), do: Keyword.get(opts, :proc_root, "/proc")
  defp staging_root(opts), do: opts |> Keyword.fetch!(:tmp_dir) |> Path.expand()

  defp owner_scope(opts) do
    with managed when is_list(managed) <- Keyword.get(opts, :managed_staging),
         {:ok, boot} <- File.read(Path.join(proc_root(opts), "sys/kernel/random/boot_id")),
         {:ok, namespace} <- File.stat(Path.join(proc_root(opts), "self/ns/pid")),
         {:ok, status} <- File.read(Path.join(proc_root(opts), "self/status")),
         [_, pid] <- Regex.run(~r/^Pid:\s+(\d+)$/m, status),
         true <- pid == System.pid(),
         [_, uid] <- Regex.run(~r/^Uid:\s+\d+\s+(\d+)\s+/m, status) do
      scope =
        {String.trim(boot), namespace.inode, namespace.major_device, namespace.minor_device, uid,
         node(), managed[:instance], managed[:node_id], staging_root(opts)}

      {:ok,
       scope
       |> :erlang.term_to_binary()
       |> then(&:crypto.hash(:sha256, &1))
       |> Base.encode16(case: :lower)}
    else
      _ -> {:error, :owner_scope_unavailable}
    end
  end
end
