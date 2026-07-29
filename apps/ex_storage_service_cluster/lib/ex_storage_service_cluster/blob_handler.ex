defmodule ExStorageServiceCluster.BlobHandler do
  @moduledoc false

  import Plug.Conn

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.Cluster.ReplicaAck
  alias ExStorageServiceCluster.InternalAuth

  @read_length 262_144
  @telemetry_prefix [:ex_storage_service, :cluster, :blob_transport]
  @health_hash String.duplicate("0", 64)

  def put(conn, hash, opts) do
    started_at = System.monotonic_time()

    conn =
      with :ok <- LocalCAS.validate_hash(hash),
           {:ok, size} <- declared_size(conn, Keyword.fetch!(opts, :max_blob_size)),
           {:ok, claims} <- authenticate(conn, hash, size, opts) do
        receive_blob(conn, hash, size, claims, opts)
      else
        {:error, :invalid_hash} -> error(conn, 400, "invalid blob hash")
        {:error, :missing_length} -> error(conn, 411, "content length required")
        {:error, :invalid_length} -> error(conn, 400, "invalid content length")
        {:error, :entity_too_large} -> error(conn, 413, "blob too large")
        {:error, _auth_reason} -> error(conn, 401, "unauthorized")
      end

    emit_stop(:put_blob, started_at, conn, Map.get(conn.private, :ess_blob_bytes, 0), hash)
  end

  def head(conn, hash, opts) do
    started_at = System.monotonic_time()

    conn =
      with :ok <- LocalCAS.validate_hash(hash),
           {:ok, claims} <- authenticate(conn, hash, "-", opts) do
        serve_head(conn, hash, claims.request_id, opts)
      else
        {:error, :invalid_hash} -> error(conn, 400, "invalid blob hash")
        {:error, _auth_reason} -> error(conn, 401, "unauthorized")
      end

    emit_stop(:head_blob, started_at, conn, 0, hash)
  end

  def get(conn, hash, opts) do
    started_at = System.monotonic_time()

    conn =
      with :ok <- LocalCAS.validate_hash(hash),
           {:ok, claims} <- authenticate(conn, hash, "-", opts) do
        serve_get(conn, hash, claims.request_id, opts)
      else
        {:error, :invalid_hash} -> error(conn, 400, "invalid blob hash")
        {:error, _auth_reason} -> error(conn, 401, "unauthorized")
      end

    emit_stop(:open_blob, started_at, conn, response_bytes(conn), hash)
  end

  def delete(conn, hash, opts) do
    started_at = System.monotonic_time()

    conn =
      with :ok <- LocalCAS.validate_hash(hash),
           {:ok, claims} <- authenticate(conn, hash, "-", opts),
           {:ok, fence} <- cleanup_fence(conn),
           :ok <- authorize_cleanup(hash, fence, opts) do
        case blob_store(opts).delete(hash, blob_store_opts(opts)) do
          :ok ->
            conn
            |> ack_request(claims.request_id)
            |> send_resp(204, "")

          {:error, _reason} ->
            error(conn, 500, "blob delete failed", claims.request_id)
        end
      else
        {:error, :invalid_hash} ->
          error(conn, 400, "invalid blob hash")

        {:error, {:cleanup, :invalid_cleanup_fence}} ->
          error(conn, 400, "invalid cleanup fence")

        {:error, {:cleanup, reason}}
        when reason in [:timeout, :unknown, :cluster_not_ready, :no_leader] ->
          error(conn, 503, "cleanup authorization unavailable")

        {:error, {:cleanup, _reason}} ->
          error(conn, 409, "stale cleanup fence")

        {:error, _auth_reason} ->
          error(conn, 401, "unauthorized")
      end

    emit_stop(:delete_blob, started_at, conn, 0, hash)
  end

  def health(conn, opts) do
    with {:ok, claims} <- authenticate(conn, @health_hash, "-", opts) do
      case storage_ready(opts, claims.request_id) do
        :ok ->
          conn
          |> ack_request(claims.request_id)
          |> put_resp_header("x-ess-node-id", to_string(Keyword.fetch!(opts, :node_id)))
          |> put_resp_header(
            "x-ess-node-generation",
            Integer.to_string(Keyword.get(opts, :node_generation, 1))
          )
          |> send_resp(200, "")

        {:error, _reason} ->
          error(conn, 503, "storage unavailable", claims.request_id)
      end
    else
      {:error, _auth_reason} -> error(conn, 401, "unauthorized")
    end
  end

  @doc false
  def parse_range(nil, _total_size), do: {:ok, nil}

  def parse_range("bytes=" <> specification, total_size)
      when is_integer(total_size) and total_size >= 0 do
    if String.contains?(specification, ",") do
      {:error, :invalid_range}
    else
      parse_single_range(String.split(specification, "-", parts: 2), total_size)
    end
  end

  def parse_range(_range, _total_size), do: {:error, :invalid_range}

  defp serve_head(conn, hash, request_id, opts) do
    with :ok <- blob_store(opts).verify(hash, blob_store_opts(opts)),
         {:ok, %{size: size}} <- blob_store(opts).stat(hash, blob_store_opts(opts)) do
      ack = %ReplicaAck{
        node_id: Keyword.fetch!(opts, :node_id),
        node_generation: Keyword.get(opts, :node_generation, 1),
        hash: hash,
        size: size,
        verified_at: System.system_time(:second),
        fencing_or_request_id: request_id
      }

      conn
      |> put_ack_headers(ack)
      |> put_resp_header("accept-ranges", "bytes")
      |> put_resp_header("content-length", Integer.to_string(size))
      |> send_resp(200, "")
    else
      {:error, :not_found} -> error(conn, 404, "blob not found", request_id)
      {:error, :checksum_mismatch} -> error(conn, 422, "blob checksum mismatch", request_id)
      {:error, _reason} -> error(conn, 500, "blob lookup failed", request_id)
    end
  end

  defp serve_get(conn, hash, request_id, opts) do
    case blob_store(opts).stat(hash, blob_store_opts(opts)) do
      {:ok, %{size: total_size}} ->
        open_and_send(conn, hash, total_size, request_id, opts)

      {:error, :not_found} ->
        error(conn, 404, "blob not found", request_id)

      {:error, _reason} ->
        error(conn, 500, "blob lookup failed", request_id)
    end
  end

  defp open_and_send(conn, hash, total_size, request_id, opts) do
    with {:ok, range} <- request_range(conn, total_size),
         {:ok, source} <- blob_store(opts).open(hash, range_tuple(range), blob_store_opts(opts)) do
      send_source(conn, source, hash, total_size, range, request_id)
    else
      {:error, :not_found} -> error(conn, 404, "blob not found", request_id)
      {:error, :invalid_range} -> range_error(conn, total_size, request_id)
      {:error, _reason} -> error(conn, 500, "blob open failed", request_id)
    end
  end

  defp receive_blob(conn, hash, size, claims, opts) do
    reader = fn current_conn ->
      Plug.Conn.read_body(current_conn,
        length: @read_length,
        read_length: @read_length,
        read_timeout: Keyword.get(opts, :read_timeout, 60_000)
      )
    end

    stage_opts = Keyword.put(blob_store_opts(opts), :max_size, size)

    case blob_store(opts).stage_from_reader(reader, conn, stage_opts) do
      {:ok, staged, final_conn} ->
        final_conn
        |> put_private(:ess_blob_bytes, staged.size)
        |> validate_and_commit(staged, hash, size, claims, opts)

      {:error, :entity_too_large, final_conn} ->
        error(final_conn, 413, "blob too large")

      {:error, _reason, final_conn} ->
        error(final_conn, 400, "incomplete blob body")

      {:error, _reason} ->
        error(conn, 400, "incomplete blob body")
    end
  end

  defp validate_and_commit(conn, staged, hash, size, claims, opts) do
    cond do
      staged.hash != hash ->
        _ = blob_store(opts).discard(staged, blob_store_opts(opts))
        emit_checksum_failure(hash, staged.size, claims.request_id, conn)
        error(conn, 422, "checksum mismatch", claims.request_id)

      staged.size != size ->
        _ = blob_store(opts).discard(staged, blob_store_opts(opts))
        error(conn, 422, "size mismatch", claims.request_id)

      true ->
        commit_blob(conn, staged, claims, opts)
    end
  end

  defp commit_blob(conn, staged, claims, opts) do
    case blob_store(opts).commit(staged, blob_store_opts(opts)) do
      {:ok, ready} ->
        ack = %ReplicaAck{
          node_id: Keyword.fetch!(opts, :node_id),
          node_generation: Keyword.get(opts, :node_generation, 1),
          hash: ready.hash,
          size: ready.size,
          verified_at: System.system_time(:second),
          fencing_or_request_id: claims.request_id
        }

        conn
        |> put_ack_headers(ack)
        |> send_resp(200, "")

      {:error, reason} ->
        _ = blob_store(opts).discard(staged, blob_store_opts(opts))
        commit_error(conn, reason, claims.request_id)
    end
  end

  defp commit_error(conn, {:commit, :existing_blob_mismatch}, request_id),
    do: error(conn, 409, "blob conflict", request_id)

  defp commit_error(conn, :checksum_mismatch, request_id),
    do: error(conn, 409, "blob conflict", request_id)

  defp commit_error(conn, _reason, request_id),
    do: error(conn, 500, "blob commit failed", request_id)

  defp send_source(conn, {:file, path, offset, length}, hash, total_size, range, request_id) do
    conn =
      conn
      |> ack_request(request_id)
      |> put_resp_header("accept-ranges", "bytes")
      |> put_resp_header("content-type", "application/octet-stream")
      |> put_resp_header("content-length", Integer.to_string(length))
      |> put_resp_header("x-ess-blob-sha256", hash)
      |> maybe_put_content_range(range, total_size)

    status = if range, do: 206, else: 200

    if length == 0,
      do: send_resp(conn, status, ""),
      else: send_file(conn, status, path, offset, length)
  end

  defp send_source(conn, _source, _hash, _total_size, _range, request_id),
    do: error(conn, 500, "unsupported blob source", request_id)

  defp declared_size(conn, maximum) do
    case get_req_header(conn, "content-length") do
      [value] ->
        case Integer.parse(value) do
          {size, ""} when size >= 0 and size <= maximum -> {:ok, size}
          {size, ""} when size > maximum -> {:error, :entity_too_large}
          _ -> {:error, :invalid_length}
        end

      [] ->
        {:error, :missing_length}

      _duplicate ->
        {:error, :invalid_length}
    end
  end

  defp authenticate(conn, hash, size, opts) do
    InternalAuth.verify(conn, conn.method, hash, size,
      secret: Keyword.fetch!(opts, :secret),
      replay_table: Keyword.fetch!(opts, :replay_table),
      skew_seconds: Keyword.get(opts, :auth_skew_seconds, 60)
    )
  end

  defp request_range(conn, total_size) do
    case get_req_header(conn, "range") do
      [] -> parse_range(nil, total_size)
      [range] -> parse_range(range, total_size)
      _duplicate -> {:error, :invalid_range}
    end
  end

  defp parse_single_range([start_text, end_text], total_size)
       when start_text != "" and end_text != "" do
    with {first, ""} <- Integer.parse(start_text),
         {last, ""} <- Integer.parse(end_text),
         true <- first >= 0 and last >= first and first < total_size do
      last = min(last, total_size - 1)
      {:ok, %{offset: first, length: last - first + 1}}
    else
      _ -> {:error, :invalid_range}
    end
  end

  defp parse_single_range([start_text, ""], total_size) when start_text != "" do
    with {first, ""} <- Integer.parse(start_text),
         true <- first >= 0 and first < total_size do
      {:ok, %{offset: first, length: total_size - first}}
    else
      _ -> {:error, :invalid_range}
    end
  end

  defp parse_single_range(["", suffix_text], total_size) when suffix_text != "" do
    with {suffix, ""} <- Integer.parse(suffix_text),
         true <- suffix > 0 and total_size > 0 do
      length = min(suffix, total_size)
      {:ok, %{offset: total_size - length, length: length}}
    else
      _ -> {:error, :invalid_range}
    end
  end

  defp parse_single_range(_parts, _total_size), do: {:error, :invalid_range}

  defp range_tuple(nil), do: nil
  defp range_tuple(%{offset: offset, length: length}), do: {offset, length}

  defp maybe_put_content_range(conn, nil, _total_size), do: conn

  defp maybe_put_content_range(conn, range, total_size) do
    last = range.offset + range.length - 1
    put_resp_header(conn, "content-range", "bytes #{range.offset}-#{last}/#{total_size}")
  end

  defp range_error(conn, total_size, request_id) do
    conn
    |> put_resp_header("content-range", "bytes */#{total_size}")
    |> error(416, "invalid range", request_id)
  end

  defp put_ack_headers(conn, ack) do
    conn
    |> ack_request(ack.fencing_or_request_id)
    |> put_resp_header("x-ess-node-id", to_string(ack.node_id))
    |> put_resp_header("x-ess-node-generation", to_string(ack.node_generation))
    |> put_resp_header("x-ess-blob-sha256", ack.hash)
    |> put_resp_header("x-ess-blob-size", Integer.to_string(ack.size))
    |> put_resp_header("x-ess-verified-at", Integer.to_string(ack.verified_at))
  end

  defp ack_request(conn, request_id), do: put_resp_header(conn, "x-ess-request-id", request_id)

  defp error(conn, status, message, request_id \\ nil) do
    conn = if request_id, do: ack_request(conn, request_id), else: conn
    send_resp(conn, status, message)
  end

  defp cleanup_fence(conn) do
    with {:ok, job_id} <- request_header(conn, "x-ess-cleanup-job-id"),
         {:ok, target_generation} <-
           request_integer_header(conn, "x-ess-cleanup-target-generation"),
         {:ok, owner_node} <- request_header(conn, "x-ess-cleanup-owner-node"),
         {:ok, owner_generation} <-
           request_integer_header(conn, "x-ess-cleanup-owner-generation"),
         {:ok, fencing_token} <- request_integer_header(conn, "x-ess-cleanup-fencing-token") do
      {:ok,
       %{
         job_id: job_id,
         target_generation: target_generation,
         owner_node: owner_node,
         owner_generation: owner_generation,
         fencing_token: fencing_token
       }}
    else
      _other -> {:error, {:cleanup, :invalid_cleanup_fence}}
    end
  end

  defp authorize_cleanup(hash, fence, opts) do
    authorizer =
      Keyword.get(
        opts,
        :cleanup_authorizer,
        &ExStorageService.Metadata.BlobLocations.authorize_cleanup/8
      )

    with true <- fence.target_generation == Keyword.get(opts, :node_generation, 1),
         :ok <-
           authorizer.(
             hash,
             to_string(Keyword.fetch!(opts, :node_id)),
             fence.target_generation,
             fence.job_id,
             fence.owner_node,
             fence.owner_generation,
             fence.fencing_token,
             now_ms: System.system_time(:millisecond)
           ) do
      :ok
    else
      false -> {:error, {:cleanup, :stale_target_generation}}
      {:error, reason} -> {:error, {:cleanup, reason}}
    end
  end

  defp request_header(conn, name) do
    case get_req_header(conn, name) do
      [value] when value != "" -> {:ok, value}
      _other -> {:error, :invalid_cleanup_fence}
    end
  end

  defp request_integer_header(conn, name) do
    with {:ok, value} <- request_header(conn, name),
         {integer, ""} when integer >= 0 <- Integer.parse(value) do
      {:ok, integer}
    else
      _other -> {:error, :invalid_cleanup_fence}
    end
  end

  defp blob_store(opts), do: Keyword.get(opts, :blob_store, LocalCAS)
  defp blob_store_opts(opts), do: Keyword.get(opts, :blob_store_opts, [])

  defp storage_ready(opts, request_id) do
    blob_opts = blob_store_opts(opts)
    root = Keyword.get(blob_opts, :root)
    tmp_dir = Keyword.get(blob_opts, :tmp_dir)

    with true <- is_binary(root) and File.dir?(root),
         true <- is_binary(tmp_dir),
         :ok <- File.mkdir_p(tmp_dir),
         :ok <- writable_probe(tmp_dir, request_id) do
      :ok
    else
      false -> {:error, :invalid_storage_root}
      {:error, _reason} = error -> error
    end
  end

  defp writable_probe(tmp_dir, request_id) do
    path = Path.join(tmp_dir, ".health-#{request_id}")

    try do
      case File.open(path, [:write, :exclusive, :binary]) do
        {:ok, io} ->
          result =
            with :ok <- IO.binwrite(io, :binary.copy(<<0>>, 4_096)),
                 :ok <- :file.sync(io) do
              :ok
            end

          _ = File.close(io)
          _ = File.rm(path)
          result

        {:error, reason} ->
          {:error, reason}
      end
    rescue
      _error ->
        _ = File.rm(path)
        {:error, :storage_probe_failed}
    end
  end

  defp response_bytes(%Plug.Conn{status: status} = conn) when status in [200, 206] do
    case get_resp_header(conn, "content-length") do
      [value] -> String.to_integer(value)
      _ -> 0
    end
  end

  defp response_bytes(_conn), do: 0

  defp emit_stop(operation, started_at, conn, bytes, hash) do
    :telemetry.execute(
      @telemetry_prefix ++ [:stop],
      %{duration: System.monotonic_time() - started_at, bytes: bytes},
      %{
        direction: :server,
        operation: operation,
        peer: format_peer(conn.remote_ip),
        hash: hash,
        status: conn.status
      }
    )

    conn
  end

  defp emit_checksum_failure(hash, bytes, request_id, conn) do
    :telemetry.execute(
      @telemetry_prefix ++ [:checksum_failure],
      %{count: 1, bytes: bytes},
      %{direction: :server, peer: format_peer(conn.remote_ip), hash: hash, request_id: request_id}
    )
  end

  defp format_peer(nil), do: "unknown"
  defp format_peer(ip) when is_tuple(ip), do: ip |> :inet.ntoa() |> to_string()
end
