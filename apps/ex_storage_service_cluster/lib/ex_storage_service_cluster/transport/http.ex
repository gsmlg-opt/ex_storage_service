defmodule ExStorageServiceCluster.Transport.HTTP do
  @moduledoc """
  Authenticated HTTP implementation of the cluster blob transport.

  Upload bodies are lazy Enumerables and downloads use Req's callback-based
  HTTP/1 streaming path. Retries and redirects are disabled so a signed
  request ID always describes exactly one transfer attempt.
  """

  @behaviour ExStorageService.Cluster.Transport

  alias ExStorageService.BlobStore.{Source, StagedBlob}
  alias ExStorageService.Cluster.{BlobDescriptor, ReplicaAck}
  alias ExStorageServiceCluster.InternalAuth

  @telemetry_prefix [:ex_storage_service, :cluster, :blob_transport]
  @health_hash String.duplicate("0", 64)

  @impl true
  def put_blob(context, node, source, %BlobDescriptor{} = descriptor, opts \\ []) do
    started_at = System.monotonic_time()
    path = blob_path(descriptor.hash)
    request_id = Keyword.get_lazy(opts, :request_id, &request_id/0)

    with {:ok, body} <- source_enumerable(source),
         {:ok, headers} <-
           signed_headers(:put, descriptor.hash, descriptor.size, context, opts,
             path: path,
             request_id: request_id
           ),
         {:ok, response} <-
           Req.request(
             request_options(node, path, opts) ++
               [
                 method: :put,
                 headers: [{"content-length", descriptor.size} | headers],
                 body: body
               ]
           ),
         {:ok, ack} <- decode_ack(response, descriptor, request_id) do
      emit_stop(:put_blob, started_at, descriptor.size, node, descriptor.hash, :ok)
      {:ok, ack}
    else
      {:error, reason} = error ->
        emit_stop(:put_blob, started_at, 0, node, descriptor.hash, reason)
        error
    end
  end

  @impl true
  def head_blob(context, node, hash, opts \\ []) do
    started_at = System.monotonic_time()
    path = blob_path(hash)
    request_id = Keyword.get_lazy(opts, :request_id, &request_id/0)

    with {:ok, headers} <-
           signed_headers(:head, hash, "-", context, opts,
             path: path,
             request_id: request_id
           ),
         {:ok, response} <-
           Req.request(request_options(node, path, opts) ++ [method: :head, headers: headers]),
         {:ok, info} <- decode_head(response, hash, request_id) do
      emit_stop(:head_blob, started_at, 0, node, hash, :ok)
      {:ok, info}
    else
      {:error, reason} = error ->
        emit_stop(:head_blob, started_at, 0, node, hash, reason)
        error
    end
  end

  @impl true
  def open_blob(context, node, hash, range, opts \\ []) do
    with {:ok, info} <- verified_head(context, node, hash, opts),
         :ok <- validate_expected_head(info, opts),
         total_size = info.size,
         {:ok, {offset, length}} <- normalize_range(range, total_size) do
      source =
        Source.stateful_stream(
          fn initial, reducer ->
            download(
              context,
              node,
              hash,
              offset,
              length,
              total_size,
              initial,
              reducer,
              scoped_request_opts(opts, "get")
            )
          end,
          length
        )

      {:ok, source}
    end
  end

  defp verified_head(context, node, hash, opts) do
    if Keyword.get(opts, :verified_head, false) do
      with {:ok, size} <- Keyword.fetch(opts, :expected_size),
           {:ok, node_id} <- Keyword.fetch(opts, :expected_node_id),
           {:ok, node_generation} <- Keyword.fetch(opts, :expected_node_generation) do
        {:ok,
         %{
           hash: hash,
           size: size,
           node_id: node_id,
           node_generation: node_generation
         }}
      end
    else
      head_blob(context, node, hash, scoped_request_opts(opts, "head"))
    end
  end

  @impl true
  def delete_blob(context, node, hash, opts \\ []) do
    started_at = System.monotonic_time()
    path = blob_path(hash)
    request_id = Keyword.get_lazy(opts, :request_id, &request_id/0)

    with {:ok, cleanup_headers} <- cleanup_headers(opts),
         {:ok, headers} <-
           signed_headers(:delete, hash, "-", context, opts,
             path: path,
             request_id: request_id
           ),
         {:ok, response} <-
           Req.request(
             request_options(node, path, opts) ++
               [method: :delete, headers: cleanup_headers ++ headers]
           ),
         :ok <- decode_delete(response) do
      emit_stop(:delete_blob, started_at, 0, node, hash, :ok)
      :ok
    else
      {:error, reason} = error ->
        emit_stop(:delete_blob, started_at, 0, node, hash, reason)
        error
    end
  end

  @impl true
  def health(context, node, opts \\ []) do
    path = "/internal/v1/health"
    request_id = Keyword.get_lazy(opts, :request_id, &request_id/0)

    with {:ok, headers} <-
           signed_headers(:head, @health_hash, "-", context, opts,
             path: path,
             request_id: request_id
           ),
         {:ok, response} <-
           Req.request(request_options(node, path, opts) ++ [method: :head, headers: headers]),
         :ok <- decode_health(response, request_id, node) do
      :ok
    end
  end

  defp download(
         _context,
         _node,
         _hash,
         _offset,
         0,
         _total_size,
         initial,
         _reducer,
         _opts
       ),
       do: {:ok, initial}

  defp download(context, node, hash, offset, length, total_size, initial, reducer, opts) do
    started_at = System.monotonic_time()
    path = blob_path(hash)
    request_id = Keyword.get_lazy(opts, :request_id, &request_id/0)
    range = if offset == 0 and length == total_size, do: nil, else: {offset, length}
    range_header = encode_range(range)

    with {:ok, headers} <-
           signed_headers(:get, hash, "-", context, opts,
             path: path,
             request_id: request_id,
             range: range_header
           ),
         {:ok, response} <-
           Req.request(
             request_options(node, path, opts) ++
               [
                 method: :get,
                 headers: headers,
                 into: stream_into(reducer, initial, range, offset, length, total_size)
               ]
           ),
         {:ok, final} <-
           validate_download(response, initial, range, offset, length, total_size) do
      emit_stop(:open_blob, started_at, length, node, hash, :ok)
      {:ok, final}
    else
      {:error, reason, _final} = error ->
        emit_stop(:open_blob, started_at, 0, node, hash, reason)
        error

      {:error, reason} ->
        emit_stop(:open_blob, started_at, 0, node, hash, reason)
        {:error, reason, initial}
    end
  end

  defp stream_into(reducer, initial, range, offset, length, total_size) do
    fn {:data, data}, {request, response} ->
      case validate_response_headers(response, range, offset, length, total_size) do
        :ok ->
          current =
            Req.Response.get_private(
              response,
              :ex_storage_service_cluster_consumer_state,
              initial
            )

          case reducer.(data, current) do
            {:cont, next} ->
              response =
                response
                |> Req.Response.put_private(
                  :ex_storage_service_cluster_consumer_state,
                  next
                )
                |> Req.Response.put_private(
                  :ex_storage_service_cluster_bytes,
                  Req.Response.get_private(response, :ex_storage_service_cluster_bytes, 0) +
                    byte_size(data)
                )

              {:cont, {request, response}}

            {:halt, reason, next} ->
              response =
                response
                |> Req.Response.put_private(
                  :ex_storage_service_cluster_consumer_state,
                  next
                )
                |> Req.Response.put_private(:ex_storage_service_cluster_sink_error, reason)

              {:halt, {request, response}}
          end

        {:error, _reason} = error ->
          response =
            Req.Response.put_private(response, :ex_storage_service_cluster_stream_error, error)

          {:halt, {request, response}}
      end
    end
  end

  defp validate_download(response, initial, range, offset, length, total_size) do
    received = Req.Response.get_private(response, :ex_storage_service_cluster_bytes, 0)
    sink_error = Req.Response.get_private(response, :ex_storage_service_cluster_sink_error)
    stream_error = Req.Response.get_private(response, :ex_storage_service_cluster_stream_error)

    final =
      Req.Response.get_private(response, :ex_storage_service_cluster_consumer_state, initial)

    cond do
      stream_error ->
        {:error, elem(stream_error, 1), final}

      sink_error ->
        {:error, {:sink, sink_error}, final}

      (header_error = validate_response_headers(response, range, offset, length, total_size)) !=
          :ok ->
        {:error, elem(header_error, 1), final}

      received != length ->
        {:error, :incomplete_response, final}

      true ->
        {:ok, final}
    end
  end

  defp validate_response_headers(response, range, offset, length, total_size) do
    expected_status = if range, do: 206, else: 200

    cond do
      response.status != expected_status ->
        response_error(response)

      header_integer(response, "content-length") != {:ok, length} ->
        {:error, :invalid_content_length}

      range &&
          header(response, "content-range") !=
            {:ok, "bytes #{offset}-#{offset + length - 1}/#{total_size}"} ->
        {:error, :invalid_content_range}

      true ->
        :ok
    end
  end

  defp source_enumerable(%StagedBlob{path: path, size: size}),
    do: {:ok, Source.request_body(Source.file(path, 0, size))}

  defp source_enumerable({:file, _path, _offset, _length} = source),
    do: {:ok, Source.request_body(source)}

  defp source_enumerable({:stream, _producer, _length} = source),
    do: {:ok, Source.request_body(source)}

  defp source_enumerable(enumerable) when not is_binary(enumerable), do: {:ok, enumerable}
  defp source_enumerable(_source), do: {:error, :unsupported_source}

  defp signed_headers(method, hash, size, context, opts, auth_opts) do
    case secret(context, opts) do
      secret when is_binary(secret) and secret != "" ->
        {:ok, InternalAuth.sign(method, hash, size, secret, auth_opts)}

      _ ->
        {:error, :missing_internal_secret}
    end
  end

  defp secret(context, opts) do
    Keyword.get(opts, :secret) ||
      context.config.internal_secret ||
      Application.get_env(:ex_storage_service_cluster, :secret)
  end

  defp request_options(node, path, opts) do
    [
      url: node_url(node) <> path,
      connect_options: [protocols: [:http1]],
      retry: false,
      redirect: false,
      decode_body: false,
      receive_timeout: Keyword.get(opts, :timeout, 60_000)
    ]
  end

  defp decode_ack(%Req.Response{status: 200} = response, descriptor, request_id) do
    with {:ok, node_id} <- header(response, "x-ess-node-id"),
         {:ok, generation} <- header_integer(response, "x-ess-node-generation"),
         {:ok, hash} <- header(response, "x-ess-blob-sha256"),
         {:ok, size} <- header_integer(response, "x-ess-blob-size"),
         {:ok, verified_at} <- header_integer(response, "x-ess-verified-at"),
         true <- hash == descriptor.hash and size == descriptor.size do
      {:ok,
       %ReplicaAck{
         node_id: node_id,
         node_generation: generation,
         hash: hash,
         size: size,
         verified_at: verified_at,
         fencing_or_request_id: request_id
       }}
    else
      _ -> {:error, :invalid_replica_ack}
    end
  end

  defp decode_ack(response, _descriptor, _request_id), do: response_error(response)

  defp decode_head(%Req.Response{status: 200} = response, hash, request_id) do
    with {:ok, node_id} <- header(response, "x-ess-node-id"),
         {:ok, node_generation} <- header_integer(response, "x-ess-node-generation"),
         {:ok, ^hash} <- header(response, "x-ess-blob-sha256"),
         {:ok, size} <- header_integer(response, "x-ess-blob-size"),
         {:ok, ^size} <- header_integer(response, "content-length"),
         {:ok, verified_at} <- header_integer(response, "x-ess-verified-at"),
         {:ok, ^request_id} <- header(response, "x-ess-request-id") do
      {:ok,
       %{
         hash: hash,
         size: size,
         node_id: node_id,
         node_generation: node_generation,
         verified_at: verified_at,
         fencing_or_request_id: request_id
       }}
    else
      _ -> {:error, :invalid_blob_head}
    end
  end

  defp decode_head(%Req.Response{status: 404}, _hash, _request_id), do: {:error, :not_found}
  defp decode_head(response, _hash, _request_id), do: response_error(response)

  defp decode_delete(%Req.Response{status: status}) when status in [200, 204, 404], do: :ok
  defp decode_delete(response), do: response_error(response)

  defp decode_health(%Req.Response{status: 200} = response, request_id, node) do
    with {:ok, ^request_id} <- header(response, "x-ess-request-id"),
         {:ok, node_id} <- header(response, "x-ess-node-id"),
         true <- node_id == Map.fetch!(node, :node_id),
         {:ok, generation} <- header_integer(response, "x-ess-node-generation"),
         true <- generation == Map.fetch!(node, :generation) do
      :ok
    else
      _other -> {:error, :invalid_health_response}
    end
  end

  defp decode_health(response, _request_id, _node), do: response_error(response)

  defp validate_expected_head(info, opts) do
    expected = %{
      size: Keyword.get(opts, :expected_size, info.size),
      node_id: Keyword.get(opts, :expected_node_id, info.node_id),
      node_generation: Keyword.get(opts, :expected_node_generation, info.node_generation)
    }

    if Map.take(info, Map.keys(expected)) == expected,
      do: :ok,
      else: {:error, :replica_identity_mismatch}
  end

  defp cleanup_headers(opts) do
    with {:ok, job_id} <- Keyword.fetch(opts, :cleanup_job_id),
         {:ok, target_generation} <- Keyword.fetch(opts, :cleanup_target_generation),
         {:ok, owner_node} <- Keyword.fetch(opts, :cleanup_owner_node),
         {:ok, owner_generation} <- Keyword.fetch(opts, :cleanup_owner_generation),
         {:ok, fencing_token} <- Keyword.fetch(opts, :cleanup_fencing_token),
         true <- is_binary(job_id) and job_id != "",
         true <- is_integer(target_generation) and target_generation > 0,
         true <- is_binary(owner_node) and owner_node != "",
         true <- is_integer(owner_generation) and owner_generation > 0,
         true <- is_integer(fencing_token) and fencing_token >= 0 do
      {:ok,
       [
         {"x-ess-cleanup-job-id", job_id},
         {"x-ess-cleanup-target-generation", Integer.to_string(target_generation)},
         {"x-ess-cleanup-owner-node", owner_node},
         {"x-ess-cleanup-owner-generation", Integer.to_string(owner_generation)},
         {"x-ess-cleanup-fencing-token", Integer.to_string(fencing_token)}
       ]}
    else
      _other -> {:error, :missing_cleanup_fence}
    end
  end

  defp response_error(%Req.Response{status: 401}), do: {:error, :unauthorized}
  defp response_error(%Req.Response{status: 404}), do: {:error, :not_found}
  defp response_error(%Req.Response{status: 409}), do: {:error, :blob_conflict}
  defp response_error(%Req.Response{status: 413}), do: {:error, :entity_too_large}
  defp response_error(%Req.Response{status: 416}), do: {:error, :invalid_range}
  defp response_error(%Req.Response{status: 422}), do: {:error, :checksum_mismatch}
  defp response_error(%Req.Response{status: status}), do: {:error, {:http_status, status}}

  defp normalize_range(nil, total_size), do: {:ok, {0, total_size}}
  defp normalize_range(:all, total_size), do: {:ok, {0, total_size}}

  defp normalize_range({offset, length}, total_size)
       when is_integer(offset) and offset >= 0 and is_integer(length) and length >= 0 and
              offset <= total_size and length <= total_size - offset,
       do: {:ok, {offset, length}}

  defp normalize_range(_range, _total_size), do: {:error, :invalid_range}

  defp encode_range(nil), do: nil
  defp encode_range({offset, length}), do: "bytes=#{offset}-#{offset + length - 1}"

  defp header(response, name) do
    case Req.Response.get_header(response, name) do
      [value] -> {:ok, value}
      _ -> {:error, {:invalid_header, name}}
    end
  end

  defp header_integer(response, name) do
    with {:ok, value} <- header(response, name),
         {integer, ""} when integer >= 0 <- Integer.parse(value) do
      {:ok, integer}
    else
      _ -> {:error, {:invalid_header, name}}
    end
  end

  defp blob_path(hash), do: "/internal/v1/blobs/#{hash}"

  defp node_url(url) when is_binary(url), do: String.trim_trailing(url, "/")
  defp node_url(%{internal_endpoint: url}), do: node_url(url)
  defp node_url(%{internal_advertised_url: url}), do: node_url(url)
  defp node_url(%{advertised_url: url}), do: node_url(url)

  defp request_id do
    18 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
  end

  defp scoped_request_opts(opts, scope) do
    case Keyword.fetch(opts, :request_id) do
      {:ok, request_id} ->
        derived =
          :crypto.hash(:sha256, "#{request_id}:#{scope}")
          |> binary_part(0, 18)
          |> Base.url_encode64(padding: false)

        Keyword.put(opts, :request_id, derived)

      :error ->
        opts
    end
  end

  defp emit_stop(operation, started_at, bytes, node, hash, result) do
    :telemetry.execute(
      @telemetry_prefix ++ [:stop],
      %{duration: System.monotonic_time() - started_at, bytes: bytes},
      %{
        direction: :client,
        operation: operation,
        peer: peer_name(node),
        hash: hash,
        result: result
      }
    )
  end

  defp peer_name(url) when is_binary(url), do: url
  defp peer_name(%{id: id}), do: to_string(id)
  defp peer_name(_node), do: "unknown"
end
