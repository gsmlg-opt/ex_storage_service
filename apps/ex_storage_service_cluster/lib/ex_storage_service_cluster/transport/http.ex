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

  @chunk_size 262_144
  @telemetry_prefix [:ex_storage_service, :cluster, :blob_transport]

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
           Req.request(request_options(node, path, opts) ++
             [method: :put, headers: [{"content-length", descriptor.size} | headers], body: body]
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
         {:ok, info} <- decode_head(response, hash) do
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
    with {:ok, %{size: total_size}} <-
           head_blob(context, node, hash, scoped_request_opts(opts, "head")),
         {:ok, {offset, length}} <- normalize_range(range, total_size) do
      source =
        Source.stream(
          fn sink ->
            download(
              context,
              node,
              hash,
              offset,
              length,
              total_size,
              sink,
              scoped_request_opts(opts, "get")
            )
          end,
          length
        )

      {:ok, source}
    end
  end

  @impl true
  def delete_blob(_context, _node, _hash, _opts \\ []), do: {:error, :unsupported}

  @impl true
  def health(_context, _node, _opts \\ []), do: {:error, :unsupported}

  defp download(_context, _node, _hash, _offset, 0, _total_size, _sink, _opts), do: :ok

  defp download(context, node, hash, offset, length, total_size, sink, opts) do
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
                 into: stream_into(sink, range, offset, length, total_size)
               ]
           ),
         :ok <- validate_download(response, range, offset, length, total_size) do
      emit_stop(:open_blob, started_at, length, node, hash, :ok)
      :ok
    else
      {:error, reason} = error ->
        emit_stop(:open_blob, started_at, 0, node, hash, reason)
        error
    end
  end

  defp stream_into(sink, range, offset, length, total_size) do
    fn {:data, data}, {request, response} ->
      case validate_response_headers(response, range, offset, length, total_size) do
        :ok ->
          case sink.(data) do
            :ok ->
              response =
                Req.Response.put_private(
                  response,
                  :ex_storage_service_cluster_bytes,
                  Req.Response.get_private(response, :ex_storage_service_cluster_bytes, 0) +
                    byte_size(data)
                )

              {:cont, {request, response}}

            {:error, reason} ->
              response =
                Req.Response.put_private(response, :ex_storage_service_cluster_sink_error, reason)

              {:halt, {request, response}}
          end

        {:error, _reason} = error ->
          response =
            Req.Response.put_private(response, :ex_storage_service_cluster_stream_error, error)

          {:halt, {request, response}}
      end
    end
  end

  defp validate_download(response, range, offset, length, total_size) do
    received = Req.Response.get_private(response, :ex_storage_service_cluster_bytes, 0)
    sink_error = Req.Response.get_private(response, :ex_storage_service_cluster_sink_error)
    stream_error = Req.Response.get_private(response, :ex_storage_service_cluster_stream_error)

    cond do
      stream_error ->
        stream_error

      sink_error ->
        {:error, {:sink, sink_error}}

      (header_error = validate_response_headers(response, range, offset, length, total_size)) !=
          :ok ->
        header_error

      received != length ->
        {:error, :incomplete_response}

      true ->
        :ok
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
    do: {:ok, file_slice_stream(path, 0, size)}

  defp source_enumerable({:file, path, offset, length}),
    do: {:ok, file_slice_stream(path, offset, length)}

  defp source_enumerable({:stream, enumerable, _length}) when not is_function(enumerable),
    do: {:ok, enumerable}

  defp source_enumerable(enumerable) when not is_binary(enumerable), do: {:ok, enumerable}
  defp source_enumerable(_source), do: {:error, :unsupported_source}

  defp file_slice_stream(path, offset, length) do
    Stream.resource(
      fn ->
        with {:ok, io} <- :file.open(String.to_charlist(path), [:read, :raw, :binary]),
             {:ok, ^offset} <- :file.position(io, offset) do
          {:ok, io, length}
        end
      end,
      fn
        {:ok, io, 0} ->
          {:halt, {:ok, io, 0}}

        {:ok, io, remaining} ->
          read_length = min(remaining, @chunk_size)

          case :file.read(io, read_length) do
            {:ok, data} -> {[data], {:ok, io, remaining - byte_size(data)}}
            :eof -> raise "unexpected end of staged blob"
            {:error, reason} -> raise File.Error, reason: reason, action: "read", path: path
          end

        {:error, reason} ->
          raise File.Error, reason: reason, action: "open", path: path
      end,
      fn
        {:ok, io, _remaining} -> :file.close(io)
        {:error, _reason} -> :ok
      end
    )
  end

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

  defp decode_head(%Req.Response{status: 200} = response, hash) do
    with {:ok, size} <- header_integer(response, "content-length"),
         {:ok, ^hash} <- header(response, "x-ess-blob-sha256") do
      {:ok, %{hash: hash, size: size}}
    else
      _ -> {:error, :invalid_blob_head}
    end
  end

  defp decode_head(%Req.Response{status: 404}, _hash), do: {:error, :not_found}
  defp decode_head(response, _hash), do: response_error(response)

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
