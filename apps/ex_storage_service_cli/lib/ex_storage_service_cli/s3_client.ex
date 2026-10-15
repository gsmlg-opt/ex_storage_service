defmodule ExStorageServiceCli.S3Client do
  @moduledoc """
  S3-compatible HTTP client with automatic SigV4 request signing.

  Uses the shared raw-byte transport and signer with legacy result adapters.
  get_object/3 materializes at most 8 MiB for library compatibility; command-line
  transfers use Transfer.Engine's bounded streams and atomic downloads.
  """

  alias ExStorageServiceCli.{Context, Error, XmlParser, Transport.HTTPFetch}
  alias ExStorageServiceCli.S3.{Auth, Encoder, Encoding, Errors, Request, Response, XML}
  alias ExStorageServiceCli.{CLI.Debug, Transfer.Coordinator}

  @context_fields [
    :endpoint,
    :region,
    :profile,
    :access_key_id,
    :secret_access_key,
    :session_token,
    :credentials,
    :credential_owner,
    :coordinator,
    :directory_sessions,
    :unsigned,
    :tls,
    :addressing,
    :timeout,
    :connect_timeout,
    :capabilities,
    :frontend,
    :options
  ]

  @doc """
  Creates a new S3 client configuration.
  """
  def new(ctx) do
    Map.merge(Map.take(ctx, @context_fields), %{
      endpoint: ctx.endpoint,
      access_key_id: ctx[:access_key_id],
      secret_access_key: ctx[:secret_access_key],
      session_token: ctx[:session_token],
      unsigned: ctx[:unsigned] || false,
      tls: ctx[:tls] || [],
      addressing: ctx[:addressing] || :path,
      region: ctx[:region] || "us-east-1"
    })
  end

  @doc """
  List all buckets.
  """
  def list_buckets(client) do
    url = "#{client.endpoint}/"

    case signed_request(client, "GET", url) do
      {:ok, %{status: 200, body: body}} ->
        XmlParser.decode_list_buckets(body)

      {:ok, resp} ->
        {:error, parse_error(resp)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Create a bucket.
  """
  def create_bucket(client, bucket) do
    url = "#{client.endpoint}/#{Encoding.encode(bucket)}"

    input =
      if client.region == "us-east-1",
        do: %{"Bucket" => bucket},
        else: %{
          "Bucket" => bucket,
          "CreateBucketConfiguration" => %{"LocationConstraint" => client.region}
        }

    {:ok, request} = Encoder.encode("CreateBucket", input)

    case signed_request(client, "PUT", url, request.body || "", request.headers) do
      {:ok, %{status: status}} when status in [200, 204] ->
        :ok

      {:ok, resp} ->
        {:error, parse_error(resp)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Delete a bucket.
  """
  def delete_bucket(client, bucket) do
    url = "#{client.endpoint}/#{Encoding.encode(bucket)}"

    case signed_request(client, "DELETE", url) do
      {:ok, %{status: status}} when status in [200, 204] ->
        :ok

      {:ok, resp} ->
        {:error, parse_error(resp)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Head a bucket (check existence).
  """
  def head_bucket(client, bucket) do
    url = "#{client.endpoint}/#{Encoding.encode(bucket)}"

    case signed_request(client, "HEAD", url) do
      {:ok, %{status: 200}} ->
        :ok

      {:ok, %{status: 404}} ->
        {:error, :not_found}

      {:ok, resp} ->
        {:error, parse_error(resp)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  List objects in a bucket.

  ## Options

    * `:prefix` - Filter by prefix
    * `:delimiter` - Delimiter for hierarchy (default: nil)
    * `:max_keys` - Maximum keys to return (default: 1000)
    * `:continuation_token` - Pagination token
  """
  def list_objects(client, bucket, opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "")
    delimiter = Keyword.get(opts, :delimiter)
    max_keys = Keyword.get(opts, :max_keys, 1000)
    continuation_token = Keyword.get(opts, :continuation_token)

    query_parts =
      [
        {"list-type", "2"},
        if(prefix != "", do: {"prefix", prefix}),
        if(delimiter, do: {"delimiter", delimiter}),
        {"max-keys", to_string(max_keys)},
        if(continuation_token, do: {"continuation-token", continuation_token})
      ]
      |> Enum.reject(&is_nil/1)

    query = Encoding.query(query_parts)
    url = "#{client.endpoint}/#{Encoding.encode(bucket)}?#{query}"

    case signed_request(client, "GET", url) do
      {:ok, %{status: 200, body: body}} ->
        XmlParser.decode_list_objects(body)

      {:ok, resp} ->
        {:error, parse_error(resp)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Upload a file to S3.
  """
  def put_object(client, bucket, key, body, opts \\ []) do
    content_type = Keyword.get(opts, :content_type, guess_content_type(key))
    url = "#{client.endpoint}/#{Encoding.encode(bucket)}/#{encode_key(key)}"

    extra_headers = [{"content-type", content_type}]

    case signed_request(client, "PUT", url, body, extra_headers) do
      {:ok, %{status: 200} = resp} ->
        etag =
          resp.headers
          |> get_resp_header("etag")
          |> String.trim("\"")

        {:ok, %{etag: etag}}

      {:ok, resp} ->
        {:error, parse_error(resp)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Download an object from S3.
  """
  def get_object(client, bucket, key) do
    url = "#{client.endpoint}/#{Encoding.encode(bucket)}/#{encode_key(key)}"

    case signed_request(client, "GET", url) do
      {:ok, %{status: 200} = resp} ->
        content_type =
          get_resp_header(resp.headers, "content-type")

        {:ok, %{body: resp.body, content_type: content_type}}

      {:ok, %{status: 404}} ->
        {:error, :not_found}

      {:ok, resp} ->
        {:error, parse_error(resp)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Head an object (get metadata without body).
  """
  def head_object(client, bucket, key) do
    url = "#{client.endpoint}/#{Encoding.encode(bucket)}/#{encode_key(key)}"

    case signed_request(client, "HEAD", url) do
      {:ok, %{status: 200} = resp} ->
        {:ok,
         %{
           content_type: get_resp_header(resp.headers, "content-type"),
           content_length: get_resp_header(resp.headers, "content-length"),
           etag: get_resp_header(resp.headers, "etag") |> String.trim("\""),
           last_modified: get_resp_header(resp.headers, "last-modified")
         }}

      {:ok, %{status: 404}} ->
        {:error, :not_found}

      {:ok, resp} ->
        {:error, parse_error(resp)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Delete an object.
  """
  def delete_object(client, bucket, key) do
    url = "#{client.endpoint}/#{Encoding.encode(bucket)}/#{encode_key(key)}"

    case signed_request(client, "DELETE", url) do
      {:ok, %{status: status}} when status in [200, 204] ->
        :ok

      {:ok, resp} ->
        {:error, parse_error(resp)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Copy an object within S3.
  """
  def copy_object(client, src_bucket, src_key, dst_bucket, dst_key) do
    url = "#{client.endpoint}/#{Encoding.encode(dst_bucket)}/#{encode_key(dst_key)}"
    copy_source = Encoding.copy_source(src_bucket, src_key)
    extra_headers = [{"x-amz-copy-source", copy_source}]

    case signed_request(client, "PUT", url, "", extra_headers) do
      {:ok, %{status: 200, body: body} = response} ->
        with {:ok, node} <- XML.parse(body) do
          cond do
            XML.local_name(node.name) == "Error" ->
              {:error, parse_error(response)}

            XML.local_name(node.name) == "CopyObjectResult" and XML.child_text(node, "ETag") != "" ->
              :ok

            true ->
              {:error, Error.new(:protocol, "S3 copy response lacks a confirmed object identity")}
          end
        end

      {:ok, resp} ->
        {:error, parse_error(resp)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Check server health.
  """
  def health(client) do
    url = "#{client.endpoint}/health"

    case do_request(client, "GET", url, "", []) do
      {:ok, %{status: 200, body: body}} when is_binary(body) ->
        with {:ok, decoded} <- decode_health(body), do: JSON.decode(decoded)

      {:ok, resp} ->
        {:error, "Health check failed with status #{resp.status}"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private helpers

  defp signed_request(client, method, url, body \\ "", extra_headers \\ []) do
    request = %Request{
      operation: "LegacyRequest",
      method: method,
      body: body,
      headers: extra_headers
    }

    context = context(client)

    with {:ok, context} <- Context.refresh(context),
         {:ok, request} <- Auth.prepare(request, url, context, []),
         {:ok, response} <- request(context, url, request) do
      {:ok, Map.put(response, :legacy_context, context)}
    end
  end

  defp do_request(client, method, url, body, headers) do
    request(
      context(client),
      url,
      %Request{operation: "Health", method: method, body: body, headers: headers},
      max_control_bytes: 1_048_576
    )
  end

  defp request(context, url, request, opts \\ []) do
    with {:ok, lease} <- reserve(context.coordinator) do
      try do
        Debug.request(context, request, url)

        with {:ok, response} <-
               HTTPFetch.request(
                 url,
                 %{request | lease: lease},
                 Keyword.merge(opts,
                   ssl: context.tls,
                   timeout: context.timeout,
                   connect_timeout: context.connect_timeout,
                   resolve_entries: context.options[:resolve_entries] || %{}
                 )
               ) do
          Debug.response(context, request, response)
          {:ok, response}
        end
      after
        Coordinator.release(lease)
      end
    end
  end

  defp reserve(nil), do: {:ok, nil}
  defp reserve(coordinator), do: Coordinator.acquire(coordinator, 131_072)

  defp get_resp_header(headers, key) do
    headers |> HTTP.Headers.new() |> HTTP.Headers.get(key) || ""
  end

  defp parse_error(%{body: body} = response) when is_binary(body) and byte_size(body) > 0 do
    Errors.from_response(
      "LegacyRequest",
      struct(Response, Map.take(response, [:status, :headers, :body, :request_id])),
      Map.get(response, :legacy_context, %Context{})
    ).message
  end

  defp parse_error(%{status: status}) do
    "HTTP #{status}"
  end

  defp encode_key(key) do
    Encoding.path(key)
  end

  @doc false
  def context(client), do: struct(Context, Map.take(client, @context_fields))

  defp decode_health(<<0x1F, 0x8B, _::binary>> = body) do
    zlib = :zlib.open()

    try do
      :ok = :zlib.inflateInit(zlib, 31)
      inflate_health(zlib, body, [], 0)
    rescue
      _ -> {:error, Error.new(:protocol, "Invalid compressed health response")}
    after
      :zlib.close(zlib)
    end
  end

  defp decode_health(body), do: {:ok, body}

  defp inflate_health(zlib, input, chunks, size) do
    {status, output} = :zlib.safeInflate(zlib, input)
    size = size + IO.iodata_length(output)

    cond do
      size > 1_048_576 -> {:error, Error.new(:protocol, "Health response exceeds its size limit")}
      status == :finished -> {:ok, [output | chunks] |> Enum.reverse() |> IO.iodata_to_binary()}
      status == :continue -> inflate_health(zlib, <<>>, [output | chunks], size)
    end
  end

  defp guess_content_type(key) do
    ext = Path.extname(key) |> String.downcase()

    case ext do
      ".html" -> "text/html"
      ".htm" -> "text/html"
      ".css" -> "text/css"
      ".js" -> "application/javascript"
      ".json" -> "application/json"
      ".xml" -> "application/xml"
      ".txt" -> "text/plain"
      ".md" -> "text/markdown"
      ".csv" -> "text/csv"
      ".png" -> "image/png"
      ".jpg" -> "image/jpeg"
      ".jpeg" -> "image/jpeg"
      ".gif" -> "image/gif"
      ".svg" -> "image/svg+xml"
      ".webp" -> "image/webp"
      ".pdf" -> "application/pdf"
      ".zip" -> "application/zip"
      ".gz" -> "application/gzip"
      ".tar" -> "application/x-tar"
      ".mp4" -> "video/mp4"
      ".webm" -> "video/webm"
      ".mp3" -> "audio/mpeg"
      ".wav" -> "audio/wav"
      ".wasm" -> "application/wasm"
      _ -> "application/octet-stream"
    end
  end
end
