defmodule ExStorageServiceCli.S3Client do
  @moduledoc """
  S3-compatible HTTP client with automatic SigV4 request signing.

  Wraps `Req` with path-style S3 addressing and XML response parsing.
  """

  alias ExStorageServiceCli.SigV4
  alias ExStorageServiceCli.XmlParser

  @doc """
  Creates a new S3 client configuration.
  """
  def new(ctx) do
    %{
      endpoint: ctx.endpoint,
      access_key_id: ctx[:access_key_id],
      secret_access_key: ctx[:secret_access_key],
      region: ctx[:region] || "us-east-1"
    }
  end

  @doc """
  List all buckets.
  """
  def list_buckets(client) do
    url = "#{client.endpoint}/"

    case signed_request(client, "GET", url) do
      {:ok, %{status: 200, body: body}} ->
        {:ok, XmlParser.parse_list_buckets(body)}

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
    url = "#{client.endpoint}/#{URI.encode(bucket)}"

    case signed_request(client, "PUT", url) do
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
    url = "#{client.endpoint}/#{URI.encode(bucket)}"

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
    url = "#{client.endpoint}/#{URI.encode(bucket)}"

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

    query = URI.encode_query(query_parts)
    url = "#{client.endpoint}/#{URI.encode(bucket)}?#{query}"

    case signed_request(client, "GET", url) do
      {:ok, %{status: 200, body: body}} ->
        {:ok, XmlParser.parse_list_objects(body)}

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
    url = "#{client.endpoint}/#{URI.encode(bucket)}/#{encode_key(key)}"

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
    url = "#{client.endpoint}/#{URI.encode(bucket)}/#{encode_key(key)}"

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
    url = "#{client.endpoint}/#{URI.encode(bucket)}/#{encode_key(key)}"

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
    url = "#{client.endpoint}/#{URI.encode(bucket)}/#{encode_key(key)}"

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
    url = "#{client.endpoint}/#{URI.encode(dst_bucket)}/#{encode_key(dst_key)}"
    copy_source = "/#{src_bucket}/#{src_key}"
    extra_headers = [{"x-amz-copy-source", copy_source}]

    case signed_request(client, "PUT", url, "", extra_headers) do
      {:ok, %{status: 200}} ->
        :ok

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

    case Req.get(url) do
      {:ok, %{status: 200, body: body}} when is_binary(body) ->
        {:ok, Jason.decode!(body)}

      {:ok, %{status: 200, body: body}} when is_map(body) ->
        {:ok, body}

      {:ok, resp} ->
        {:error, "Health check failed with status #{resp.status}"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private helpers

  defp signed_request(client, method, url, body \\ "", extra_headers \\ []) do
    if client.access_key_id && client.secret_access_key do
      auth_headers =
        SigV4.sign_headers(method, url, extra_headers, body,
          access_key_id: client.access_key_id,
          secret_access_key: client.secret_access_key,
          region: client.region
        )

      # Merge auth headers with extra headers, auth takes precedence
      all_headers = merge_headers(extra_headers, auth_headers)
      do_request(method, url, body, all_headers)
    else
      do_request(method, url, body, extra_headers)
    end
  end

  defp do_request(method, url, body, headers) do
    req =
      Req.new(
        url: url,
        method: String.downcase(method) |> String.to_atom(),
        headers: Map.new(headers),
        body: if(body == "", do: nil, else: body),
        decode_body: false,
        retry: false,
        redirect: false
      )

    Req.request(req)
  end

  defp merge_headers(base, override) do
    override_keys =
      override
      |> Enum.map(fn {k, _} -> String.downcase(k) end)
      |> MapSet.new()

    filtered_base =
      Enum.reject(base, fn {k, _} -> String.downcase(k) in override_keys end)

    filtered_base ++ override
  end

  defp get_resp_header(headers, key) do
    key_down = String.downcase(key)

    case headers do
      %{} ->
        # Req returns headers as a map of lists
        headers
        |> Enum.find_value("", fn {k, v} ->
          if String.downcase(k) == key_down do
            case v do
              [val | _] -> val
              val when is_binary(val) -> val
              _ -> ""
            end
          end
        end)

      _ ->
        ""
    end
  end

  defp parse_error(%{body: body, status: status}) when is_binary(body) and byte_size(body) > 0 do
    case XmlParser.parse_error(body) do
      {:ok, error} -> "#{error.code}: #{error.message} (HTTP #{status})"
      _ -> "HTTP #{status}: #{body}"
    end
  end

  defp parse_error(%{status: status}) do
    "HTTP #{status}"
  end

  defp encode_key(key) do
    key
    |> String.split("/")
    |> Enum.map(&URI.encode/1)
    |> Enum.join("/")
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
