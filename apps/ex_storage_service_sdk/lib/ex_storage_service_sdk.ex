defmodule ExStorageServiceSdk do
  @moduledoc """
  S3 client for ExStorageService and other S3-compatible object storage.

  Build a client config once and pass it to every call. The config captures its
  endpoint, region, credentials and transport, so in-flight listings and multipart
  uploads keep their connection even if application settings change later.

      {:ok, s3} =
        ExStorageServiceSdk.new(
          endpoint: "http://localhost:9000",
          region: "us-east-1",
          access_key_id: "AKIA...",
          secret_access_key: "..."
        )

      {:ok, %{etag: etag}} = ExStorageServiceSdk.put_object(s3, "bucket", "a b+c%.txt", "hello")
      {:ok, %{body: "hello"}} = ExStorageServiceSdk.get_object(s3, "bucket", "a b+c%.txt")

  ## Operations

  `call/4` runs any operation in the pinned S3 service model (AWS CLI 2.31.18) with
  model-shaped input and output maps, for example
  `call(s3, "ListObjectsV2", %{"Bucket" => "b", "EncodingType" => "url"})`.
  The helpers in this module cover common object and multipart operations and
  return response metadata alongside the result.

  ## Errors

  Every failure is `{:error, %ExStorageServiceSdk.Error{}}`:

    * `category: :service` - the server answered with an error. `http_status`
      distinguishes 404, 412, 416 and so on. `code` is the S3 error code (for
      example `"NoSuchKey"` or `"PreconditionFailed"`), or `"HTTP<status>"` when the
      response had no body, as for HEAD requests.
    * `category: :transport` - connection, TLS or timeout failure. `code` is an atom.
    * `category: :protocol` - the server answered with something that is not valid
      S3, including an HTTP 200 response that carries an S3 `<Error>` document.

  ## Retries

  Reads (GET, HEAD, List) and idempotent multipart steps (`UploadPart`,
  `AbortMultipartUpload`) retry transient failures up to three attempts in total.
  `PutObject`, `CopyObject`, `DeleteObject` and `CompleteMultipartUpload` are never
  replayed automatically. If such a request may have reached the server but its
  outcome is unknown, the call fails with `code: :indeterminate`. For conditional
  writes, reconcile by reading the object (for example comparing its ETag) before
  deciding to try again, because a replayed `If-None-Match: *` write would fail
  with 412 even when the first attempt succeeded.
  """

  alias ExStorageServiceSdk.{Config, Credentials, Error}
  alias ExStorageServiceSdk.S3.{Client, Decoder, Paginator}

  @default_max_body_bytes 8_388_608

  @doc """
  Builds a client config.

  ## Options

    * `:endpoint` - required HTTP(S) base URL, for example `"https://minio.local:9000"`.
    * `:region` - signing region. Defaults to `"us-east-1"`.
    * `:access_key_id`, `:secret_access_key`, `:session_token` - static credentials.
    * `:credential_provider` - zero-arity function returning
      `{:ok, %ExStorageServiceSdk.Credentials{}}`, called before each signed request.
      Use it instead of static credentials for rotating keys.
    * `:unsigned` - send anonymous requests. Defaults to `false`.
    * `:path_style` - use path-style addressing (`/bucket/key`). Defaults to `true`;
      set `false` for virtual-hosted buckets (`bucket.host/key`).
    * `:tls` - `:ssl` client options. Defaults to `[verify: :verify_peer]`.
    * `:timeout`, `:connect_timeout`, `:read_idle_timeout`, `:write_idle_timeout` -
      milliseconds.
    * `:transport` - module implementing `ExStorageServiceSdk.Transport`.
    * `:options` - map of protocol options, for example
      `%{request_headers: [{"x-custom", "1"}]}`.
  """
  @spec new(keyword()) :: {:ok, Config.t()} | {:error, Error.t()}
  def new(opts) when is_list(opts) do
    endpoint = Keyword.get(opts, :endpoint)

    with :ok <- Config.validate_endpoint(endpoint),
         {:ok, credentials} <- static_credentials(opts) do
      defaults = %Config{}

      config = %Config{
        endpoint: String.trim_trailing(endpoint, "/"),
        region: Keyword.get(opts, :region, defaults.region),
        unsigned: Keyword.get(opts, :unsigned, false),
        credential_provider: Keyword.get(opts, :credential_provider),
        addressing: if(Keyword.get(opts, :path_style, true), do: :path, else: :virtual),
        tls: Keyword.get(opts, :tls, defaults.tls),
        timeout: Keyword.get(opts, :timeout, defaults.timeout),
        connect_timeout: Keyword.get(opts, :connect_timeout, defaults.connect_timeout),
        read_idle_timeout: Keyword.get(opts, :read_idle_timeout),
        write_idle_timeout: Keyword.get(opts, :write_idle_timeout),
        transport: Keyword.get(opts, :transport, defaults.transport),
        coordinator: Keyword.get(opts, :coordinator),
        options: Map.new(Keyword.get(opts, :options, %{}))
      }

      {:ok, Config.with_credentials(config, credentials)}
    end
  end

  @doc """
  Runs any modeled S3 operation.

  `input` uses the model's member names (`"Bucket"`, `"Key"`, `"IfMatch"`, ...).
  Pass `return_response: true` in `opts` to receive
  `{:ok, result, %{status: ..., headers: ..., request_id: ...}}`.
  """
  @spec call(Config.t(), String.t(), map(), keyword()) ::
          {:ok, map()} | {:ok, map(), map()} | {:error, Error.t()}
  def call(%Config{} = config, operation, input \\ %{}, opts \\ []),
    do: Client.call(config, operation, input, opts)

  @doc """
  Lazily streams result pages of a paginated operation, following continuation
  tokens. Each element is `{:ok, page}` or a final `{:error, error}`.

      ExStorageServiceSdk.pages(s3, "ListObjectsV2", %{"Bucket" => "b"})
      |> Enum.flat_map(fn {:ok, page} -> page["Contents"] || [] end)
  """
  @spec pages(Config.t(), String.t(), map(), keyword()) :: Enumerable.t()
  def pages(%Config{} = config, operation, input, opts \\ []) do
    Paginator.pages(operation, input, &call(config, &1, &2, opts), opts)
  end

  @doc """
  Reads an object into memory.

  Returns `%{body: binary, status: 200 | 206, etag: ..., headers: ..., result: map}`.
  A `"Range"` input yields status 206. Bodies larger than `:max_body_bytes`
  (default 8 MiB) fail; use `get_object_stream/5` for large objects.
  """
  @spec get_object(Config.t(), String.t(), String.t(), map(), keyword()) ::
          {:ok, map()} | {:error, Error.t()}
  def get_object(%Config{} = config, bucket, key, input \\ %{}, opts \\ []) do
    limit = Keyword.get(opts, :max_body_bytes, @default_max_body_bytes)

    reducer = fn chunk, {chunks, size} ->
      size = size + byte_size(chunk)

      if size > limit,
        do:
          {:error,
           Error.new(:client, "Object body exceeds max_body_bytes",
             operation: "GetObject",
             code: :body_too_large
           )},
        else: {:ok, {[chunk | chunks], size}}
    end

    with {:ok, response} <- get_object_stream(config, bucket, key, input, opts),
         {:ok, result} <- decode_headers(config, response),
         {:ok, {chunks, _size}, _metadata} <-
           config.transport.consume(response, {[], 0}, reducer, opts) do
      body = chunks |> Enum.reverse() |> IO.iodata_to_binary()
      {:ok, Map.merge(metadata(response, result), %{body: body})}
    end
  end

  @doc """
  Starts a GET and returns the open `ExStorageServiceSdk.S3.Response` with its
  body still on the wire. Consume it with
  `config.transport.consume(response, acc, reducer, opts)` (which always closes
  it), or release it with `config.transport.close(response)`.
  """
  @spec get_object_stream(Config.t(), String.t(), String.t(), map(), keyword()) ::
          {:ok, ExStorageServiceSdk.S3.Response.t()} | {:error, Error.t()}
  def get_object_stream(%Config{} = config, bucket, key, input \\ %{}, opts \\ []) do
    call(config, "GetObject", object(input, bucket, key), Keyword.put(opts, :mode, :stream))
  end

  @doc """
  Reads object metadata. Returns `%{status, etag, headers, result}`. A missing object
  fails with `%Error{http_status: 404}`.
  """
  def head_object(%Config{} = config, bucket, key, input \\ %{}, opts \\ []),
    do: with_metadata(config, "HeadObject", object(input, bucket, key), opts)

  @doc """
  Uploads `body` (a binary) in one request. `input` accepts model members such as
  `"ContentType"`, `"Metadata"`, `"IfMatch"` and `"IfNoneMatch"`.
  """
  def put_object(%Config{} = config, bucket, key, body, input \\ %{}, opts \\ [])
      when is_binary(body) do
    with_metadata(config, "PutObject", object(input, bucket, key), body_options(body, opts))
  end

  @doc "Deletes an object. Deleting a missing key succeeds, as in S3."
  def delete_object(%Config{} = config, bucket, key, input \\ %{}, opts \\ []),
    do: with_metadata(config, "DeleteObject", object(input, bucket, key), opts)

  @doc """
  Copies `input["CopySource"]` to `bucket`/`key`.

  Prefer the structured source `%{"Bucket" => b, "Key" => k}` (optionally
  `"VersionId"`): the SDK encodes it, so any exact key is safe. A string source is
  also accepted but is ambiguous for keys that contain `?versionId=`. Input also
  accepts `"CopySourceIfMatch"`, `"CopySourceIfNoneMatch"`, `"MetadataDirective"`,
  `"TaggingDirective"` and the other modeled copy members.
  """
  def copy_object(%Config{} = config, bucket, key, input, opts \\ []),
    do: with_metadata(config, "CopyObject", object(input, bucket, key), opts)

  @doc """
  Lists one page of keys.

  Exact keys are preserved: unless the caller sets `"EncodingType"`, the request
  asks for `EncodingType=url` and the SDK decodes keys and prefixes in the result,
  so Unicode, spaces, `+` and `%` round-trip. A caller that sets `"EncodingType"`
  receives the server's encoded values unchanged. Pass the returned
  `"NextContinuationToken"` as `"ContinuationToken"`, or use `pages/4`.
  """
  def list_objects_v2(%Config{} = config, bucket, input \\ %{}, opts \\ []),
    do: call(config, "ListObjectsV2", Map.put(input, "Bucket", bucket), opts)

  @doc "Starts a multipart upload and returns its result map, including `\"UploadId\"`."
  def create_multipart_upload(%Config{} = config, bucket, key, input \\ %{}, opts \\ []),
    do: call(config, "CreateMultipartUpload", object(input, bucket, key), opts)

  @doc "Uploads one part (a binary). Returns `%{etag: ..., ...}` for completion."
  def upload_part(%Config{} = config, bucket, key, upload_id, part_number, body, opts \\ [])
      when is_binary(body) do
    input =
      object(%{"UploadId" => upload_id, "PartNumber" => part_number}, bucket, key)

    with_metadata(config, "UploadPart", input, body_options(body, opts))
  end

  @doc """
  Completes a multipart upload. `parts` is a list of `{part_number, etag}`; it is
  sorted by part number before sending.
  """
  def complete_multipart_upload(%Config{} = config, bucket, key, upload_id, parts, opts \\ []) do
    sorted =
      parts
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {number, etag} -> %{"PartNumber" => number, "ETag" => etag} end)

    input =
      object(%{"UploadId" => upload_id, "MultipartUpload" => %{"Parts" => sorted}}, bucket, key)

    with_metadata(config, "CompleteMultipartUpload", input, opts)
  end

  @doc "Aborts a multipart upload, discarding its uploaded parts."
  def abort_multipart_upload(%Config{} = config, bucket, key, upload_id, opts \\ []) do
    call(config, "AbortMultipartUpload", object(%{"UploadId" => upload_id}, bucket, key), opts)
  end

  defp object(input, bucket, key), do: Map.merge(input, %{"Bucket" => bucket, "Key" => key})

  defp body_options(body, opts) do
    opts
    |> Keyword.put(:body_source, chunks(body))
    |> Keyword.put(:content_length, byte_size(body))
    |> Keyword.put_new(
      :payload_hash,
      :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)
    )
  end

  # The HTTP/1 uploader bounds each chunk; slices share the caller's binary, and a
  # list (unlike a stream) can be replayed by retries and folded for checksums.
  @chunk_bytes 65_536
  defp chunks(""), do: [""]

  defp chunks(body) do
    for offset <- 0..(byte_size(body) - 1)//@chunk_bytes,
        do: binary_part(body, offset, min(@chunk_bytes, byte_size(body) - offset))
  end

  defp with_metadata(config, operation, input, opts) do
    with {:ok, result, response} <-
           call(config, operation, input, Keyword.put(opts, :return_response, true)),
         do: {:ok, metadata(response, result)}
  end

  defp decode_headers(config, response) do
    with {:error, _} = error <- Decoder.decode_headers("GetObject", response) do
      config.transport.close(response)
      error
    end
  end

  defp metadata(response, result) do
    %{
      status: response.status,
      headers: response.headers,
      request_id: response.request_id,
      etag: result["ETag"] || header(response.headers, "etag"),
      result: result
    }
  end

  defp header(headers, name) do
    Enum.find_value(headers, fn {key, value} ->
      if String.downcase(key) == name, do: value
    end)
  end

  defp static_credentials(opts) do
    access = Keyword.get(opts, :access_key_id)
    secret = Keyword.get(opts, :secret_access_key)

    cond do
      Keyword.get(opts, :unsigned, false) ->
        {:ok, nil}

      access == nil and secret == nil ->
        {:ok, nil}

      true ->
        Credentials.new(
          %{
            access_key_id: access,
            secret_access_key: secret,
            session_token: Keyword.get(opts, :session_token)
          },
          %{provider: :static}
        )
    end
  end
end
