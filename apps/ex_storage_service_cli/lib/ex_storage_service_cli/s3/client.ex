defmodule ExStorageServiceCli.S3.Client do
  @moduledoc "Shared modeled S3 operations, authentication and raw transport."
  alias ExStorageServiceCli.{Context, Error, Transport.HTTPFetch}
  alias ExStorageServiceCli.Transfer.Coordinator
  alias ExStorageServiceCli.Credentials.DirectorySession
  alias ExStorageServiceCli.CLI.Debug

  alias ExStorageServiceCli.S3.{
    Auth,
    Customizations,
    Decoder,
    Encoder,
    Endpoint,
    Errors,
    RequestChecksum,
    RegionRedirect,
    Response,
    Retry,
    StreamingBody,
    UploadChecksum
  }

  def call(%Context{} = context, operation, input, opts \\ []) do
    with {:ok, input, customizations} <- Customizations.input(operation, input),
         do:
           call_prepared(
             context,
             operation,
             input,
             Keyword.put(opts, :customizations, customizations)
           )
  end

  defp call_prepared(context, operation, input, opts) do
    opts =
      Retry.options(context.options, opts) |> Keyword.put_new(:coordinator, context.coordinator)

    replayable =
      not is_pid(opts[:body]) and
        (opts[:body_source] == nil or opts[:checksum_source] != nil or is_list(opts[:body_source]))

    opts = Keyword.put_new(opts, :replayable, replayable)
    opts = Keyword.put(opts, :region_correction_budget, :atomics.new(1, []))
    cache_key = {context.endpoint, context.addressing, context.region, input["Bucket"]}
    opts = Keyword.put(opts, :region_cache_key, cache_key)
    context = cached_region(context, cache_key, opts)

    Retry.run(operation, opts, fn attempt, remaining, opts ->
      opts = Keyword.put(opts, :attempt, attempt)

      opts =
        if remaining == :infinity,
          do: opts,
          else:
            Keyword.put(opts, :request_deadline, System.monotonic_time(:millisecond) + remaining)

      call_once(cached_region(context, cache_key, opts), operation, input, opts)
    end)
  end

  defp call_once(context, operation, input, opts) do
    with {:ok, opts} <- request_deadline(context, opts),
         :ok <- ExStorageServiceCli.S3.Capabilities.check(context, operation, input),
         {:ok, context} <- Context.refresh(context),
         {:ok, input} <- RequestChecksum.input(context, operation, input),
         {:ok, request} <-
           Encoder.encode(operation, input, allow_streaming: opts[:allow_streaming] || false),
         {:ok, url, endpoint} <- Endpoint.resolve(context, request, input, opts),
         {:ok, context, opts} <- authentication(context, endpoint, input, opts),
         request = %{request | headers: request.headers ++ endpoint.headers},
         request = ExStorageServiceCli.CLI.NativeGlobals.apply_headers(request, context.options),
         request = override_body(request, opts),
         {:ok, request, opts} <- StreamingBody.prepare(request, input, url, context, opts),
         {:ok, request} <- RequestChecksum.prepare(request, input, context, opts),
         {:ok, request} <- Auth.prepare(request, url, context, opts),
         {:ok, lease} <- reserve(context, operation, opts) do
      try do
        result =
          with_body(%{request | lease: lease}, opts, fn request ->
            perform(context, operation, input, url, request, opts)
          end)

        case result do
          {:region_error, error, headers} ->
            Coordinator.release(lease)

            redirect_opts =
              opts
              |> Keyword.put(:discover_region, fn -> discover_region(context, input, opts) end)
              |> Keyword.put(
                :region_corrections,
                max(
                  context.options[:region_corrections] || 0,
                  :atomics.get(opts[:region_correction_budget], 1)
                )
              )

            case RegionRedirect.correct(context, operation, input, error, headers, redirect_opts) do
              {:ok, corrected} ->
                :atomics.put(opts[:region_correction_budget], 1, 1)
                cache_region(corrected, opts)
                call_once(corrected, operation, input, opts)

              :stop ->
                {:error, error}
            end

          {:ok, %Response{body: stream}} when is_pid(stream) ->
            result

          _ ->
            Coordinator.release(lease)
            result
        end
      rescue
        error ->
          Coordinator.release(lease)
          reraise error, __STACKTRACE__
      catch
        kind, reason ->
          Coordinator.release(lease)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end
  end

  defp discover_region(context, input, opts) do
    # Discovery owns a new lease after the original response is drained. It uses
    # the original endpoint and deadline, and cannot recursively redirect.
    discovery_opts =
      opts
      |> Keyword.drop([
        :body,
        :body_source,
        :checksum_source,
        :content_length,
        :payload_hash,
        :streaming,
        :mode
      ])
      |> Keyword.put(:return_response, true)
      |> Keyword.put(:skip_region_correction, true)

    case call_once(
           context,
           "HeadBucket",
           Map.take(input, ["Bucket", "ExpectedBucketOwner", "RequestPayer"]),
           discovery_opts
         ) do
      {:ok, _result, response} ->
        case for {name, value} <- response.headers,
                 String.downcase(name) == "x-amz-bucket-region",
                 do: value do
          [region] -> region
          _ -> nil
        end

      {:error, %Error{} = error} ->
        error.details[:bucket_region]
    end
  end

  defp cached_region(%{frontend: :aws} = context, key, opts) do
    if is_pid(opts[:retry_state]) do
      case Retry.State.region(opts[:retry_state], key) do
        {:ok, region} -> %{context | region: region}
        :error -> context
      end
    else
      context
    end
  end

  defp cached_region(context, _key, _opts), do: context

  defp cache_region(context, opts) do
    if is_pid(opts[:retry_state]),
      do: Retry.State.put_region(opts[:retry_state], opts[:region_cache_key], context.region)

    :ok
  end

  defp request_deadline(context, opts) do
    case opts[:request_deadline] do
      nil ->
        {:ok, opts}

      deadline ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining > 0 do
          timeout = opts[:timeout] || context.timeout

          {:ok,
           Keyword.put(
             opts,
             :timeout,
             if(timeout == :infinity, do: remaining, else: min(timeout, remaining))
           )}
        else
          {:error,
           Error.new(:transport, "Operation exceeded its overall deadline",
             code: :operation_deadline
           )}
        end
    end
  end

  defp reserve(%{coordinator: nil}, _operation, _opts), do: {:ok, nil}

  defp reserve(context, operation, opts),
    do:
      Coordinator.acquire(context.coordinator, opts[:buffer_bytes] || 131_072,
        cleanup: operation == "AbortMultipartUpload"
      )

  defp authentication(
         %{unsigned: false, directory_sessions: manager} = context,
         %{session_auth: :s3express} = endpoint,
         input,
         opts
       )
       when is_pid(manager) do
    with {:ok, credentials} <-
           DirectorySession.resolve(
             manager,
             context,
             input["Bucket"],
             endpoint,
             opts[:session_mode] || "ReadWrite"
           ) do
      context = %{
        context
        | region: endpoint.region,
          credentials: credentials,
          access_key_id: credentials.access_key_id,
          secret_access_key: credentials.secret_access_key,
          session_token: credentials.session_token
      }

      {:ok, context,
       signing_options(endpoint, opts)
       |> Keyword.put(:session_token_header, "x-amz-s3session-token")}
    end
  end

  defp authentication(%{unsigned: false}, %{session_auth: :s3express}, _input, _opts),
    do:
      {:error,
       Error.new(:credentials, "Directory-bucket requests require an invocation-owned session",
         code: :directory_session_required
       )}

  defp authentication(context, endpoint, _input, opts) do
    {:ok, %{context | region: endpoint.region}, signing_options(endpoint, opts)}
  end

  defp signing_options(endpoint, opts) do
    opts
    |> Keyword.put(:service, endpoint.service)
    |> Keyword.put(:auth, endpoint.auth)
    |> Keyword.put(:signing_region_set, endpoint.signing_region_set)
  end

  defp perform(context, operation, _input, url, request, opts) do
    transport_options =
      opts
      |> Keyword.put_new(:ssl, context.tls)
      |> Keyword.put_new(:timeout, context.timeout)
      |> Keyword.put_new(:connect_timeout, context.connect_timeout)
      |> Keyword.put_new(:read_idle_timeout, context.read_idle_timeout)
      |> Keyword.put_new(:write_idle_timeout, context.write_idle_timeout)
      |> Keyword.put_new(:resolve_entries, context.options[:resolve_entries] || %{})
      |> Keyword.put_new(:rate_limiter, context.options[:rate_limiter])
      |> Keyword.put_new(:rate_endpoint, context.endpoint)
      |> Keyword.put_new(:limit_upload, context.options[:limit_upload] || 0)
      |> Keyword.put_new(:limit_download, context.options[:limit_download] || 0)

    Debug.request(context, request, url, opts)

    with {:ok, response} <- HTTPFetch.request(url, request, transport_options) do
      Debug.response(context, request, response, opts)

      cond do
        response.status not in 200..299 ->
          case service_failure(operation, response, context, opts) do
            {:error, %Error{} = error} = result ->
              if opts[:skip_region_correction],
                do: result,
                else: {:region_error, error, response.headers}

            result ->
              result
          end

        opts[:mode] == :stream ->
          {:ok, response}

        true ->
          case Decoder.decode(operation, response) do
            {:error, %Error{code: :service_error}} ->
              {:error, Errors.from_response(operation, response, context)}

            {:ok, result} ->
              result = Customizations.output(operation, result, opts[:customizations])

              with {:ok, result} <- validate_success(operation, result),
                   :ok <- validate_checksums(request, result) do
                if opts[:return_response],
                  do: {:ok, result, Map.take(response, [:status, :headers, :request_id])},
                  else: {:ok, result}
              end

            result ->
              result
          end
      end
    end
  end

  defp with_body(request, opts, function) do
    if Keyword.has_key?(opts, :body_source) do
      with {:ok, bytes} <- StreamingBody.encode(request, opts) do
        case HTTP.Stream.from_enumerable(bytes, telemetry: false) do
          {:ok, stream} ->
            Process.unlink(stream)

            try do
              function.(%{request | body: stream})
            after
              HTTPFetch.close(%Response{body: stream})
            end

          {:error, _} ->
            {:error, Error.new(:transport, "Cannot start upload stream")}
        end
      end
    else
      function.(request)
    end
  end

  defp validate_checksums(%{operation: operation, headers: headers}, result)
       when operation in ["PutObject", "UploadPart"] do
    expected =
      for algorithm <- ExStorageServiceCli.S3.Checksum.algorithms(),
          {name, value} <- headers,
          String.downcase(name) == ExStorageServiceCli.S3.Checksum.header(algorithm),
          into: %{},
          do: {"Checksum" <> algorithm, value}

    with {:ok, _} <- UploadChecksum.verify(expected, result), do: :ok
  end

  defp validate_checksums(_request, _result), do: :ok

  defp validate_success("CopyObject", %{"CopyObjectResult" => %{"ETag" => tag}} = result)
       when is_binary(tag) and tag != "",
       do: {:ok, result}

  defp validate_success("CompleteMultipartUpload", %{"ETag" => tag} = result)
       when is_binary(tag) and tag != "",
       do: {:ok, result}

  defp validate_success(operation, _result)
       when operation in ["CopyObject", "CompleteMultipartUpload"],
       do:
         {:error,
          Error.new(:protocol, "S3 completion response lacks a confirmed object identity",
            operation: operation,
            code: :invalid_completion_result
          )}

  defp validate_success(_operation, result), do: {:ok, result}

  defp override_body(request, opts) do
    request = if Keyword.has_key?(opts, :body), do: %{request | body: opts[:body]}, else: request

    request =
      if opts[:payload_hash], do: %{request | payload_hash: opts[:payload_hash]}, else: request

    if opts[:content_length],
      do: %{
        request
        | headers:
            Enum.reject(request.headers, fn {name, _} ->
              String.downcase(name) == "content-length"
            end) ++ [{"content-length", to_string(opts[:content_length])}]
      },
      else: request
  end

  defp service_failure(operation, %Response{body: body} = response, context, opts)
       when is_pid(body) do
    limit = opts[:max_control_bytes] || 8_388_608

    reducer = fn chunk, {chunks, size} ->
      size = size + byte_size(chunk)

      if size > limit,
        do: {:error, Error.new(:protocol, "Service error response exceeds its size limit")},
        else: {:ok, {[chunk | chunks], size}}
    end

    with {:ok, {chunks, _}, _metadata} <- HTTPFetch.consume(response, {[], 0}, reducer, opts),
         do:
           {:error,
            Errors.from_response(
              operation,
              %{response | body: chunks |> Enum.reverse() |> IO.iodata_to_binary()},
              context
            )}
  end

  defp service_failure(operation, response, context, _opts),
    do: {:error, Errors.from_response(operation, response, context)}
end
