defmodule ExStorageServiceCli.Transport.HTTPFetch do
  @moduledoc "Owned raw-byte HTTP streams, acknowledged reads and bounded control bodies."
  alias ExStorageServiceCli.{Error, S3.Request, S3.Response}
  alias ExStorageServiceCli.Transfer.{Coordinator, PartGroup, RateLimiter}
  alias ExStorageServiceCli.Transport.Cancellation

  def request(url, %Request{} = request, opts \\ []) do
    with {:ok, opts} <- ExStorageServiceCli.Transport.Resolution.options(url, opts),
         {:ok, opts} <- rate_options(url, opts),
         {:ok, cancellation, controller} <-
           Cancellation.start(self(), opts[:signal], request.lease) do
      case with :ok <- PartGroup.register(opts[:part_group], cancellation),
                do: Coordinator.track(request.lease, controller) do
        :ok ->
          perform(url, request, cancellation, controller, opts)

        {:error, _} = error ->
          Cancellation.close(cancellation)
          error
      end
    else
      {:error, _} = error ->
        Coordinator.release(request.lease)

        case error do
          {:error, %Error{}} -> error
          {:error, reason} -> {:error, transport_error(reason)}
        end
    end
  end

  defp rate_options(url, opts) do
    with {:ok, upload} <- RateLimiter.parse_rate(opts[:limit_upload] || 0),
         {:ok, download} <- RateLimiter.parse_rate(opts[:limit_download] || 0),
         true <- (upload == 0 and download == 0) or is_pid(opts[:rate_limiter]) do
      endpoint = RateLimiter.endpoint(opts[:rate_endpoint] || url)
      endpoint = if opts[:rate_endpoint], do: endpoint, else: put_elem(endpoint, 3, "")

      {:ok,
       opts
       |> Keyword.put(:limit_upload, upload)
       |> Keyword.put(:limit_download, download)
       |> Keyword.put(:rate_endpoint, endpoint)}
    else
      false ->
        {:error,
         Error.new(:configuration, "Rate limits require an invocation-owned limiter",
           code: :rate_owner_required
         )}

      error ->
        error
    end
  end

  defp perform(url, request, cancellation, controller, opts) do
    limit = rate_limit(:upload, controller, opts)
    request = finite_body_length(request, limit)

    case RateLimiter.upload(request.body, limit) do
      {:ok, body, upload} ->
        try do
          perform_body(url, %{request | body: body}, cancellation, controller, opts)
        after
          RateLimiter.close_upload(upload)
        end

      error ->
        Cancellation.close(cancellation)
        error
    end
  end

  # Binary bodies normally receive this derived header inside http_fetch. Keep
  # that same framing when pacing turns the binary into an acknowledged stream.
  defp finite_body_length(%Request{body: body} = request, limit)
       when is_binary(body) and not is_nil(limit) do
    if Enum.any?(request.headers, fn {name, _} -> String.downcase(name) == "content-length" end),
      do: request,
      else: %{
        request
        | headers: request.headers ++ [{"content-length", to_string(byte_size(body))}]
      }
  end

  defp finite_body_length(request, _), do: request

  defp rate_limit(direction, controller, opts) do
    rate = opts[if(direction == :upload, do: :limit_upload, else: :limit_download)] || 0

    if rate > 0 do
      deadline =
        opts[:request_deadline] ||
          request_deadline(opts[:timeout] || 60_000)

      %{
        pid: opts[:rate_limiter],
        endpoint: opts[:rate_endpoint],
        direction: direction,
        rate: rate,
        opts: [signal: controller, deadline: deadline]
      }
    end
  end

  defp request_deadline(:infinity), do: nil
  defp request_deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp perform_body(url, request, cancellation, controller, opts) do
    response_options = [
      method: request.method,
      headers: request.headers,
      body: request.body,
      duplex: if(is_pid(request.body), do: :half, else: nil),
      signal: controller,
      redirect: :manual,
      decode_body: false,
      stream_response: true,
      response_stream_idle_timeout: :infinity,
      response_stream_ack_timeout: :infinity,
      timeout: opts[:timeout] || 60_000,
      connect_timeout: opts[:connect_timeout] || 60_000,
      read_idle_timeout: opts[:read_idle_timeout],
      write_idle_timeout: opts[:write_idle_timeout],
      connect_address: opts[:connect_address],
      ssl: opts[:ssl] || [],
      http_version: opts[:http_version] || :http1
    ]

    case fetch(cancellation, url, response_options) do
      %HTTP.Response{} = raw ->
        response = %Response{
          status: raw.status,
          headers: HTTP.Headers.to_list(raw.headers),
          body: HTTP.Response.stream_pid(raw) || raw.body || "",
          controller: controller,
          cancellation: cancellation,
          lease: request.lease,
          rate_limit: rate_limit(:download, controller, opts),
          timeout: opts[:timeout] || 60_000,
          request_id: HTTP.Headers.get(raw.headers, "x-amz-request-id")
        }

        if opts[:mode] == :stream and request.method != "HEAD" do
          {:ok, response}
        else
          control(response, request.method, opts)
        end

      {:error, reason} ->
        Cancellation.close(cancellation)

        {:error, transport_error(reason)}
    end
  end

  defp fetch(cancellation, url, options) do
    Cancellation.fetch(cancellation, url, options)
  rescue
    _error -> {:error, :invalid_request}
  catch
    :exit, _reason -> {:error, :transport_failure}
  end

  @doc "Consumes a response with backpressure and always releases owned resources."
  def consume(%Response{} = response, initial, reducer, opts \\ []) do
    case adopt(response.cancellation) do
      :ok ->
        consume_owned(response, initial, reducer, opts)

      error ->
        close(response)
        error
    end
  end

  defp adopt(nil), do: :ok

  defp adopt(cancellation) do
    case Cancellation.adopt(cancellation) do
      :ok -> :ok
      {:error, reason} -> {:error, transport_error(reason)}
    end
  catch
    :exit, _ ->
      {:error, Error.new(:interrupted, "Response ownership has ended", code: :request_closed)}
  end

  defp consume_owned(response, initial, reducer, opts) do
    reducer = fn bytes, state ->
      RateLimiter.reduce(bytes, response.rate_limit, state, reducer)
    end

    if is_pid(response.body) do
      monitor = Process.monitor(response.body)
      send(response.body, {:read_chunk, self(), :ack})

      try do
        receive_chunks(
          response.body,
          monitor,
          initial,
          reducer,
          0,
          [],
          opts[:timeout] || response.timeout || 60_000
        )
      after
        Process.demonitor(monitor, [:flush])
        close(response)
      end
    else
      try do
        body = response.body || ""

        with {:ok, state} <- reducer.(body, initial),
             do: {:ok, state, %{bytes: byte_size(body), trailers: response.trailers}}
      after
        close(response)
      end
    end
  end

  @doc "Aborts an unconsumed response and closes only its owned controller."
  def close(%Response{cancellation: cancellation, body: body}) when is_pid(cancellation) do
    Cancellation.close(cancellation)
    flush(body)
  end

  def close(%Response{} = response) do
    Coordinator.release(response.lease)
    stream = response.body
    monitor = if is_pid(stream), do: Process.monitor(stream)
    if is_pid(stream) and Process.alive?(stream), do: send(stream, {:read_chunk, self(), :ack})
    controller = response.controller

    if response.owns_controller and is_pid(controller) and Process.alive?(controller) do
      HTTP.AbortController.abort(controller)
      Agent.stop(controller)
    end

    if monitor do
      deadline = System.monotonic_time(:millisecond) + 1000
      drain(stream, monitor, deadline)
      Process.demonitor(monitor, [:flush])
    end

    :ok
  catch
    :exit, _reason -> :ok
  end

  defp flush(stream) when is_pid(stream) do
    receive do
      {:stream_chunk, ^stream, _, _} -> flush(stream)
      {:stream_chunk, ^stream, _} -> flush(stream)
      {:stream_error, ^stream, _} -> flush(stream)
      {:stream_end, ^stream} -> flush(stream)
      {:stream_trailers, ^stream, _} -> flush(stream)
    after
      0 -> :ok
    end
  end

  defp flush(_), do: :ok

  defp control(response, method, opts) do
    limit = opts[:max_control_bytes] || 8_388_608
    length = response.headers |> HTTP.Headers.new() |> HTTP.Headers.get("content-length")

    oversized? =
      case Integer.parse(length || "") do
        {size, ""} -> method != "HEAD" and size > limit
        _ -> false
      end

    if oversized? do
      close(response)

      {:error,
       Error.new(:protocol, "Control response exceeds its size limit", code: :control_limit)}
    else
      reducer = fn chunk, {chunks, total} ->
        total = total + byte_size(chunk)

        if total > limit,
          do:
            {:error,
             Error.new(:protocol, "Control response exceeds its size limit", code: :control_limit)},
          else: {:ok, {[chunk | chunks], total}}
      end

      with {:ok, {chunks, _total}, metadata} <- consume(response, {[], 0}, reducer, opts) do
        {:ok,
         %{
           response
           | body: chunks |> Enum.reverse() |> IO.iodata_to_binary(),
             trailers: metadata.trailers,
             controller: nil,
             cancellation: nil,
             lease: nil,
             rate_limit: nil
         }}
      end
    end
  end

  defp receive_chunks(stream, monitor, state, reducer, bytes, trailers, timeout) do
    receive do
      {:stream_chunk, ^stream, chunk, reference} ->
        case reducer.(chunk, state) do
          {:ok, updated} ->
            send(stream, {:stream_chunk_ack, reference})

            receive_chunks(
              stream,
              monitor,
              updated,
              reducer,
              bytes + byte_size(chunk),
              trailers,
              timeout
            )

          {:error, %Error{}} = error ->
            error
        end

      {:stream_trailers, ^stream, fields} ->
        receive_chunks(stream, monitor, state, reducer, bytes, fields, timeout)

      {:stream_end, ^stream} ->
        {:ok, state, %{bytes: bytes, trailers: trailers}}

      {:stream_error, ^stream, reason} ->
        {:error, transport_error(reason)}

      {:DOWN, ^monitor, :process, ^stream, reason} ->
        {:error, transport_error({:stream_closed, reason})}
    after
      timeout -> {:error, transport_error(:request_timeout)}
    end
  end

  defp drain(stream, monitor, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:DOWN, ^monitor, :process, ^stream, _} ->
        :ok

      {:stream_chunk, ^stream, _chunk, reference} ->
        send(stream, {:stream_chunk_ack, reference})
        drain(stream, monitor, deadline)

      {:stream_chunk, ^stream, _chunk} ->
        drain(stream, monitor, deadline)

      {:stream_error, ^stream, _reason} ->
        drain(stream, monitor, deadline)

      {:stream_end, ^stream} ->
        drain(stream, monitor, deadline)

      {:stream_trailers, ^stream, _fields} ->
        drain(stream, monitor, deadline)
    after
      remaining -> if Process.alive?(stream), do: Process.exit(stream, :kill)
    end
  end

  defp transport_error(reason) do
    category = if reason == :aborted, do: :interrupted, else: :transport

    Error.new(category, "Storage transport failed",
      code: safe_code(reason),
      retryable:
        reason in [
          :request_timeout,
          :read_idle_timeout,
          :write_idle_timeout,
          :timeout,
          :closed,
          :econnrefused
        ]
    )
  end

  defp safe_code(reason) when is_atom(reason), do: reason
  defp safe_code(_reason), do: :transport_failure
end
