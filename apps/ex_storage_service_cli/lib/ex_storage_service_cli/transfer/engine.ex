defmodule ExStorageServiceCli.Transfer.Engine do
  @moduledoc "Shared bounded upload and verified download effects."
  alias ExStorageServiceCli.{Error, S3.Client, S3.Decoder, Transport.HTTPFetch}
  alias ExStorageServiceCli.S3.UploadChecksum

  alias ExStorageServiceCli.Transfer.{
    Coordinator,
    DownloadChecksum,
    Limits,
    Multipart,
    Planner,
    RemoteSource,
    Sink,
    Source
  }

  def upload_file(context, bucket, key, path, opts \\ []) do
    with {:ok, source} <- Source.file(path, Keyword.put(opts, :coordinator, context.coordinator)),
         do: upload_source(context, bucket, key, source, opts)
  end

  def upload_source(context, bucket, key, source, opts \\ []) do
    with {:ok, plan} <-
           Planner.plan(
             source.length,
             opts[:limits] || Limits.for_context(context),
             Keyword.put_new(opts, :multipart_threshold, 8_388_608)
           ) do
      case plan.mode do
        :multipart -> Multipart.upload(context, bucket, key, source, plan, opts)
        :single -> single_upload(context, bucket, key, source, opts)
      end
    end
  end

  defp single_upload(context, bucket, key, %RemoteSource{} = source, opts) do
    with {:ok, staged} <- Source.range(source, 0, source.length, opts) do
      try do
        single_upload(context, bucket, key, staged, opts)
      after
        Source.cleanup(staged)
      end
    end
  end

  defp single_upload(context, bucket, key, source, opts) do
    input = Map.merge(opts[:input] || %{}, %{"Bucket" => bucket, "Key" => key})

    with :ok <- Source.validate(source),
         {:ok, contract} <- UploadChecksum.select(context, input, :object),
         {:ok, _checksums} <- UploadChecksum.object(contract, source, %{}),
         {:ok, result} <-
           Client.call(
             context,
             "PutObject",
             input,
             Keyword.merge(opts,
               body_source: upload_chunks(source, opts),
               checksum_source: source,
               payload_hash: source.payload_hash,
               content_length: source.length
             )
           ),
         :ok <- Source.validate(source),
         {:ok, checksums} <- UploadChecksum.object(contract, source, result),
         {:ok, verification} <-
           UploadChecksum.verify(
             Map.new(checksums, fn {algorithm, value} -> {"Checksum" <> algorithm, value} end),
             result
           ) do
      {:ok,
       %{
         bytes: source.length,
         etag: String.trim(result["ETag"] || "", "\""),
         result: result,
         checksum: verification
       }}
    end
  end

  def download_file(context, bucket, key, path, opts \\ []) do
    with {:ok, sink} <- Sink.open(path) do
      try do
        with {:ok, result} <-
               download(context, bucket, key, fn bytes -> Sink.write(sink, bytes) end, opts),
             :ok <- Sink.publish(sink) do
          {:ok, result}
        end
      after
        Sink.abort(sink)
      end
    end
  end

  @doc "Streams raw bytes to a sink; the caller owns stdout exclusivity when used with IO.binwrite."
  def download(context, bucket, key, write, opts \\ []) do
    input = Map.merge(opts[:input] || %{}, %{"Bucket" => bucket, "Key" => key})
    operation = opts[:operation] || "GetObject"

    with {:ok, response} <-
           Client.call(context, operation, input, Keyword.put(opts, :mode, :stream)) do
      try do
        with :ok <- selected_download(response, input, opts) do
          cond do
            input["Range"] != nil or input["PartNumber"] != nil ->
              consume_download(response, nil, write, opts)

            DownloadChecksum.composite?(response) and context.coordinator != nil and
                Coordinator.stats(context.coordinator).maximum == 1 ->
              # Release the data request before its metadata preflight under a one-request budget.
              HTTPFetch.close(response)

              with {:ok, digest} <- DownloadChecksum.start(context, input, response, opts),
                   {:ok, pinned} <- pin_download(input, response),
                   {:ok, reopened} <-
                     Client.call(context, "GetObject", pinned, Keyword.put(opts, :mode, :stream)) do
                try do
                  with :ok <- same_response(response, reopened),
                       do: consume_download(reopened, digest, write, opts)
                after
                  HTTPFetch.close(reopened)
                end
              end

            true ->
              with {:ok, digest} <- DownloadChecksum.start(context, input, response, opts),
                   do: consume_download(response, digest, write, opts)
          end
        end
      after
        HTTPFetch.close(response)
      end
    end
  end

  defp consume_download(response, digest, write, opts) do
    with {:ok, result} <- Decoder.decode_headers(opts[:operation] || "GetObject", response),
         {:ok, {digest, _bytes}, metadata} <-
           HTTPFetch.consume(
             response,
             {digest, 0},
             fn chunk, {state, bytes} ->
               with {:ok, updated} <- checksum_update(state, chunk),
                    :ok <- write.(chunk) do
                 total = bytes + byte_size(chunk)
                 progress(opts, total)
                 {:ok, {updated, total}}
               end
             end,
             opts
           ),
         :ok <- length_valid(metadata.bytes, result["ContentLength"], opts[:expected_bytes]),
         {:ok, checksum} <- checksum_finish(digest, response.headers, metadata.trailers) do
      {:ok, %{bytes: metadata.bytes, etag: result["ETag"], result: result, checksum: checksum}}
    end
  end

  # Whole-object and multipart composite checksums describe a different byte
  # sequence from a selected range/part. Length and selection remain verified.
  defp checksum_update(nil, _chunk), do: {:ok, nil}
  defp checksum_update(state, chunk), do: DownloadChecksum.update(state, chunk)
  defp checksum_finish(nil, _headers, _trailers), do: {:ok, :partial_not_verified}

  defp checksum_finish(state, headers, trailers),
    do: DownloadChecksum.finish(state, headers, trailers)

  defp selected_download(response, %{"PartNumber" => part} = input, opts) do
    content = header(response.headers, "content-range") || ""

    with true <- response.status == 206,
         [_, first, last, total] <- Regex.run(~r/^bytes (\d+)-(\d+)\/(\d+)$/, content),
         first = String.to_integer(first),
         last = String.to_integer(last),
         total = String.to_integer(total),
         true <- first <= last and last < total,
         {count, ""} <- Integer.parse(header(response.headers, "x-amz-mp-parts-count") || ""),
         true <- count in 1..10_000 and part <= count,
         true <- opts[:expected_part_range] == nil or content == opts[:expected_part_range],
         true <- opts[:expected_bytes] == nil or opts[:expected_bytes] == last - first + 1,
         true <- header(response.headers, "content-length") == to_string(last - first + 1),
         :ok <- partial_identity(response, input) do
      :ok
    else
      {:error, _} = error -> error
      _ -> failure(:part_mismatch, "S3 response did not confirm the requested multipart part")
    end
  end

  defp selected_download(response, input, opts) do
    with :ok <- range(response, input["Range"]),
         do:
           if(input["Range"] || opts[:verify_selection],
             do: partial_identity(response, input),
             else: :ok
           )
  end

  defp partial_identity(response, input) do
    if (input["VersionId"] == nil or
          header(response.headers, "x-amz-version-id") == input["VersionId"]) and
         (input["IfMatch"] == nil or header(response.headers, "etag") == input["IfMatch"]),
       do: :ok,
       else:
         failure(
           :source_version_mismatch,
           "Provider did not confirm the selected partial object identity"
         )
  end

  @doc "Adds bounded progress notifications to the same replayable upload chunks."
  def upload_chunks(source, opts, offset \\ 0) do
    if is_function(opts[:on_progress], 1) do
      Stream.transform(Source.enumerable(source), offset, fn chunk, bytes ->
        total = bytes + byte_size(chunk)
        progress(opts, total)
        {[chunk], total}
      end)
    else
      Source.enumerable(source)
    end
  end

  defp progress(opts, bytes) do
    if is_function(opts[:on_progress], 1), do: opts[:on_progress].(bytes)
    :ok
  end

  defp pin_download(input, response) do
    case header(response.headers, "etag") do
      etag when is_binary(etag) and etag != "" ->
        input = Map.put(input, "IfMatch", etag)
        version = header(response.headers, "x-amz-version-id")
        {:ok, if(version, do: Map.put(input, "VersionId", version), else: input)}

      _ ->
        failure(
          :checksum_identity_changed,
          "Composite checksum preflight requires an object identity"
        )
    end
  end

  defp same_response(first, next) do
    keys =
      ["etag", "content-length", "x-amz-version-id", "x-amz-checksum-type"] ++
        Enum.map(
          ExStorageServiceCli.S3.Checksum.algorithms(),
          &ExStorageServiceCli.S3.Checksum.header/1
        )

    if next.status == first.status and
         Enum.all?(keys, &(header(first.headers, &1) == header(next.headers, &1))),
       do: :ok,
       else: failure(:checksum_identity_changed, "Object changed during checksum preflight")
  end

  defp length_valid(actual, header, explicit) do
    if (is_integer(header) and actual != header) or (is_integer(explicit) and actual != explicit),
      do: failure(:length_mismatch, "Downloaded object length does not match its response"),
      else: :ok
  end

  defp range(%{status: 200}, nil), do: :ok

  defp range(%{status: 206, headers: headers}, "bytes=" <> requested) do
    content = header(headers, "content-range") || ""

    with [_, start, finish, total] <- Regex.run(~r/^bytes (\d+)-(\d+)\/(\d+)$/, content),
         [_, wanted_start, wanted_end] <- Regex.run(~r/^(\d+)-(\d*)$/, requested),
         start = String.to_integer(start),
         finish = String.to_integer(finish),
         total = String.to_integer(total),
         true <- start == String.to_integer(wanted_start) and finish >= start and finish < total,
         true <- wanted_end == "" or finish == min(String.to_integer(wanted_end), total - 1) do
      :ok
    else
      _ -> failure(:range_mismatch, "S3 response did not honor the requested byte range")
    end
  end

  defp range(_response, _requested),
    do: failure(:range_mismatch, "S3 response did not honor the requested byte range")

  defp header(headers, key),
    do:
      Enum.find_value(headers, fn {name, value} -> if String.downcase(name) == key, do: value end)

  defp failure(code, message), do: {:error, Error.new(:protocol, message, code: code)}
end
