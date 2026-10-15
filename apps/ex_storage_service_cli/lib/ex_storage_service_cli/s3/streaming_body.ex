defmodule ExStorageServiceCli.S3.StreamingBody do
  @moduledoc "Selects and frames replayable aws-chunked request bodies after fresh authentication."
  alias ExStorageServiceCli.{Error, S3.Checksum, S3.OperationCatalog, S3.StreamingSignature}

  def prepare(request, input, url, context, opts) do
    {:ok, model} = OperationCatalog.operation(request.operation)
    traits = model["httpChecksum"] || %{}
    member = traits["requestAlgorithmMember"]
    algorithm = if member, do: input[member]

    algorithm =
      algorithm ||
        if(member && context.options[:request_checksum_calculation] != "when_required",
          do: "CRC64NVME"
        )

    enabled = streaming_input?(model) and algorithm != nil and not supplied?(request.headers)
    automatic = enabled and context.frontend == :aws and URI.parse(url).scheme == "https"
    mode = opts[:streaming] || if(automatic, do: :unsigned)

    if mode do
      length = opts[:content_length] || if(is_binary(request.body), do: byte_size(request.body))
      source = opts[:body_source] || if(is_binary(request.body), do: binary_chunks(request.body))

      spec = [
        mode: mode,
        auth: opts[:auth],
        checksum_algorithm: algorithm,
        decoded_length: length,
        now: opts[:now] || DateTime.utc_now(),
        region: context.region,
        service: opts[:service] || "s3",
        access_key_id: context.access_key_id,
        secret_access_key: context.secret_access_key,
        content_encoding: header(request.headers, "content-encoding")
      ]

      with true <- source != nil,
           {:ok, headers} <- StreamingSignature.headers(length, spec) do
        names = [
          "x-amz-sdk-checksum-algorithm"
          | Enum.map(headers, &elem(&1, 0)) ++ Enum.map(Checksum.algorithms(), &Checksum.header/1)
        ]

        headers =
          Enum.reject(request.headers, fn {name, _} -> String.downcase(name) in names end) ++
            headers

        headers =
          if algorithm,
            do: headers ++ [{"x-amz-sdk-checksum-algorithm", algorithm}],
            else: headers

        {:ok, %{request | headers: headers, payload_hash: StreamingSignature.payload_hash(spec)},
         opts
         |> Keyword.put(:streaming_spec, spec)
         |> Keyword.put(:body_source, source)
         |> Keyword.put(:now, spec[:now])}
      else
        false ->
          {:error,
           Error.new(:client, "Streaming signing requires a replayable byte source",
             code: :streaming_source_required
           )}

        error ->
          error
      end
    else
      {:ok, request, opts}
    end
  end

  def encode(request, opts) do
    case opts[:streaming_spec] do
      nil ->
        {:ok, opts[:body_source]}

      spec ->
        with {:ok, seed} <- seed(spec, request.headers),
             do:
               StreamingSignature.encode(
                 opts[:body_source],
                 Keyword.put(spec, :seed_signature, seed)
               )
    end
  end

  defp seed(spec, headers) do
    if spec[:mode] == :unsigned,
      do: {:ok, nil},
      else: StreamingSignature.seed(header(headers, "authorization"))
  end

  defp binary_chunks(bytes),
    do:
      Stream.unfold(bytes, fn
        "" ->
          nil

        rest ->
          length = min(byte_size(rest), 65_536)
          <<chunk::binary-size(length), tail::binary>> = rest
          {chunk, tail}
      end)

  defp streaming_input?(%{"input" => %{"shape" => shape}}) do
    input = OperationCatalog.shape(shape)
    binding = (input["members"] || %{})[input["payload"]]

    not is_nil(binding) and
      (binding["streaming"] == true or
         OperationCatalog.shape(binding["shape"])["streaming"] == true)
  end

  defp streaming_input?(_), do: false

  defp supplied?(headers),
    do:
      Enum.any?(headers, fn {name, _} ->
        String.downcase(name) in [
          "content-md5" | Enum.map(Checksum.algorithms(), &Checksum.header/1)
        ]
      end)

  defp header(headers, key),
    do:
      Enum.find_value(headers, fn {name, value} -> if String.downcase(name) == key, do: value end)
end
