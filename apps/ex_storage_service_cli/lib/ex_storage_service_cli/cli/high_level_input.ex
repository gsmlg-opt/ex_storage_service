defmodule ExStorageServiceCli.CLI.HighLevelInput do
  @moduledoc "High-level S3 flags mapped through the shared modeled input decoder."
  alias ExStorageServiceCli.{CLI.AWSInput, Error}

  @fields [
    acl: "acl",
    sse: "server-side-encryption",
    sse_kms_key_id: "ssekms-key-id",
    storage_class: "storage-class",
    website_redirect: "website-redirect-location",
    content_type: "content-type",
    cache_control: "cache-control",
    content_disposition: "content-disposition",
    content_encoding: "content-encoding",
    content_language: "content-language",
    expires: "expires",
    request_payer: "request-payer",
    metadata: "metadata",
    checksum_algorithm: "checksum-algorithm"
  ]

  def decode(options, opts \\ []) do
    args =
      Enum.flat_map(@fields, fn {key, flag} ->
        if options[key] != nil, do: ["--" <> flag, options[key]], else: []
      end)

    # Frontend configuration and prompting have already been resolved. Internal
    # member decoding must not reopen AWS profiles or prompt a second time.
    args = [
      "--no-cli-auto-prompt",
      "--bucket",
      "input-validation",
      "--key",
      "input-validation" | args
    ]

    opts = Keyword.put(opts, :profile_settings, %{})

    with {:ok, parsed} <- AWSInput.parse("PutObject", args, opts),
         {:ok, customer} <- customer(options[:sse_c], options[:sse_c_key], opts),
         {:ok, source} <-
           customer(options[:sse_c_copy_source], options[:sse_c_copy_source_key], opts),
         {:ok, grants} <- grants(options[:grants] || []) do
      input =
        parsed.input |> Map.drop(["Bucket", "Key"]) |> Map.merge(customer) |> Map.merge(grants)

      input =
        if options[:checksum_mode],
          do: Map.put(input, "ChecksumMode", options[:checksum_mode]),
          else: input

      input =
        if options[:metadata_directive],
          do: Map.put(input, "MetadataDirective", options[:metadata_directive]),
          else: input

      source = Map.merge(source, Map.take(input, ["RequestPayer"]))

      source =
        if options[:version_id],
          do: Map.put(source, "VersionId", options[:version_id]),
          else: source

      {:ok,
       %{
         input: input,
         source_input: source,
         copy_props: options[:copy_props]
       }}
    end
  end

  defp customer(nil, nil, _), do: {:ok, %{}}

  defp customer(algorithm, key, opts) when is_binary(key) do
    with {:ok, bytes} <- key_bytes(key, opts), true <- byte_size(bytes) == 32 do
      {:ok,
       %{
         "SSECustomerAlgorithm" => algorithm || "AES256",
         "SSECustomerKey" => Base.encode64(bytes),
         "SSECustomerKeyMD5" => Base.encode64(:crypto.hash(:md5, bytes))
       }}
    else
      {:error, _} = error -> error
      _ -> failure("SSE-C key must contain exactly 32 raw bytes")
    end
  end

  defp customer(_, _, _), do: failure("SSE-C requires a customer key")

  defp key_bytes("fileb://" <> path, opts), do: read_key(path, opts)
  defp key_bytes("file://" <> path, opts), do: read_key(path, opts)
  defp key_bytes(value, _), do: {:ok, value}

  defp read_key(path, _opts) do
    case File.open(path, [:read, :binary]) do
      {:ok, file} ->
        try do
          case IO.binread(file, 33) do
            data when is_binary(data) and byte_size(data) == 32 -> {:ok, data}
            _ -> failure("SSE-C key file must contain exactly 32 raw bytes")
          end
        after
          File.close(file)
        end

      _ ->
        failure("SSE-C key file must contain exactly 32 raw bytes")
    end
  end

  defp grants(values) do
    Enum.reduce_while(values, {:ok, %{}}, fn value, {:ok, acc} ->
      case String.split(value, "=", parts: 2) do
        [permission, recipients] ->
          field =
            %{
              "read" => "GrantRead",
              "readacl" => "GrantReadACP",
              "writeacl" => "GrantWriteACP",
              "full" => "GrantFullControl"
            }[permission]

          if field != nil and recipients != "" and
               not String.contains?(recipients, ["\r", "\n", <<0>>]) do
            {:cont, {:ok, Map.put(acc, field, recipients)}}
          else
            {:halt,
             failure("Grant syntax must be permission=principal with read|readacl|writeacl|full")}
          end

        _ ->
          {:halt, failure("Grant syntax must include a permission and recipient")}
      end
    end)
  end

  defp failure(message), do: {:error, Error.new(:syntax, message, code: :highlevel_input)}
end
