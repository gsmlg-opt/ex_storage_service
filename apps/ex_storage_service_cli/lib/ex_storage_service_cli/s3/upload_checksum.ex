defmodule ExStorageServiceCli.S3.UploadChecksum do
  @moduledoc "Serializable upload checksum contracts, locally retained part digests, and typed multipart completion."
  alias ExStorageServiceCli.{Error, Transfer.Source}
  alias ExStorageServiceCli.S3.Checksum
  @algorithms ~w(CRC32 CRC32C CRC64NVME SHA1 SHA256)
  @crc ~w(CRC32 CRC32C CRC64NVME)

  @doc "Selects a JSON-safe checksum contract; multipart CRC64NVME defaults to FULL_OBJECT."
  def select(context, input, mode \\ :multipart) when mode in [:multipart, :object, :copy] do
    supplied = Enum.filter(@algorithms, &Map.has_key?(input, "Checksum" <> &1))

    algorithm =
      input["ChecksumAlgorithm"] || List.first(supplied) ||
        if(mode != :copy and context.options[:request_checksum_calculation] != "when_required",
          do: "CRC64NVME"
        )

    cond do
      length(supplied) > 1 ->
        failure(:ambiguous_upload_checksum, "Choose one upload checksum algorithm")

      algorithm == nil and input["ChecksumType"] != nil ->
        failure(:upload_checksum_type, "Checksum type requires an algorithm")

      algorithm == nil ->
        {:ok, nil}

      algorithm not in @algorithms ->
        failure(:unsupported_upload_checksum, "Unsupported upload checksum algorithm")

      supplied != [] and hd(supplied) != algorithm ->
        failure(
          :upload_checksum_algorithm,
          "Explicit checksum does not match its selected algorithm"
        )

      true ->
        type =
          input["ChecksumType"] ||
            if(mode == :object or supplied != [] or algorithm == "CRC64NVME",
              do: "FULL_OBJECT",
              else: "COMPOSITE"
            )

        with :ok <- valid_type(algorithm, type, if(mode == :copy, do: :multipart, else: mode)),
             :ok <- valid_expected(input["Checksum" <> algorithm], algorithm) do
          {:ok,
           %{
             "algorithm" => algorithm,
             "type" => type,
             "expected" => input["Checksum" <> algorithm]
           }}
        end
    end
  end

  @doc "Retains a selected server-copy digest without downloading or claiming to hash the source bytes."
  def copied_part(nil, _response), do: {:ok, %{}}

  def copied_part(contract, response) do
    algorithm = contract["algorithm"]

    case response["Checksum" <> algorithm] do
      nil ->
        failure(:missing_copy_checksum, "Server-copy part lacks the selected checksum")

      digest ->
        with :ok <- valid_expected(digest, algorithm), do: {:ok, %{algorithm => digest}}
    end
  end

  @doc "Builds initiation fields without incorrectly placing an object checksum on CreateMultipartUpload."
  def initiation(nil, input), do: input

  def initiation(contract, input) do
    input
    |> Map.drop(Enum.map(@algorithms, &("Checksum" <> &1)))
    |> Map.put("ChecksumAlgorithm", contract["algorithm"])
    |> Map.put("ChecksumType", contract["type"])
  end

  @doc "Rejects a provider's conflicting algorithm or checksum type without losing local selection."
  def acknowledge(nil, _response), do: :ok

  def acknowledge(contract, response) do
    cond do
      response["ChecksumAlgorithm"] != nil and
          response["ChecksumAlgorithm"] != contract["algorithm"] ->
        failure(:upload_checksum_algorithm, "Provider changed the selected checksum algorithm")

      response["ChecksumType"] != nil and response["ChecksumType"] != contract["type"] ->
        failure(:upload_checksum_type, "Provider changed the selected checksum type")

      true ->
        :ok
    end
  end

  @doc "Hashes bounded Source chunks once for selected and returned algorithms, retaining local checksums even when absent remotely."
  def part(contract, %Source{} = source, response \\ %{}) do
    selected = if contract, do: [contract["algorithm"]], else: []
    returned = Enum.filter(@algorithms, &Map.has_key?(response, "Checksum" <> &1))
    algorithms = Enum.uniq(selected ++ returned)

    initial =
      Map.new(algorithms, fn algorithm -> {algorithm, elem(Checksum.new(algorithm), 1)} end)

    with {:ok, states} <-
           Source.reduce(source, initial, fn bytes, states ->
             Map.new(states, fn {algorithm, state} ->
               {algorithm, Checksum.update(state, bytes)}
             end)
           end) do
      checksums =
        Map.new(states, fn {algorithm, state} -> {algorithm, Checksum.encoded(state)} end)

      expected =
        Map.new(checksums, fn {algorithm, digest} -> {"Checksum" <> algorithm, digest} end)

      with {:ok, _} <- verify(expected, response), do: {:ok, checksums}
    end
  end

  @doc "Verifies single-PUT bytes, any explicit caller checksum, and provider-returned checksums."
  def object(contract, source, response) do
    with :ok <- acknowledge_object(contract, response),
         {:ok, checksums} <- part(contract, source, response),
         :ok <- requested(contract, checksums) do
      {:ok, checksums}
    end
  end

  @doc "Returns modeled completion fields and separately the expected provider checksum response."
  def completion(nil, parts) do
    with {:ok, parts, total} <- ordered_parts(parts, nil) do
      {:ok, %{"MultipartUpload" => %{"Parts" => manifest(parts, nil)}, "MpuObjectSize" => total},
       %{}}
    end
  end

  def completion(contract, parts) do
    algorithm = contract["algorithm"]
    type = contract["type"]

    with :ok <- valid_type(algorithm, type, :multipart),
         {:ok, parts, total} <- ordered_parts(parts, algorithm),
         {:ok, checksum} <- combined(algorithm, type, parts),
         :ok <- requested(contract, %{algorithm => checksum}) do
      fields = %{
        "MultipartUpload" => %{"Parts" => manifest(parts, algorithm)},
        "ChecksumType" => type,
        "MpuObjectSize" => total
      }

      fields =
        if type == "FULL_OBJECT",
          do: Map.put(fields, "Checksum" <> algorithm, checksum),
          else: fields

      expected = %{("Checksum" <> algorithm) => checksum, "ChecksumType" => type}
      {:ok, fields, expected}
    end
  end

  @doc "Validates every expected digest that is returned; absence never claims verification."
  def verify(expected, response) when is_map(expected) and is_map(response) do
    keys = Enum.filter(Enum.map(@algorithms, &("Checksum" <> &1)), &Map.has_key?(expected, &1))
    present = Enum.filter(keys, &Map.has_key?(response, &1))

    cond do
      expected["ChecksumType"] != nil and response["ChecksumType"] != nil and
          expected["ChecksumType"] != response["ChecksumType"] ->
        failure(:upload_checksum_type, "Upload response checksum type does not match")

      Enum.any?(present, fn key -> not equal?(expected[key], response[key]) end) ->
        failure(:upload_checksum_mismatch, "Upload response checksum does not match local bytes")

      present == [] ->
        {:ok, :not_provided}

      length(present) != length(keys) ->
        {:ok, :partially_verified}

      expected["ChecksumType"] != nil and response["ChecksumType"] == nil ->
        {:ok, :digest_verified}

      true ->
        {:ok, :verified}
    end
  end

  defp acknowledge_object(nil, _), do: :ok

  defp acknowledge_object(contract, response),
    do: acknowledge(%{contract | "type" => "FULL_OBJECT"}, response)

  defp requested(nil, _checksums), do: :ok
  defp requested(%{"expected" => nil}, _checksums), do: :ok

  defp requested(contract, checksums) do
    if equal?(contract["expected"], checksums[contract["algorithm"]]),
      do: :ok,
      else: failure(:upload_checksum_mismatch, "Caller checksum does not match local bytes")
  end

  defp equal?(a, b) when is_binary(a) and is_binary(b) and byte_size(a) == byte_size(b),
    do: :crypto.hash_equals(a, b)

  defp equal?(_, _), do: false
  defp valid_expected(nil, _), do: :ok

  defp valid_expected(value, algorithm) when is_binary(value) do
    sizes = %{"CRC32" => 4, "CRC32C" => 4, "CRC64NVME" => 8, "SHA1" => 20, "SHA256" => 32}
    size = Map.fetch!(sizes, algorithm)

    case Base.decode64(value) do
      {:ok, bytes} when byte_size(bytes) == size ->
        :ok

      _ ->
        failure(
          :invalid_upload_checksum,
          "Explicit upload checksum is not a valid algorithm digest"
        )
    end
  end

  defp valid_expected(_, _),
    do: failure(:invalid_upload_checksum, "Explicit upload checksum must be base64 text")

  defp valid_type(algorithm, type, mode) do
    cond do
      algorithm not in @algorithms ->
        failure(:unsupported_upload_checksum, "Unsupported upload checksum algorithm")

      type not in ["FULL_OBJECT", "COMPOSITE"] ->
        failure(:upload_checksum_type, "Unsupported upload checksum type")

      mode == :object and type != "FULL_OBJECT" ->
        failure(:upload_checksum_type, "Single uploads require FULL_OBJECT checksums")

      algorithm == "CRC64NVME" and type != "FULL_OBJECT" ->
        failure(:upload_checksum_type, "CRC64NVME requires FULL_OBJECT")

      mode == :multipart and type == "FULL_OBJECT" and algorithm not in @crc ->
        failure(:upload_checksum_type, "Multipart FULL_OBJECT supports CRC algorithms only")

      true ->
        :ok
    end
  end

  defp ordered_parts(parts, algorithm) when is_list(parts) and length(parts) in 1..10_000 do
    parts = Enum.sort_by(parts, & &1["number"])

    valid =
      Enum.with_index(parts, 1)
      |> Enum.all?(fn {part, index} ->
        part["number"] == index and is_integer(part["length"]) and part["length"] >= 0 and
          is_binary(part["etag"]) and part["etag"] != "" and
          (algorithm == nil or is_binary((part["checksums"] || %{})[algorithm]))
      end)

    total = if valid, do: Enum.reduce(parts, 0, &(&1["length"] + &2)), else: 0

    if valid and total <= 0x7FFFFFFFFFFFFFFF,
      do: {:ok, parts, total},
      else:
        failure(
          :upload_checksum_parts,
          "Completion requires consecutive parts with the selected checksums"
        )
  end

  defp ordered_parts(_, _),
    do: failure(:upload_checksum_parts, "Completion requires one to 10000 parts")

  defp manifest(parts, algorithm) do
    Enum.map(parts, fn part ->
      fields = %{"PartNumber" => part["number"], "ETag" => part["etag"]}

      if algorithm,
        do: Map.put(fields, "Checksum" <> algorithm, part["checksums"][algorithm]),
        else: fields
    end)
  end

  defp combined(algorithm, "FULL_OBJECT", parts),
    do: Checksum.combine(algorithm, Enum.map(parts, &{&1["checksums"][algorithm], &1["length"]}))

  defp combined(algorithm, "COMPOSITE", parts),
    do: Checksum.composite(algorithm, Enum.map(parts, & &1["checksums"][algorithm]))

  defp failure(code, message), do: {:error, Error.new(:protocol, message, code: code)}
end
