defmodule ExStorageServiceS3.MultipartChecksums do
  @moduledoc false
  import Plug.Conn, only: [get_req_header: 2]
  alias ExStorageServiceS3.Integrity
  alias ExStorageService.{BlobStore.LocalCAS, Context}

  def initiation(conn) do
    algorithm = get_req_header(conn, "x-amz-checksum-algorithm")
    type = get_req_header(conn, "x-amz-checksum-type")

    case {algorithm, type} do
      {[], []} ->
        {:ok, %{}}

      {[name], type} when type == [] or length(type) == 1 ->
        kind = List.first(type) || if(name == "CRC64NVME", do: "FULL_OBJECT", else: "COMPOSITE")

        cond do
          String.downcase(name) not in Integrity.algorithms() ->
            {:error, :unsupported_checksum}

          name != String.upcase(name) ->
            {:error, :unsupported_checksum}

          kind == "COMPOSITE" and name != "CRC64NVME" ->
            {:ok, %{checksum_algorithm: name, checksum_type: kind}}

          kind == "FULL_OBJECT" and name in ["CRC32", "CRC32C", "CRC64NVME"] ->
            {:ok, %{checksum_algorithm: name, checksum_type: kind}}

          true ->
            {:error, :unsupported_checksum_type}
        end

      _ ->
        {:error, :unsupported_checksum_type}
    end
  end

  def read_options(upload) do
    case get_in(upload, [:attributes, :checksum_algorithm]) do
      nil -> []
      algorithm -> [calculate_checksums: [String.downcase(algorithm)]]
    end
  end

  def part_attributes(state), do: %{checksums: Map.get(state, :checksums, %{})}

  def response_headers(conn, checksums) do
    Enum.reduce(checksums, conn, fn
      {"md5", _}, current ->
        current

      {algorithm, value}, current ->
        Plug.Conn.put_resp_header(current, "x-amz-checksum-" <> algorithm, value)
    end)
  end

  def validate_parts(prepared, declarations) do
    stored =
      Map.new(
        get_in(prepared, [:upload, :completion, :part_checksums]) || [],
        fn part -> {part.part_number, part.checksums} end
      )

    algorithm = get_in(prepared, [:upload, :attributes, :checksum_algorithm])
    type = get_in(prepared, [:upload, :attributes, :checksum_type])

    if Enum.all?(declarations, fn {number, checksums} ->
         actual = Map.get(stored, number, %{})

         Enum.all?(checksums, fn {name, value} -> Map.get(actual, name) == value end) and
           (algorithm == nil or type == "FULL_OBJECT" or
              Map.has_key?(checksums, String.downcase(algorithm)))
       end),
       do: :ok,
       else: {:error, :bad_digest}
  end

  def complete(conn, prepared) do
    attributes = Map.get(prepared.upload, :attributes, %{})
    algorithm = Map.get(attributes, :checksum_algorithm)
    type = Map.get(attributes, :checksum_type)
    requested_type = get_req_header(conn, "x-amz-checksum-type")

    with true <- requested_type in [[], [type]],
         {:ok, checksums} <- calculated(prepared, algorithm, type, conn),
         :ok <- verify_headers(conn, checksums, algorithm) do
      {:ok, checksums}
    else
      false -> {:error, :unsupported_checksum_type}
      error -> error
    end
  end

  defp calculated(prepared, nil, _type, conn) do
    names =
      for name <- Integrity.algorithms(),
          get_req_header(conn, "x-amz-checksum-" <> name) != [],
          do: name

    if names == [], do: {:ok, %{}}, else: checksum_file(prepared, names)
  end

  defp calculated(prepared, algorithm, "FULL_OBJECT", _conn),
    do: checksum_file(prepared, [String.downcase(algorithm)])

  defp calculated(prepared, algorithm, "COMPOSITE", _conn) do
    name = String.downcase(algorithm)
    parts = get_in(prepared, [:upload, :completion, :part_checksums]) || []

    with {:ok, bytes} <- composite_bytes(parts, name),
         {:ok, state} <- calculator([name], byte_size(bytes)),
         {:ok, state} <- Integrity.update(state, bytes),
         {:ok, %{^name => value}} <- Integrity.finish(state) do
      {:ok, %{name => value <> "-#{length(parts)}"}}
    end
  end

  defp composite_bytes(parts, name) do
    Enum.reduce_while(parts, {:ok, []}, fn part, {:ok, acc} ->
      case Base.decode64(Map.get(part.checksums, name, "")) do
        {:ok, bytes} when byte_size(bytes) > 0 -> {:cont, {:ok, [bytes | acc]}}
        _ -> {:halt, {:error, :bad_digest}}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}
      error -> error
    end
  end

  def checksum_file(prepared, names) do
    with {:ok, context} <- Context.default() do
      path = LocalCAS.blob_path(prepared.content_hash, Context.blob_store_options(context))
      checksum_path(path, prepared.size, names)
    end
  end

  def checksum_source(_source, []), do: {:ok, %{}}

  def checksum_source(source, names) do
    alias ExStorageService.BlobStore.Source

    with {:ok, state} <- calculator(names, Source.content_length(source)),
         {:ok, final} <-
           Source.reduce(source, state, fn bytes, current ->
             case Integrity.update(current, bytes) do
               {:ok, next} -> {:cont, next}
               {:error, reason} -> {:halt, reason, current}
             end
           end) do
      Integrity.finish(final)
    else
      {:error, reason, _} -> {:error, reason}
      error -> error
    end
  end

  def checksum_path(_path, _size, []), do: {:ok, %{}}

  def checksum_path(path, size, names) do
    with {:ok, state} <- calculator(names, size) do
      File.stream!(path, [], 262_144)
      |> Enum.reduce_while({:ok, state}, fn bytes, {:ok, current} ->
        case Integrity.update(current, bytes) do
          {:ok, next} -> {:cont, {:ok, next}}
          error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, final} -> Integrity.finish(final)
        error -> error
      end
    end
  rescue
    File.Error -> {:error, :blob_not_found}
  end

  defp calculator(names, size) do
    conn = %Plug.Conn{req_headers: [{"content-length", to_string(size)}]}
    Integrity.new(conn, size, calculate_checksums: names)
  end

  defp verify_headers(conn, checksums, algorithm) do
    headers =
      Enum.filter(conn.req_headers, fn {name, _} ->
        String.starts_with?(name, "x-amz-checksum-") and name != "x-amz-checksum-type"
      end)

    sdk = get_req_header(conn, "x-amz-sdk-checksum-algorithm")

    cond do
      sdk != [] and sdk != [algorithm] ->
        {:error, :unsupported_checksum}

      Enum.any?(headers, fn {header, _} ->
        String.replace_prefix(header, "x-amz-checksum-", "") not in Integrity.algorithms()
      end) ->
        {:error, :unsupported_checksum}

      length(Enum.uniq_by(headers, &elem(&1, 0))) != length(headers) ->
        {:error, :invalid_digest}

      Enum.any?(headers, fn {header, value} ->
        Map.get(checksums, String.replace_prefix(header, "x-amz-checksum-", "")) != value
      end) ->
        {:error, :bad_digest}

      algorithm == nil ->
        :ok

      true ->
        :ok
    end
  end
end
