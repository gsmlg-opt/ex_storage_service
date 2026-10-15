defmodule ExStorageServiceCli.S3.Checksum do
  @moduledoc "Incremental raw-byte S3 checksums, multipart composites and bounded checksum trailers."
  import Bitwise
  alias ExStorageServiceCli.Error
  defstruct [:algorithm, :value]

  @sizes %{"CRC32" => 4, "CRC32C" => 4, "CRC64NVME" => 8, "SHA1" => 20, "SHA256" => 32}
  @tables Map.new([{0x82F63B78, 32}, {0x9A6C9329AC4BC9B5, 64}], fn {polynomial, width} ->
            table =
              for byte <- 0..255 do
                Enum.reduce(1..8, byte, fn _, value ->
                  if band(value, 1) == 1, do: bxor(bsr(value, 1), polynomial), else: bsr(value, 1)
                end)
              end
              |> List.to_tuple()

            {width, table}
          end)

  def algorithms, do: ~w(CRC32 CRC32C CRC64NVME SHA1 SHA256)

  def new(algorithm) when algorithm in ["SHA1", "SHA256"] do
    hash = if algorithm == "SHA1", do: :sha, else: :sha256
    {:ok, %__MODULE__{algorithm: algorithm, value: :crypto.hash_init(hash)}}
  end

  def new(algorithm) when algorithm in ["CRC32", "CRC32C", "CRC64NVME"],
    do: {:ok, %__MODULE__{algorithm: algorithm, value: 0}}

  def new(_algorithm), do: failure(:unsupported_checksum)

  def update(%__MODULE__{algorithm: algorithm, value: value} = state, chunk)
      when is_binary(chunk) and algorithm in ["SHA1", "SHA256"],
      do: %{state | value: :crypto.hash_update(value, chunk)}

  def update(%__MODULE__{algorithm: "CRC32", value: value} = state, chunk) when is_binary(chunk),
    do: %{state | value: :erlang.crc32(value, chunk)}

  def update(%__MODULE__{algorithm: algorithm, value: value} = state, chunk)
      when is_binary(chunk) do
    width = if algorithm == "CRC32C", do: 32, else: 64
    mask = (1 <<< width) - 1

    crc =
      for <<byte <- chunk>>, reduce: bxor(value, mask) do
        crc -> bxor(bsr(crc, 8), elem(@tables[width], band(bxor(crc, byte), 255)))
      end

    %{state | value: bxor(crc, mask)}
  end

  def finish(%__MODULE__{algorithm: algorithm, value: value})
      when algorithm in ["SHA1", "SHA256"],
      do: :crypto.hash_final(value)

  def finish(%__MODULE__{algorithm: "CRC64NVME", value: value}), do: <<value::64>>
  def finish(%__MODULE__{value: value}), do: <<value::32>>
  def encoded(state), do: state |> finish() |> Base.encode64()
  def header(algorithm), do: "x-amz-checksum-" <> String.downcase(algorithm)

  def digest(algorithm, bytes) when is_binary(bytes) do
    with {:ok, state} <- new(algorithm), do: {:ok, state |> update(bytes) |> finish()}
  end

  @doc "Checksums of parts in wire order, encoded as base64(digest(raw part digests))-count."
  def composite("CRC64NVME", _parts), do: failure(:invalid_composite_algorithm)

  def composite(algorithm, parts) when is_list(parts) and length(parts) in 1..10_000 do
    with {:ok, state} <- new(algorithm),
         {:ok, state} <-
           Enum.reduce_while(parts, {:ok, state}, fn part, {:ok, state} ->
             case decode(part, algorithm) do
               {:ok, bytes} -> {:cont, {:ok, update(state, bytes)}}
               error -> {:halt, error}
             end
           end) do
      {:ok, encoded(state) <> "-" <> to_string(length(parts))}
    end
  end

  def composite(_algorithm, _parts), do: failure(:invalid_composite_parts)

  @doc "Combines finalized CRC part checksums and byte lengths into a full-object CRC."
  def combine(algorithm, parts)
      when algorithm in ["CRC32", "CRC32C", "CRC64NVME"] and is_list(parts) and
             length(parts) in 1..10_000 do
    {width, polynomial} =
      case algorithm do
        "CRC32" -> {32, 0xEDB88320}
        "CRC32C" -> {32, 0x82F63B78}
        "CRC64NVME" -> {64, 0x9A6C9329AC4BC9B5}
      end

    matrix = [polynomial | Enum.map(0..(width - 2), &(1 <<< &1))] |> List.to_tuple()
    byte_operator = matrix |> square() |> square() |> square()

    result =
      Enum.reduce_while(parts, {:ok, 0}, fn
        {checksum, size}, {:ok, crc} when is_integer(size) and size in 0..0x7FFFFFFFFFFFFFFF ->
          case decode(checksum, algorithm) do
            {:ok, bytes} ->
              {:cont,
               {:ok, bxor(advance(byte_operator, crc, size), :binary.decode_unsigned(bytes))}}

            error ->
              {:halt, error}
          end

        _, _ ->
          {:halt, failure(:invalid_checksum_parts)}
      end)

    with {:ok, crc} <- result, do: {:ok, Base.encode64(<<crc::size(width)>>)}
  end

  def combine(_algorithm, _parts), do: failure(:invalid_checksum_parts)

  defp advance(_matrix, crc, 0), do: crc

  defp advance(matrix, crc, bytes) do
    crc = if band(bytes, 1) == 1, do: times(matrix, crc), else: crc
    advance(square(matrix), crc, bsr(bytes, 1))
  end

  defp square(matrix),
    do: for(value <- Tuple.to_list(matrix), do: times(matrix, value)) |> List.to_tuple()

  defp times(matrix, vector), do: times(matrix, vector, 0, 0)
  defp times(_matrix, 0, _index, acc), do: acc

  defp times(matrix, vector, index, acc) do
    acc = if band(vector, 1) == 1, do: bxor(acc, elem(matrix, index)), else: acc
    times(matrix, bsr(vector, 1), index + 1, acc)
  end

  @doc "Verifies a full-object digest, or an explicit ordered collection of part digests."
  def verify(state, expected, opts \\ []) do
    type =
      opts[:checksum_type] ||
        if(is_binary(expected) and String.contains?(expected, "-"),
          do: "COMPOSITE",
          else: "FULL_OBJECT"
        )

    case type do
      "FULL_OBJECT" ->
        with {:ok, bytes} <- decode(expected, state.algorithm),
             true <- :crypto.hash_equals(finish(state), bytes),
             do: {:ok, :verified},
             else: (_ -> failure(:checksum_mismatch))

      "COMPOSITE" ->
        with parts when is_list(parts) <- opts[:parts],
             {:ok, calculated} <- composite(state.algorithm, parts),
             true <- calculated == expected,
             do: {:ok, :verified},
             else: (_ -> failure(:composite_checksum_unverified))

      _ ->
        failure(:invalid_checksum_type)
    end
  end

  @doc "Rejects conflicting header/trailer values; absence is reported without a verification claim."
  def verify_response(state, headers, trailers \\ [], opts \\ []) do
    fields = Enum.map(headers ++ trailers, fn {name, value} -> {String.downcase(name), value} end)
    values = for {name, value} <- fields, name == header(state.algorithm), do: value
    types = for {"x-amz-checksum-type", type} <- fields, do: type

    cond do
      length(Enum.uniq(values)) > 1 or length(Enum.uniq(types)) > 1 ->
        failure(:conflicting_checksums)

      values == [] ->
        {:ok, :not_provided}

      true ->
        verify(state, hd(values), Keyword.put_new(opts, :checksum_type, List.first(types)))
    end
  end

  @doc "A lazy aws-chunked unsigned checksum envelope. Source chunks must obey the configured bound."
  def encode_trailers(enumerable, algorithm, opts \\ []) do
    with {:ok, _state} <- new(algorithm) do
      stream =
        Stream.transform(
          enumerable,
          fn -> elem(new(algorithm), 1) end,
          fn chunk, state ->
            if not is_binary(chunk) or byte_size(chunk) > (opts[:max_chunk_bytes] || 131_072),
              do: raise(ArgumentError, "Checksum trailer source chunk exceeds its byte bound")

            output =
              if chunk == "",
                do: [],
                else: [
                  Integer.to_string(byte_size(chunk), 16) |> String.downcase(),
                  "\r\n",
                  chunk,
                  "\r\n"
                ]

            {if(output == [], do: [], else: [IO.iodata_to_binary(output)]), update(state, chunk)}
          end,
          fn state ->
            {["0\r\n" <> header(algorithm) <> ":" <> encoded(state) <> "\r\n\r\n"], state}
          end,
          fn _state -> :ok end
        )

      {:ok, stream}
    end
  end

  defp decode(value, algorithm) when is_binary(value) do
    case Base.decode64(value) do
      {:ok, bytes} ->
        if byte_size(bytes) == @sizes[algorithm],
          do: {:ok, bytes},
          else: failure(:invalid_checksum)

      _ ->
        failure(:invalid_checksum)
    end
  end

  defp decode(_value, _algorithm), do: failure(:invalid_checksum)

  defp failure(code),
    do: {:error, Error.new(:protocol, "Invalid or unverified S3 checksum", code: code)}
end
