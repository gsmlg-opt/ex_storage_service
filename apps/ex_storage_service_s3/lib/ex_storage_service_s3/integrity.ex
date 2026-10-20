defmodule ExStorageServiceS3.Integrity do
  @moduledoc false
  import Bitwise
  import Plug.Conn, only: [get_req_header: 2]

  @algorithms %{
    "crc32" => {:crc32, 4},
    "crc32c" => {:crc32c, 4},
    "crc64nvme" => {:crc64nvme, 8},
    "sha1" => {:sha, 20},
    "sha256" => {:sha256, 32}
  }
  @crc32c_table List.to_tuple(
                  for byte <- 0..255 do
                    Enum.reduce(1..8, byte, fn _, crc ->
                      if band(crc, 1) == 1, do: bxor(crc >>> 1, 0x82F63B78), else: crc >>> 1
                    end)
                  end
                )
  @crc64_table List.to_tuple(
                 for byte <- 0..255 do
                   Enum.reduce(1..8, byte, fn _, crc ->
                     if band(crc, 1) == 1,
                       do: bxor(crc >>> 1, 0x9A6C9329AC4BC9B5),
                       else: crc >>> 1
                   end)
                 end
               )

  def new(conn, max_size, opts \\ []) do
    conn =
      if Keyword.get(opts, :verify_wire_checksums, true),
        do: conn,
        else: %{
          conn
          | req_headers:
              Enum.reject(conn.req_headers, fn {name, _} ->
                String.starts_with?(name, "x-amz-checksum-") or
                  name in ["x-amz-sdk-checksum-algorithm", "x-amz-trailer"]
              end)
        }

    with {:ok, payload} <- payload_expectation(conn),
         {:ok, expected} <- checksum_expectations(conn),
         {:ok, trailers} <- trailer_names(conn),
         :ok <- checksum_declaration(conn, expected, trailers),
         {:ok, length} <- decoded_length(conn) do
      names =
        Enum.uniq(Map.keys(expected) ++ trailers ++ Keyword.get(opts, :calculate_checksums, []))

      contexts = Map.new(names, fn name -> {name, init(name)} end)

      {:ok,
       %{
         payload: payload,
         sha256: :crypto.hash_init(:sha256),
         contexts: contexts,
         expected: expected,
         trailers: trailers,
         size: 0,
         max_size: max_size,
         length: length
       }}
    end
  end

  @doc false
  def algorithms, do: Map.keys(@algorithms)

  def update(state, bytes) do
    size = state.size + byte_size(bytes)

    if state.max_size != :infinity and size > state.max_size do
      {:error, :entity_too_large}
    else
      contexts =
        Map.new(state.contexts, fn {name, context} ->
          {name, update_checksum(name, context, bytes)}
        end)

      {:ok,
       %{state | sha256: :crypto.hash_update(state.sha256, bytes), contexts: contexts, size: size}}
    end
  end

  def finish(state, trailers \\ %{}) do
    with :ok <- verify_length(state),
         :ok <- verify_payload(state),
         {:ok, expected} <- merge_trailers(state, trailers),
         :ok <- verify_checksums(state, expected) do
      {:ok,
       Map.new(state.contexts, fn {name, context} ->
         {name, Base.encode64(final(name, context))}
       end)}
    end
  end

  defp payload_expectation(conn) do
    case get_req_header(conn, "x-amz-content-sha256") do
      [] ->
        {:ok, nil}

      ["UNSIGNED-PAYLOAD"] ->
        {:ok, nil}

      ["STREAMING-AWS4-HMAC-SHA256-PAYLOAD"] ->
        {:ok, nil}

      ["STREAMING-UNSIGNED-PAYLOAD-TRAILER"] ->
        {:ok, nil}

      [hash] ->
        case Base.decode16(hash, case: :mixed) do
          {:ok, binary} when byte_size(binary) == 32 -> {:ok, binary}
          _ -> {:error, :unsupported_payload_mode}
        end

      _ ->
        {:error, :invalid_digest}
    end
  end

  defp checksum_expectations(conn) do
    Enum.reduce_while(
      [
        {"content-md5", "md5", 16}
        | Enum.map(@algorithms, fn {name, {_, size}} ->
            {"x-amz-checksum-" <> name, name, size}
          end)
      ],
      {:ok, %{}},
      fn {header, name, size}, {:ok, acc} ->
        case get_req_header(conn, header) do
          [] ->
            {:cont, {:ok, acc}}

          [value] ->
            case Base.decode64(value) do
              {:ok, bytes} when byte_size(bytes) == size ->
                {:cont, {:ok, Map.put(acc, name, bytes)}}

              _ ->
                {:halt, {:error, :invalid_digest}}
            end

          _ ->
            {:halt, {:error, :invalid_digest}}
        end
      end
    )
  end

  defp checksum_declaration(conn, expected, trailers) do
    algorithm = get_req_header(conn, "x-amz-sdk-checksum-algorithm")
    type = get_req_header(conn, "x-amz-checksum-type")

    unknown =
      Enum.any?(conn.req_headers, fn {name, _} ->
        String.starts_with?(name, "x-amz-checksum-") and name != "x-amz-checksum-type" and
          not Map.has_key?(@algorithms, String.replace_prefix(name, "x-amz-checksum-", ""))
      end)

    cond do
      unknown ->
        {:error, :unsupported_checksum}

      type not in [[], ["FULL_OBJECT"]] ->
        {:error, :unsupported_checksum_type}

      algorithm == [] ->
        :ok

      length(algorithm) != 1 ->
        {:error, :invalid_digest}

      true ->
        [declared] = algorithm
        name = String.downcase(declared)

        if Map.has_key?(@algorithms, name) and (Map.has_key?(expected, name) or name in trailers),
          do: :ok,
          else: {:error, :unsupported_checksum}
    end
  end

  defp trailer_names(conn) do
    case get_req_header(conn, "x-amz-trailer") do
      [] ->
        {:ok, []}

      [value] ->
        names = value |> String.split(",") |> Enum.map(&String.trim/1)
        checksums = Enum.map(names, &String.replace_prefix(&1, "x-amz-checksum-", ""))

        if names != [] and length(Enum.uniq(names)) == length(names) and
             Enum.all?(Enum.zip(names, checksums), fn {header, name} ->
               header == "x-amz-checksum-" <> name and Map.has_key?(@algorithms, name)
             end),
           do: {:ok, checksums},
           else: {:error, :unsupported_checksum}

      _ ->
        {:error, :invalid_digest}
    end
  end

  defp decoded_length(conn) do
    case get_req_header(conn, "x-amz-decoded-content-length") do
      [] ->
        {:ok, nil}

      [value] ->
        case Integer.parse(value) do
          {size, ""} when size >= 0 -> {:ok, size}
          _ -> {:error, :invalid_content_length}
        end

      _ ->
        {:error, :invalid_content_length}
    end
  end

  defp verify_length(%{length: length, size: size}) when is_integer(length) and length != size,
    do: {:error, :invalid_content_length}

  defp verify_length(_), do: :ok
  defp verify_payload(%{payload: nil}), do: :ok

  defp verify_payload(state) do
    if Plug.Crypto.secure_compare(:crypto.hash_final(state.sha256), state.payload),
      do: :ok,
      else: {:error, :payload_hash_mismatch}
  end

  defp merge_trailers(state, trailers) do
    if Enum.sort(Map.keys(trailers)) ==
         Enum.sort(Enum.map(state.trailers, &("x-amz-checksum-" <> &1))) do
      Enum.reduce_while(trailers, {:ok, state.expected}, fn {header, value}, {:ok, expected} ->
        name = String.replace_prefix(header, "x-amz-checksum-", "")
        {_, size} = Map.fetch!(@algorithms, name)

        case Base.decode64(value) do
          {:ok, binary} when byte_size(binary) == size ->
            {:cont, {:ok, Map.put(expected, name, binary)}}

          _ ->
            {:halt, {:error, :invalid_digest}}
        end
      end)
    else
      {:error, :invalid_digest}
    end
  end

  defp verify_checksums(state, expected) do
    if Enum.all?(expected, fn {name, digest} ->
         Plug.Crypto.secure_compare(final(name, Map.fetch!(state.contexts, name)), digest)
       end),
       do: :ok,
       else: {:error, :bad_digest}
  end

  defp init("crc32"), do: 0
  defp init("crc32c"), do: 0xFFFFFFFF
  defp init("crc64nvme"), do: 0xFFFFFFFFFFFFFFFF
  defp init("md5"), do: :crypto.hash_init(:md5)
  defp init(name), do: :crypto.hash_init(elem(Map.fetch!(@algorithms, name), 0))
  defp update_checksum("crc32", context, bytes), do: :erlang.crc32(context, bytes)
  defp update_checksum("crc32c", context, bytes), do: crc(bytes, context, @crc32c_table)
  defp update_checksum("crc64nvme", context, bytes), do: crc(bytes, context, @crc64_table)
  defp update_checksum(_name, context, bytes), do: :crypto.hash_update(context, bytes)
  defp crc(<<>>, context, _table), do: context

  defp crc(<<byte, rest::binary>>, context, table),
    do: crc(rest, bxor(context >>> 8, elem(table, band(bxor(context, byte), 255))), table)

  defp final("crc32", context), do: <<context::unsigned-big-32>>
  defp final("crc32c", context), do: <<bxor(context, 0xFFFFFFFF)::unsigned-big-32>>
  defp final("crc64nvme", context), do: <<bxor(context, 0xFFFFFFFFFFFFFFFF)::unsigned-big-64>>
  defp final(_name, context), do: :crypto.hash_final(context)
end
