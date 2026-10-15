defmodule ExStorageServiceCli.Transfer.DownloadChecksum do
  @moduledoc "Full-object and independently recomputed multipart checksums over the bytes delivered to the sink."
  alias ExStorageServiceCli.{Error, S3.Checksum, S3.Client}

  def composite?(response) do
    header(response.headers, "x-amz-checksum-type") == "COMPOSITE" or
      Enum.any?(Checksum.algorithms(), fn algorithm ->
        String.contains?(header(response.headers, Checksum.header(algorithm)) || "", "-")
      end)
  end

  def start(context, input, response, opts) do
    fields = Map.new(response.headers, fn {name, value} -> {String.downcase(name), value} end)

    trailers =
      String.split((fields["trailer"] || fields["x-amz-trailer"] || "") |> String.downcase(), ",",
        trim: true
      )
      |> Enum.map(&String.trim/1)

    algorithms =
      Enum.filter(
        Checksum.algorithms(),
        &(Map.has_key?(fields, Checksum.header(&1)) or Checksum.header(&1) in trailers)
      )

    states =
      Map.new(algorithms, fn algorithm ->
        {:ok, state} = Checksum.new(algorithm)
        {algorithm, state}
      end)

    composite = composite?(response)

    with :ok <- declarations(fields, algorithms, composite),
         {:ok, parts} <- manifest(context, input, response, composite, opts) do
      {:ok,
       %{
         states: states,
         composite: composite,
         parts: parts,
         current: states,
         remaining: if(parts != [], do: hd(parts)["Size"], else: nil),
         completed: Map.new(algorithms, &{&1, []}),
         declared_trailers: trailers
       }}
    end
  end

  defp declarations(fields, algorithms, composite) do
    cond do
      fields["x-amz-checksum-type"] not in [nil, "FULL_OBJECT", "COMPOSITE"] ->
        failure(:invalid_checksum_type)

      composite and algorithms == [] ->
        failure(:missing_checksum)

      composite and "CRC64NVME" in algorithms ->
        failure(:invalid_composite_algorithm)

      true ->
        :ok
    end
  end

  def update(state, chunk) do
    state = %{
      state
      | states:
          Map.new(state.states, fn {algorithm, digest} ->
            {algorithm, Checksum.update(digest, chunk)}
          end)
    }

    if state.composite, do: parts(state, chunk), else: {:ok, state}
  end

  def finish(state, headers, trailers) do
    checksum_names =
      for {name, _} <- headers ++ trailers,
          String.starts_with?(String.downcase(name), "x-amz-checksum-"),
          do: String.downcase(name)

    unknown =
      Enum.any?(checksum_names, fn name ->
        name != "x-amz-checksum-type" and
          name not in Enum.map(Map.keys(state.states), &Checksum.header/1)
      end)

    missing =
      Enum.any?(state.declared_trailers, fn name ->
        String.starts_with?(name, "x-amz-checksum-") and header(trailers, name) == nil
      end)

    cond do
      unknown ->
        failure(:undeclared_checksum_trailer)

      missing ->
        failure(:missing_checksum_trailer)

      state.composite and state.parts != [] ->
        failure(:incomplete_checksum_parts)

      true ->
        Enum.reduce_while(state.states, {:ok, :not_provided}, fn {algorithm, digest},
                                                                 {:ok, outcome} ->
          opts =
            if state.composite, do: [parts: Enum.reverse(state.completed[algorithm])], else: []

          case Checksum.verify_response(digest, headers, trailers, opts) do
            {:ok, :verified} -> {:cont, {:ok, :verified}}
            {:ok, :not_provided} -> {:cont, {:ok, outcome}}
            error -> {:halt, error}
          end
        end)
    end
  end

  defp parts(state, ""), do: {:ok, state}
  defp parts(%{parts: []}, _chunk), do: failure(:excess_checksum_bytes)

  defp parts(state, chunk) do
    take = min(state.remaining, byte_size(chunk))
    <<piece::binary-size(take), rest::binary>> = chunk

    current =
      Map.new(state.current, fn {algorithm, digest} ->
        {algorithm, Checksum.update(digest, piece)}
      end)

    if take == state.remaining do
      [part | remaining] = state.parts

      verified =
        Enum.reduce_while(current, :ok, fn {algorithm, digest}, :ok ->
          case Checksum.verify(digest, part["Checksum" <> algorithm],
                 checksum_type: "FULL_OBJECT"
               ) do
            {:ok, :verified} -> {:cont, :ok}
            error -> {:halt, error}
          end
        end)

      with :ok <- verified do
        completed =
          Map.new(current, fn {algorithm, digest} ->
            {algorithm, [Checksum.encoded(digest) | state.completed[algorithm]]}
          end)

        next =
          Map.new(current, fn {algorithm, _} ->
            {:ok, digest} = Checksum.new(algorithm)
            {algorithm, digest}
          end)

        parts(
          %{
            state
            | parts: remaining,
              current: next,
              completed: completed,
              remaining: if(remaining != [], do: hd(remaining)["Size"], else: nil)
          },
          rest
        )
      end
    else
      {:ok, %{state | current: current, remaining: state.remaining - take}}
    end
  end

  defp manifest(_context, _input, _response, false, _opts), do: {:ok, []}

  defp manifest(context, input, response, true, opts) do
    if input["Range"] != nil do
      failure(:composite_range_checksum)
    else
      with {:ok, parts} <- load_parts(context, input, response, 0, [], opts),
           true <-
             parts != [] and
               Enum.with_index(parts, 1)
               |> Enum.all?(fn {part, index} ->
                 is_map(part) and part["PartNumber"] == index and is_integer(part["Size"]) and
                   part["Size"] > 0
               end),
           true <- Enum.reduce(parts, 0, &(&1["Size"] + &2)) == length_header(response.headers) do
        {:ok, parts}
      else
        false -> failure(:invalid_checksum_parts)
        error -> error
      end
    end
  end

  defp load_parts(context, input, response, marker, accumulated, opts) do
    attributes =
      input
      |> Map.take(
        ~w(Bucket Key VersionId SSECustomerAlgorithm SSECustomerKey SSECustomerKeyMD5 RequestPayer ExpectedBucketOwner)
      )
      |> Map.put("ObjectAttributes", ~w(ObjectParts Checksum ETag ObjectSize))
      |> Map.put("MaxParts", 1000)

    attributes =
      if marker > 0, do: Map.put(attributes, "PartNumberMarker", marker), else: attributes

    version = header(response.headers, "x-amz-version-id")
    attributes = if version, do: Map.put(attributes, "VersionId", version), else: attributes

    with {:ok, page} <- Client.call(context, "GetObjectAttributes", attributes, opts),
         :ok <- identity(page, response),
         {:ok, object_parts, parts} <- page_parts(page, accumulated) do
      accumulated = accumulated ++ parts

      if object_parts["IsTruncated"] == true do
        next = object_parts["NextPartNumberMarker"]

        if is_integer(next) and next > marker and next <= 10_000,
          do: load_parts(context, input, response, next, accumulated, opts),
          else: failure(:invalid_checksum_parts)
      else
        {:ok, accumulated}
      end
    else
      error -> error
    end
  end

  defp identity(page, response) do
    expected = tag(header(response.headers, "etag"))

    if expected != nil and expected == tag(page["ETag"]),
      do: :ok,
      else: failure(:checksum_identity_changed)
  end

  defp page_parts(%{"ObjectParts" => %{"Parts" => parts} = object_parts}, accumulated)
       when is_list(parts) and length(parts) > 0 and length(parts) <= 1000 do
    if length(accumulated) + length(parts) <= 10_000,
      do: {:ok, object_parts, parts},
      else: failure(:invalid_checksum_parts)
  end

  defp page_parts(_page, _accumulated), do: failure(:invalid_checksum_parts)

  defp length_header(headers) do
    case Integer.parse(header(headers, "content-length") || "") do
      {value, ""} -> value
      _ -> nil
    end
  end

  defp header(headers, key),
    do:
      Enum.find_value(headers, fn {name, value} -> if String.downcase(name) == key, do: value end)

  defp tag(value) when is_binary(value), do: String.trim(value, "\"")
  defp tag(_), do: nil

  defp failure(code),
    do:
      {:error,
       Error.new(:protocol, "Object checksum verification could not be completed", code: code)}
end
