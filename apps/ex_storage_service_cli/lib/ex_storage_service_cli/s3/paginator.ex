defmodule ExStorageServiceCli.S3.Paginator do
  @moduledoc "Modeled service pagination and interoperable bounded CLI resume tokens."
  alias ExStorageServiceCli.{Error, S3.OperationCatalog}

  def pages(operation, input, fetch, opts \\ []) do
    cli_pages(operation, input, fetch, opts)
    |> Stream.map(fn
      {:ok, page, _metadata} -> {:ok, page}
      error -> error
    end)
  end

  @doc "Page iterator retaining CLI resume metadata separately from service data."
  def cli_pages(operation, input, fetch, opts \\ []) do
    name = OperationCatalog.operation_name(operation)
    model = OperationCatalog.paginators()[name]

    case configuration(model, input, opts) do
      {:error, _} = error ->
        [error]

      {:ok, initial} ->
        Stream.resource(
          fn -> initial end,
          fn
            :done ->
              {:halt, :done}

            state ->
              case fetch.(name, state.input) do
                {:ok, page} ->
                  with {:ok, next} <- advance(model, page, state, opts),
                       {:ok, page, resume, next} <- truncate(model, page, state, next, opts) do
                    {[{:ok, page, %{first: state.first, resume_token: resume}}], next}
                  else
                    {:error, _} = error -> {[error], :done}
                  end

                {:error, _} = error ->
                  {[error], :done}
              end
          end,
          fn _ -> :ok end
        )
    end
  end

  @doc "Materializes only an explicitly bounded result; large scans use pages/4."
  def collect(operation, input, fetch, opts \\ []) do
    model = OperationCatalog.paginators()[OperationCatalog.operation_name(operation)]
    keys = if model, do: List.wrap(model["result_key"]), else: []
    limit = opts[:max_aggregate_bytes] || 16_777_216

    cli_pages(operation, input, fetch, opts)
    |> Enum.reduce_while({:ok, nil, 0}, fn
      {:error, _} = error, _ ->
        {:halt, error}

      {:ok, page, metadata}, {:ok, result, total} ->
        total = total + :erlang.external_size(page)

        if total > limit do
          {:halt,
           failure(
             :aggregate_limit,
             "Paginated result exceeds its materialization budget; request a limit or streaming output"
           )}
        else
          first =
            if model && opts[:no_paginate] != true, do: output_page(model, page, true), else: page

          merged = if result == nil, do: first, else: aggregate(keys, result, page)

          merged =
            if metadata.resume_token,
              do: Map.put(merged, "NextToken", metadata.resume_token),
              else: merged

          {:cont, {:ok, merged, total}}
        end
    end)
    |> case do
      {:ok, result, _} -> {:ok, result || %{}}
      error -> error
    end
  end

  @doc "AWS text pages expose result keys and first-page nonaggregate values."
  def output_page(operation, page, first) when is_binary(operation),
    do:
      output_page(
        OperationCatalog.paginators()[OperationCatalog.operation_name(operation)],
        page,
        first
      )

  def output_page(nil, page, _first), do: page

  def output_page(model, page, first) do
    result = Map.take(page, List.wrap(model["result_key"]))

    if first,
      do: Enum.reduce(model["non_aggregate_keys"] || [], result, &Map.put(&2, &1, page[&1])),
      else: result
  end

  defp aggregate(keys, result, page) do
    Enum.reduce(keys, result, fn key, acc ->
      case page[key] do
        nil ->
          acc

        value ->
          Map.update(acc, key, value, fn
            nil -> value
            current when is_list(current) and is_list(value) -> current ++ value
            current when is_number(current) and is_number(value) -> current + value
            current when is_binary(current) and is_binary(value) -> current <> value
          end)
      end
    end)
  end

  defp configuration(model, input, opts) do
    cond do
      opts[:no_paginate] && (opts[:max_items] != nil or opts[:starting_token] != nil) ->
        syntax("CLI pagination options conflict with --no-paginate")

      (opts[:max_items] != nil or opts[:starting_token] != nil or opts[:page_size] != nil) and
          is_nil(model) ->
        syntax("This operation does not support CLI pagination")

      opts[:max_items] != nil and (not is_integer(opts[:max_items]) or opts[:max_items] < 0) ->
        syntax("Max items must be a nonnegative integer")

      opts[:page_size] != nil and (not is_integer(opts[:page_size]) or opts[:page_size] <= 0) ->
        syntax("Page size must be a positive integer")

      true ->
        with {:ok, tokens, index} <- starting(model, opts[:starting_token]) do
          input = inject(input, tokens)

          input =
            if model && opts[:page_size],
              do: Map.put(input, model["limit_key"], opts[:page_size]),
              else: input

          {:ok,
           %{
             input: input,
             count: 0,
             previous: nil,
             checkpoint: nil,
             power: 1,
             distance: 0,
             total: 0,
             first: true,
             starting: opts[:starting_token] != nil,
             index: index
           }}
        end
    end
  end

  defp starting(_model, nil), do: {:ok, %{}, 0}

  defp starting(model, value) when is_binary(value) and byte_size(value) <= 65_536 do
    keys = List.wrap(model["input_token"])

    parsed =
      with {:ok, bytes} <- Base.decode64(value),
           {:ok, map} when is_map(map) <- JSON.decode(bytes),
           do: {:ok, map}

    case parsed do
      {:ok, map} ->
        index = map["boto_truncate_amount"] || 0

        if is_integer(index) and index >= 0 and
             Enum.all?(Map.keys(map), &(&1 in (keys ++ ["boto_truncate_amount"]))) and
             Enum.all?(Map.take(map, keys), fn {_, token} -> valid_token?(token) end),
           do: {:ok, Map.take(map, keys), index},
           else: syntax("Invalid starting token")

      _ ->
        deprecated(keys, value)
    end
  end

  defp starting(_model, _value), do: syntax("Invalid or oversized starting token")

  defp deprecated(keys, value) do
    parts = String.split(value, "___")

    {parts, index} =
      if length(parts) == length(keys) + 1 do
        case Integer.parse(List.last(parts)) do
          {index, ""} when index >= 0 -> {Enum.drop(parts, -1), index}
          _ -> {[value], 0}
        end
      else
        {parts, 0}
      end

    if length(parts) <= length(keys) do
      values = Enum.map(parts, fn value -> if value == "None", do: nil, else: value end)

      {:ok,
       Enum.zip(keys, values ++ List.duplicate(nil, length(keys) - length(values))) |> Map.new(),
       index}
    else
      syntax("Invalid starting token")
    end
  end

  defp advance(nil, _page, _state, _opts), do: {:ok, :done}

  defp advance(model, page, state, opts) do
    tokens = Enum.map(List.wrap(model["output_token"]), &token(page, &1))

    more =
      if model["more_results"],
        do: Map.get(page, model["more_results"]),
        else: Enum.any?(tokens, &present?/1)

    cond do
      opts[:no_paginate] ->
        {:ok, :done}

      more == false ->
        {:ok, :done}

      more != true ->
        failure(:invalid_pagination, "Paginated response lacks its completion marker")

      not Enum.any?(tokens, &present?/1) ->
        failure(:missing_pagination_token, "Truncated response lacks a continuation token")

      Enum.any?(tokens, &(not valid_token?(&1))) ->
        failure(:invalid_pagination_token, "Invalid or oversized continuation token")

      state.count + 1 >= (opts[:max_pages] || 1_000_000) ->
        failure(:pagination_limit, "Service pagination exceeded its page budget")

      true ->
        digest = :crypto.hash(:sha256, :erlang.term_to_binary(tokens))

        if digest == state.previous or digest == state.checkpoint do
          failure(:repeated_pagination_token, "Service pagination repeated a continuation token")
        else
          input =
            inject(state.input, Enum.zip(List.wrap(model["input_token"]), tokens) |> Map.new())

          {checkpoint, power, distance} =
            if state.distance + 1 >= state.power,
              do: {digest, state.power * 2, 0},
              else: {state.checkpoint, state.power, state.distance + 1}

          {:ok,
           %{
             state
             | input: input,
               count: state.count + 1,
               previous: digest,
               checkpoint: checkpoint,
               power: power,
               distance: distance
           }}
        end
    end
  end

  defp truncate(nil, page, _state, next, _opts), do: {:ok, page, nil, next}

  defp truncate(model, page, state, next, opts) do
    [primary | secondary] = List.wrap(model["result_key"])

    page =
      if state.first && state.starting do
        page = Map.put(page, primary, slice(page[primary], state.index, :all))

        Enum.reduce(secondary, page, fn key, acc ->
          value =
            cond do
              is_list(page[key]) -> []
              is_binary(page[key]) -> ""
              is_number(page[key]) -> page[key]
              true -> nil
            end

          Map.put(acc, key, value)
        end)
      else
        page
      end

    count = count(page[primary])
    wanted = if opts[:max_items] == nil, do: count, else: max(opts[:max_items] - state.total, 0)

    cond do
      opts[:no_paginate] ->
        {:ok, page, nil, :done}

      wanted < count ->
        tokens =
          token_input(model, state.input)
          |> Map.put("boto_truncate_amount", wanted + if(state.first, do: state.index, else: 0))

        {:ok, Map.put(page, primary, slice(page[primary], 0, wanted)), encode_token(tokens),
         :done}

      opts[:max_items] != nil and state.total + count == opts[:max_items] and next != :done ->
        {:ok, page, encode_token(token_input(model, next.input)), :done}

      next == :done ->
        {:ok, page, nil, :done}

      true ->
        {:ok, page, nil, %{next | total: state.total + count, first: false, index: 0}}
    end
  end

  defp slice(nil, _offset, _length), do: nil
  defp slice(value, offset, :all) when is_list(value), do: Enum.drop(value, offset)
  defp slice(value, offset, count) when is_list(value), do: Enum.slice(value, offset, count)

  defp slice(value, offset, :all) when is_binary(value),
    do: value |> String.codepoints() |> Enum.drop(offset) |> Enum.join()

  defp slice(value, offset, count) when is_binary(value),
    do: value |> String.codepoints() |> Enum.slice(offset, count) |> Enum.join()

  defp count(nil), do: 0
  defp count(value) when is_list(value), do: length(value)
  defp count(value) when is_binary(value), do: value |> String.codepoints() |> length()
  defp token_input(model, input), do: Map.new(List.wrap(model["input_token"]), &{&1, input[&1]})
  defp encode_token(tokens), do: tokens |> JSON.encode!() |> Base.encode64()

  defp inject(input, tokens),
    do:
      Enum.reduce(tokens, input, fn {key, value}, input ->
        if value == nil, do: Map.delete(input, key), else: Map.put(input, key, value)
      end)

  defp token(page, "NextMarker || Contents[-1].Key"),
    do:
      if(present?(page["NextMarker"]),
        do: page["NextMarker"],
        else: (List.last(page["Contents"] || []) || %{})["Key"]
      )

  defp token(page, key), do: Map.get(page, key)
  defp present?(value), do: value not in [nil, ""]
  defp valid_token?(nil), do: true
  defp valid_token?(value) when is_binary(value), do: byte_size(value) <= 16_384
  defp valid_token?(value) when is_integer(value), do: value >= 0
  defp valid_token?(_), do: false

  defp syntax(message),
    do: {:error, Error.new(:syntax, message, code: :invalid_pagination_option)}

  defp failure(code, message), do: {:error, Error.new(:protocol, message, code: code)}
end
