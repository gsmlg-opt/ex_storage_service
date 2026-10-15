defmodule ExStorageServiceCli.YAML do
  @moduledoc """
  Bounded safe YAML input and block YAML output for modeled CLI values.

  Standard timestamp and binary inputs retain tagged values for modeled coercion.
  Unknown tags, non-string map keys, non-finite numbers, multiple documents and
  excessive alias expansion fail explicitly. No Erlang extension constructor is
  packaged or loaded. Source provenance is retained in `yaml/NOTICE.txt`.
  """

  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.YAML.Encode

  @defaults [
    max_bytes: 8_388_608,
    max_nodes: 100_000,
    max_depth: 128,
    timeout: 1000,
    max_heap_words: 8_388_608
  ]

  # Load only this fixed, packaged parser dependency set before the worker's
  # input-execution deadline. Cold code-server contention is not YAML work.
  @parser_modules [
    ExStorageServiceCli.YAML.Scalar,
    :ess_cli_yaml_constr,
    :ess_cli_yaml_parser,
    :ess_cli_yaml_errors,
    :ess_cli_yaml_node_timestamp,
    :ess_cli_yaml_node_null,
    :ess_cli_yaml_node_bool,
    :ess_cli_yaml_node_bool_ext,
    :ess_cli_yaml_node_int,
    :ess_cli_yaml_node_int_ext,
    :ess_cli_yaml_node_float,
    :ess_cli_yaml_node_float_ext,
    :ess_cli_yaml_node_str,
    :ess_cli_yaml_node_binary,
    :ess_cli_yaml_node_seq,
    :ess_cli_yaml_node_map
  ]

  def decode(text, opts \\ []) do
    limits = Keyword.merge(@defaults, opts)

    cond do
      not is_binary(text) ->
        failure(:invalid)

      byte_size(text) > limits[:max_bytes] ->
        failure(:budget)

      not String.valid?(text) ->
        failure(:invalid)

      true ->
        if Enum.all?(@parser_modules, &Code.ensure_loaded?/1),
          do: isolated(fn -> parse(text, limits) end, limits),
          else: failure(:invalid)
    end
  end

  def encode(data, opts \\ []) do
    limits = Keyword.merge(@defaults, opts)

    with {:ok, data, _} <- normalize(data, 0, {0, 0}, limits, :output) do
      isolated(fn -> emit(data, limits) end, limits)
    end
  end

  defp parse(text, limits) do
    docs =
      :ess_cli_yaml_constr.string(text,
        str_node_as_binary: true,
        map_node_format: :map,
        node_mods: [:ess_cli_yaml_node_timestamp],
        schema: :auto,
        ignore_unrecognized_tags: false
      )

    case docs do
      [] ->
        {:ok, nil}

      [data] ->
        with {:ok, data, _} <- normalize(data, 0, {0, 0}, limits, :input), do: {:ok, data}

      _ ->
        failure(:invalid)
    end
  end

  defp emit(data, limits) do
    text =
      if is_map(data) or is_list(data),
        do: Encode.to_s!(data, sort_maps: true) <> "\n",
        else: scalar(data) <> "\n"

    if byte_size(text) > limits[:max_bytes], do: failure(:budget), else: {:ok, text}
  end

  defp scalar(data) when is_float(data), do: ExStorageServiceCli.Presentation.Scalar.float(data)
  defp scalar(data), do: JSON.encode!(data)

  defp isolated(fun, limits) do
    owner = self()
    ref = make_ref()

    {guardian, monitor} =
      spawn_monitor(fn ->
        owner_monitor = Process.monitor(owner)
        recipient = self()

        {worker, worker_monitor} =
          :erlang.spawn_opt(
            fn ->
              result =
                try do
                  fun.()
                rescue
                  _ -> failure(:invalid)
                catch
                  :throw, :ess_cli_yaml_budget -> failure(:budget)
                  _, _ -> failure(:invalid)
                end

              send(recipient, {ref, result})
            end,
            [
              :monitor,
              {:max_heap_size, %{size: limits[:max_heap_words], kill: true, error_logger: false}}
            ]
          )

        result =
          receive do
            {^ref, result} -> result
            {:DOWN, ^owner_monitor, :process, ^owner, _} -> :owner_gone
            {:DOWN, ^worker_monitor, :process, ^worker, _} -> failure(:budget)
          after
            limits[:timeout] -> failure(:budget)
          end

        Process.exit(worker, :kill)
        Process.demonitor(worker_monitor, [:flush])
        Process.demonitor(owner_monitor, [:flush])
        if result != :owner_gone, do: send(owner, {ref, result})
      end)

    receive do
      {^ref, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^guardian, _} ->
        failure(:budget)
    end
  end

  defp normalize(value, depth, {nodes, bytes}, limits, mode) do
    cond do
      depth > limits[:max_depth] or nodes >= limits[:max_nodes] or bytes > limits[:max_bytes] ->
        failure(:budget)

      is_map(value) and map_size(value) > limits[:max_nodes] - nodes ->
        failure(:budget)

      is_map(value) ->
        normalize_map(value, depth, {nodes + 1, bytes}, limits, mode)

      is_list(value) ->
        normalize_list(value, depth, {nodes + 1, bytes}, limits, mode)

      value == :null and mode == :input ->
        finish(nil, nodes + 1, bytes + 1, limits)

      value == {:ess_cli_yaml_merge_key, "<<"} and mode == :input ->
        finish("<<", nodes + 1, bytes + 2, limits)

      is_binary(value) and String.valid?(value) ->
        finish(value, nodes + 1, bytes + byte_size(value), limits)

      is_number(value) or is_boolean(value) or is_nil(value) ->
        finish(value, nodes + 1, bytes + :erlang.external_size(value), limits)

      mode == :input and is_tuple(value) ->
        normalize_tag(value, nodes + 1, bytes, limits)

      true ->
        failure(:invalid)
    end
  end

  defp normalize_tag({tag, value} = tagged, nodes, bytes, limits)
       when tag in [:yaml_timestamp, :yaml_date, :binary_file] and is_binary(value),
       do: finish(tagged, nodes, bytes + byte_size(value), limits)

  defp normalize_tag(_, _, _, _), do: failure(:invalid)

  defp normalize_map(map, depth, counts, limits, mode) do
    Enum.reduce_while(map, {:ok, %{}, counts}, fn
      {key, value}, {:ok, acc, counts} when is_binary(key) ->
        with {:ok, key, counts} <- normalize(key, depth + 1, counts, limits, mode),
             {:ok, value, counts} <- normalize(value, depth + 1, counts, limits, mode) do
          {:cont, {:ok, Map.put(acc, key, value), counts}}
        else
          error -> {:halt, error}
        end

      _, _ ->
        {:halt, failure(:invalid)}
    end)
  end

  defp normalize_list(list, depth, counts, limits, mode) do
    Enum.reduce_while(list, {:ok, [], counts}, fn value, {:ok, acc, counts} ->
      case normalize(value, depth + 1, counts, limits, mode) do
        {:ok, value, counts} -> {:cont, {:ok, [value | acc], counts}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed, counts} -> {:ok, Enum.reverse(reversed), counts}
      error -> error
    end
  end

  defp finish(value, nodes, bytes, limits) do
    if nodes > limits[:max_nodes] or bytes > limits[:max_bytes],
      do: failure(:budget),
      else: {:ok, value, {nodes, bytes}}
  end

  defp failure(reason) do
    if reason == :budget,
      do:
        {:error,
         Error.new(:usage, "YAML exceeds its materialization or execution budget",
           code: "yaml-budget-exceeded"
         )},
      else:
        {:error,
         Error.new(:usage, "Invalid YAML or unsupported modeled YAML value", code: "invalid-yaml")}
  end
end
