defmodule ExStorageServiceCli.Query do
  @moduledoc """
  Bounded JMESPath evaluation over JSON values with binary object keys.

  The private MIT port and compliance provenance are recorded in `query/NOTICE.txt`.
  Materialized input and output are limited rather than silently truncated.
  """

  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.Query.{Evaluator, Parser}

  @defaults [
    max_expression_bytes: 8192,
    max_nodes: 100_000,
    max_bytes: 8_388_608,
    max_depth: 128,
    timeout: 1000,
    max_heap_words: 8_388_608
  ]

  def search(expression, data, opts \\ []) do
    limits = Keyword.merge(@defaults, opts)

    with :ok <- expression_budget(expression, limits),
         :ok <- json_budget(data, limits) do
      isolated(expression, data, limits)
    end
  end

  def validate(expression, opts \\ []) do
    limits = Keyword.merge(@defaults, opts)

    with :ok <- expression_budget(expression, limits) do
      isolated(expression, nil, limits, :validate)
    end
  end

  defp expression_budget(expression, limits) when is_binary(expression) do
    cond do
      byte_size(expression) > limits[:max_expression_bytes] -> failure(:budget)
      not String.valid?(expression) -> failure(:syntax)
      true -> :ok
    end
  end

  defp expression_budget(_, _), do: failure(:syntax)

  defp isolated(expression, data, limits, mode \\ :search) do
    owner = self()
    ref = make_ref()

    {guardian, monitor} =
      spawn_monitor(fn -> guard(owner, ref, expression, data, limits, mode) end)

    receive do
      {^ref, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^guardian, _} ->
        failure(:budget)
    end
  end

  defp guard(owner, ref, expression, data, limits, mode) do
    owner_monitor = Process.monitor(owner)
    guardian = self()

    {worker, worker_monitor} =
      :erlang.spawn_opt(
        fn ->
          result =
            if mode == :validate,
              do: validate_expression(expression, limits),
              else: evaluate(expression, data, limits)

          send(guardian, {ref, result})
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

    receive do
      {:DOWN, ^worker_monitor, :process, ^worker, _} -> :ok
    after
      0 -> Process.demonitor(worker_monitor, [:flush])
    end

    Process.demonitor(owner_monitor, [:flush])
    if result != :owner_gone, do: send(owner, {ref, result})
  end

  defp validate_expression(expression, limits) do
    with {:ok, ast} <- Parser.parse(expression),
         :ok <- ast_depth(ast, limits[:max_depth]) do
      :ok
    else
      {:error, %Error{}} = error -> error
      _ -> failure(:syntax)
    end
  rescue
    _ -> failure(:syntax)
  catch
    _, _ -> failure(:syntax)
  end

  defp evaluate(expression, data, limits) do
    with {:ok, ast} <- Parser.parse(expression),
         :ok <- ast_depth(ast, limits[:max_depth]),
         {:ok, result} <- Evaluator.search(ast, data),
         :ok <- json_budget(result, limits) do
      {:ok, result}
    else
      {:error, %Error{}} = error -> error
      {:error, reason} -> failure(reason)
      {:error, _, _} -> failure(:syntax)
    end
  rescue
    _ -> failure(:syntax)
  catch
    _, _ -> failure(:syntax)
  end

  defp ast_depth(_, remaining) when remaining < 0, do: failure(:budget)

  defp ast_depth(tuple, remaining) when is_tuple(tuple) do
    tuple |> Tuple.to_list() |> ast_depth(remaining - 1)
  end

  defp ast_depth(list, remaining) when is_list(list) do
    Enum.reduce_while(list, :ok, fn child, :ok ->
      case ast_depth(child, remaining - 1) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp ast_depth(_, _), do: :ok

  defp json_budget(value, limits) do
    case measure(value, 0, {0, 0}, limits) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp measure(value, depth, {nodes, bytes}, limits) do
    cond do
      depth > limits[:max_depth] or nodes >= limits[:max_nodes] or bytes > limits[:max_bytes] ->
        failure(:budget)

      is_map(value) and map_size(value) > limits[:max_nodes] - nodes ->
        failure(:budget)

      is_map(value) ->
        measure_children(Map.to_list(value), depth, {nodes + 1, bytes}, limits)

      is_list(value) ->
        measure_children(value, depth, {nodes + 1, bytes}, limits)

      is_binary(value) ->
        finish_measure(nodes + 1, bytes + byte_size(value), limits)

      is_number(value) or is_boolean(value) or is_nil(value) ->
        finish_measure(nodes + 1, bytes + :erlang.external_size(value), limits)

      true ->
        failure(:invalid_type)
    end
  end

  defp measure_children(children, depth, counts, limits) do
    Enum.reduce_while(children, {:ok, counts}, fn
      {key, value}, {:ok, counts} when is_binary(key) ->
        with {:ok, counts} <- measure(key, depth + 1, counts, limits),
             {:ok, counts} <- measure(value, depth + 1, counts, limits) do
          {:cont, {:ok, counts}}
        else
          error -> {:halt, error}
        end

      {_, _}, _ ->
        {:halt, failure(:invalid_type)}

      child, {:ok, counts} ->
        case measure(child, depth + 1, counts, limits) do
          {:ok, counts} -> {:cont, {:ok, counts}}
          error -> {:halt, error}
        end
    end)
  end

  defp finish_measure(nodes, bytes, limits) do
    if nodes > limits[:max_nodes] or bytes > limits[:max_bytes],
      do: failure(:budget),
      else: {:ok, {nodes, bytes}}
  end

  defp failure(reason) do
    code =
      case reason do
        :budget -> "query-budget-exceeded"
        :invalid_type -> "invalid-type"
        :invalid_arity -> "invalid-arity"
        :invalid_step -> "invalid-value"
        :unknown_function -> "unknown-function"
        _ -> "syntax"
      end

    message =
      if reason == :budget,
        do: "JMESPath query exceeds its materialization or execution budget",
        else: "Invalid JMESPath query: #{code}"

    {:error, Error.new(:usage, message, code: code)}
  end
end
