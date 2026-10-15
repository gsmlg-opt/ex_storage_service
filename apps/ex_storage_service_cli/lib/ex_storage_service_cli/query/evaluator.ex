# MIT-derived code; full copyright and license in NOTICE.txt.
defmodule ExStorageServiceCli.Query.Evaluator do
  @moduledoc """
  JMES implements JMESPath, a query language for JSON.

  Ported from jmes 0.4.1; see NOTICE.txt and the pinned corpus report.

  See [jmespath.org](http://jmespath.org).
  """

  alias ExStorageServiceCli.Query.{Functions, Parser}

  @type expr :: String.t() | charlist
  @type ast :: tuple | atom
  @type error :: {:error, any}

  @doc """
  Evaluates a JMESPath expression against some data.

  The expression can be a string, a charlist, or an Abstract Syntax Tree (see
  `ExStorageServiceCli.Query.Parser.parse/1`).

  ## Options

  - `underscore`: if `true`, underscore identifiers in the expression (default `false`)

  ## Examples

      iex> ExStorageServiceCli.Query.Evaluator.search("[name, age]", %{"name" => "Alice", "age" => 28, "place" => "wonderland"})
      {:ok, ["Alice", 28]}
  """
  @spec search(ast | expr, any) :: {:ok, any} | error
  def search(expr, data) do
    search(expr, data, [])
  end

  @spec search(ast, any, keyword) :: {:ok, any} | error
  def search(expr, data, opts) when is_tuple(expr) or is_atom(expr) do
    case eval(expr, data, opts) do
      {:ok, value} -> {:ok, unproject(value)}
      err -> err
    end
  end

  @spec search(expr, any, keyword) :: {:ok, any} | error
  def search(expr, data, opts) do
    with {:ok, ast} <- Parser.parse(expr) do
      search(ast, data, opts)
    end
  end

  @spec eval(ast, any, keyword) :: {:ok, any} | error

  # ==============================================================================================
  # Structs
  # ==============================================================================================

  defp eval(ast, %{__struct__: _} = struct, opts) do
    eval(ast, Map.from_struct(struct), opts)
  end

  # ==============================================================================================
  # Projection
  # ==============================================================================================

  defp eval(ast, {:project, data}, opts) do
    List.foldr(data, {:ok, {:project, []}}, fn
      item, {:ok, {:project, list}} = acc ->
        case eval(ast, item, opts) do
          {:ok, value} when is_nil(value) -> acc
          {:ok, value} -> {:ok, {:project, [value | list]}}
          err -> err
        end

      _expr, err ->
        err
    end)
  end

  # ==============================================================================================
  # Wildcard
  # ==============================================================================================

  defp eval(:wildcard, data, _opts) when is_map(data) do
    values = Map.values(data)

    {:ok, {:project, values}}
  end

  defp eval(:wildcard, _data, _opts) do
    {:ok, nil}
  end

  defp eval({:wildcard, expr}, data, opts) do
    case eval(expr, data, opts) do
      {:ok, {:project, _date} = value} -> eval({:list, [:wildcard]}, value, opts)
      {:ok, value} when is_list(value) -> {:ok, {:project, value}}
      {:ok, _value} -> {:ok, nil}
      err -> err
    end
  end

  defp eval({:list, [:wildcard]}, data, _opts) when is_list(data) do
    {:ok, {:project, data}}
  end

  defp eval({:list, [:wildcard]}, _data, _opts) do
    {:ok, nil}
  end

  # ==============================================================================================
  # ID
  # ==============================================================================================

  defp eval({:id, id}, data, _opts) when is_map(data) do
    {:ok, Map.get(data, id)}
  end

  defp eval({:id, _id}, _data, _opts) do
    {:ok, nil}
  end

  # ==============================================================================================
  # Literals
  # ==============================================================================================

  defp eval({:string, value}, _data, _opts) do
    {:ok, value}
  end

  defp eval({:literal, value}, _data, _opts), do: {:ok, value}

  defp eval(value, _data, _opts) when is_binary(value) do
    {:ok, value}
  end

  defp eval(value, _data, _opts) when is_number(value) do
    {:ok, value}
  end

  defp eval(value, _data, _opts) when is_list(value) do
    {:ok, value}
  end

  defp eval(value, _data, _opts) when is_map(value) do
    {:ok, value}
  end

  defp eval(value, _data, _opts) when is_nil(value) do
    {:ok, nil}
  end

  # ==============================================================================================
  # Node
  # ==============================================================================================

  defp eval(:node, data, _opts) do
    {:ok, data}
  end

  # ==============================================================================================
  # Pipe
  # ==============================================================================================

  defp eval({:pipe, [left, right]}, data, opts) do
    with {:ok, left} <- eval(left, data, opts) do
      eval(right, unproject(left), opts)
    end
  end

  # ==============================================================================================
  # Logical Operators
  # ==============================================================================================

  defp eval({:and, [left, right]}, data, opts) do
    with {:ok, value} <- eval(left, data, opts) do
      if truthy?(unproject(value)), do: eval(right, data, opts), else: {:ok, value}
    end
  end

  defp eval({:or, [left, right]}, data, opts) do
    with {:ok, value} <- eval(left, data, opts) do
      if truthy?(unproject(value)), do: {:ok, value}, else: eval(right, data, opts)
    end
  end

  defp eval({:eq, [left, right]}, data, opts) do
    binop_ok(left, right, data, &==/2, opts)
  end

  defp eval({:neq, [left, right]}, data, opts) do
    binop_ok(left, right, data, &!=/2, opts)
  end

  defp eval({:lt, [left, right]}, data, opts) do
    compare(left, right, data, &</2, opts)
  end

  defp eval({:gt, [left, right]}, data, opts) do
    compare(left, right, data, &>/2, opts)
  end

  defp eval({:lte, [left, right]}, data, opts) do
    compare(left, right, data, &<=/2, opts)
  end

  defp eval({:gte, [left, right]}, data, opts) do
    compare(left, right, data, &>=/2, opts)
  end

  defp eval({:not, expr}, data, opts) do
    with {:ok, value} <- eval(expr, data, opts),
         value = unproject(value) do
      {:ok, !truthy?(value)}
    end
  end

  # ==============================================================================================
  # Child
  # ==============================================================================================

  defp eval({:child, [expr, child]}, data, opts) do
    with {:ok, parent} <- eval(expr, data, opts) do
      eval(child, parent, opts)
    end
  end

  # ==============================================================================================
  # Flatten
  # ==============================================================================================

  defp eval(:flatten, data, _opts) when is_list(data) do
    {:ok, {:project, flatten(data)}}
  end

  defp eval(:flatten, _data, _opts) do
    {:ok, nil}
  end

  defp eval({:flatten, expr}, data, opts) do
    with {:ok, parent} <- eval(expr, data, opts),
         parent = unproject(parent) do
      eval(:flatten, parent, opts)
    end
  end

  # ==============================================================================================
  # Index
  # ==============================================================================================

  defp eval({:index, [nil, index]}, data, _opts) when is_integer(index) and is_list(data) do
    {:ok, Enum.at(data, index)}
  end

  defp eval({:index, [expr, index]}, data, opts) when is_integer(index) and not is_nil(expr) do
    with {:ok, parent} <- eval(expr, data, opts) do
      eval({:index, [nil, index]}, parent, opts)
    end
  end

  defp eval({:index, [_expr, _index]}, _data, _opts) do
    {:ok, nil}
  end

  # ==============================================================================================
  # Slice
  # ==============================================================================================

  defp eval({:slice, [_expr, [_start, _stop, _step]]}, nil, _opts) do
    {:ok, nil}
  end

  defp eval({:slice, [nil, [start, stop, step]]}, data, _opts) do
    project_slice(data, start, stop, step)
  end

  defp eval({:slice, [expr, [start, stop, step]]}, data, opts) do
    with {:ok, parent} <- eval(expr, data, opts) do
      project_slice(parent, start, stop, step)
    end
  end

  # ==============================================================================================
  # Filter
  # ==============================================================================================

  defp eval({:filter, [_expr, _query]}, nil, _opts) do
    {:ok, nil}
  end

  defp eval({:filter, [nil, query]}, data, opts) when is_list(data) do
    List.foldr(data, {:ok, {:project, []}}, fn
      item, {:ok, {:project, list}} = acc ->
        case eval(query, item, opts) do
          {:ok, value} ->
            if truthy?(unproject(value)) do
              {:ok, {:project, [item | list]}}
            else
              acc
            end

          err ->
            err
        end

      _expr, err ->
        err
    end)
  end

  defp eval({:filter, [nil, _query]}, _data, _opts) do
    {:ok, nil}
  end

  defp eval({:filter, [expr, query]}, data, opts) do
    with {:ok, parent} <- eval(expr, data, opts) do
      eval({:filter, [nil, query]}, parent, opts)
    end
  end

  # ==============================================================================================
  # List
  # ==============================================================================================

  defp eval({:list, _exprs}, nil, _opts) do
    {:ok, nil}
  end

  defp eval({:list, exprs}, data, opts) do
    with {:ok, value} <- eval_list(exprs, data, opts) do
      {:ok, value}
    end
  end

  # ==============================================================================================
  # Dictionary
  # ==============================================================================================

  defp eval({:dict, _keyvalv}, nil, _opts) do
    {:ok, nil}
  end

  defp eval({:dict, keyvalv}, data, opts) do
    List.foldl(keyvalv, {:ok, %{}}, fn
      [key, expr], {:ok, map} ->
        case eval(expr, data, opts) do
          {:ok, value} -> {:ok, Map.put(map, key, value)}
          err -> err
        end

      _expr, err ->
        err
    end)
  end

  # ==============================================================================================
  # Call
  # ==============================================================================================

  defp eval({:call, [name, argv]}, data, opts) do
    with {:ok, args} <- eval_list(argv, data, opts),
         args = unproject(args) do
      Functions.call(name, args, opts)
    end
  end

  defp eval({:quote, expr}, _data, _opts) do
    {:ok, {:expression, expr}}
  end

  # ==============================================================================================
  # Fallback
  # ==============================================================================================

  defp eval(ast, _data, _opts) do
    {:error, {:invalid_ast, ast}}
  end

  # ==============================================================================================
  # Helpers
  # ==============================================================================================

  @spec unproject(any) :: any
  defp unproject({:project, data}) do
    unproject(data)
  end

  defp unproject(data) when is_list(data) do
    Enum.map(data, &unproject(&1))
  end

  defp unproject(%{__struct__: _} = struct) do
    unproject(Map.from_struct(struct))
  end

  defp unproject(data) when is_map(data) do
    data
    |> Enum.map(fn {key, value} -> {key, unproject(value)} end)
    |> Enum.into(%{})
  end

  defp unproject(data) do
    data
  end

  @spec binop(expr, expr, any, (any, any -> {:ok, any} | error), keyword) :: {:ok, any} | error
  defp binop(left, right, data, fun, opts) do
    with {:ok, left} <- eval(left, data, opts),
         {:ok, right} <- eval(right, data, opts),
         left = unproject(left),
         right = unproject(right) do
      fun.(left, right)
    end
  end

  @spec binop_ok(expr, expr, any, (any, any -> any), keyword) :: {:ok, any}
  defp binop_ok(left, right, data, fun, opts) do
    binop(
      left,
      right,
      data,
      fn left, right ->
        {:ok, fun.(left, right)}
      end,
      opts
    )
  end

  @spec compare(expr, expr, any, (any, any -> boolean), keyword) :: {:ok, boolean} | error
  defp compare(left, right, data, fun, opts) do
    binop(
      left,
      right,
      data,
      fn left, right ->
        if is_number(left) and is_number(right) do
          {:ok, fun.(left, right)}
        else
          {:ok, nil}
        end
      end,
      opts
    )
  end

  @spec truthy?(any) :: boolean
  defp truthy?("") do
    false
  end

  defp truthy?([]) do
    false
  end

  defp truthy?(%{} = map) do
    length(Map.keys(map)) > 0
  end

  defp truthy?(value) do
    !!value
  end

  @spec flatten(any) :: list | nil
  defp flatten(data) when is_list(data) do
    List.foldr(data, [], fn
      [], acc -> acc
      [_ | _] = list, acc -> list ++ acc
      item, acc -> [item | acc]
    end)
  end

  defp flatten(_data) do
    nil
  end

  @spec slice(any, integer | nil, integer | nil, integer | nil) :: {:ok, list | nil} | error
  defp slice(_data, _start, _stop, step) when step == 0 do
    {:error, :invalid_step}
  end

  defp slice(data, _start, _stop, _step) when not is_list(data) do
    {:ok, nil}
  end

  defp slice(data, start, stop, step) do
    size = length(data)
    step = step || 1
    start = slice_bound(start, size, step, :start)
    stop = slice_bound(stop, size, step, :stop)
    tuple = List.to_tuple(data)

    values =
      Stream.unfold(start, fn index ->
        if (step > 0 and index < stop) or (step < 0 and index > stop),
          do: {elem(tuple, index), index + step},
          else: nil
      end)
      |> Enum.to_list()

    {:ok, values}
  end

  defp slice_bound(nil, _size, step, :start) when step > 0, do: 0
  defp slice_bound(nil, size, step, :start) when step < 0, do: size - 1
  defp slice_bound(nil, size, step, :stop) when step > 0, do: size
  defp slice_bound(nil, _size, step, :stop) when step < 0, do: -1

  defp slice_bound(value, size, step, _) do
    value = if value < 0, do: size + value, else: value
    if step > 0, do: min(size, max(0, value)), else: min(size - 1, max(-1, value))
  end

  @spec project_slice(any, integer | nil, integer | nil, integer | nil) :: {:ok, any} | error
  defp project_slice(data, start, stop, step) do
    case slice(data, start, stop, step) do
      {:ok, values} when is_list(values) -> {:ok, {:project, values}}
      default -> default
    end
  end

  @spec eval_list([ast], any, keyword) :: {:ok, list} | error
  defp eval_list(exprs, data, opts) do
    List.foldr(exprs, {:ok, []}, fn
      expr, {:ok, list} ->
        case eval(expr, data, opts) do
          {:ok, value} -> {:ok, [value | list]}
          err -> err
        end

      _expr, err ->
        err
    end)
  end
end
