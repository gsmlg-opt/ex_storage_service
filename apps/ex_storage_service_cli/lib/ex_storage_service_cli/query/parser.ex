# MIT-derived code; full copyright and license in NOTICE.txt.
defmodule ExStorageServiceCli.Query.Parser do
  @moduledoc """
  Parses JMESPath expressions.
  """

  alias ExStorageServiceCli.Query.Lexer

  @type expr :: String.t() | charlist
  @type ast :: tuple | atom
  @type error :: {:error, any}

  @doc """
  Parses a JMESPath expression into an Abstract Syntax Tree.
  """
  @spec parse(expr) :: {:ok, ast} | error
  def parse(expr) do
    with {:ok, tokens, _} <- Lexer.scan(expr),
         {:ok, ast} <- :ess_cli_jmes_parser.parse(tokens) do
      literals(ast)
    end
  end

  defp literals({:json, encoded}) do
    case JSON.decode(encoded) do
      {:ok, value} -> {:ok, {:literal, value}}
      {:error, _} -> {:error, :syntax}
    end
  end

  defp literals(tuple) when is_tuple(tuple) do
    with {:ok, values} <- literals(Tuple.to_list(tuple)), do: {:ok, List.to_tuple(values)}
  end

  defp literals(list) when is_list(list) do
    Enum.reduce_while(list, {:ok, []}, fn child, {:ok, acc} ->
      case literals(child) do
        {:ok, child} -> {:cont, {:ok, [child | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp literals(value), do: {:ok, value}
end
