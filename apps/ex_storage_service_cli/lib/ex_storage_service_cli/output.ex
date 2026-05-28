defmodule ExStorageServiceCli.Output do
  @moduledoc """
  Output formatting for the CLI.

  Supports human-readable table output with ANSI colors and
  machine-readable JSON output.
  """

  @doc """
  Prints an informational message to stdout.
  """
  def info(message) do
    IO.puts(message)
  end

  @doc """
  Prints a success message to stdout with green color.
  """
  def success(message) do
    IO.puts("#{IO.ANSI.green()}✓#{IO.ANSI.reset()} #{message}")
  end

  @doc """
  Prints a warning message to stderr with yellow color.
  """
  def warn(message) do
    IO.puts(:stderr, "#{IO.ANSI.yellow()}⚠#{IO.ANSI.reset()} #{message}")
  end

  @doc """
  Prints an error message to stderr with red color.
  """
  def error(message) do
    IO.puts(:stderr, "#{IO.ANSI.red()}✗ Error:#{IO.ANSI.reset()} #{message}")
  end

  @doc """
  Prints data in the appropriate format based on context.

  When `ctx.json` is true, outputs JSON. Otherwise, uses the
  provided formatter function for human-readable output.
  """
  def render(data, ctx, formatter) do
    if ctx[:json] do
      IO.puts(Jason.encode!(data, pretty: true))
    else
      formatter.(data)
    end
  end

  @doc """
  Prints a table with headers and rows.

  ## Parameters

    * `headers` - List of column header strings
    * `rows` - List of row lists (each row is a list of cell values)
  """
  def table(headers, rows) do
    all_rows = [headers | rows]

    # Calculate column widths
    col_count = length(headers)

    widths =
      Enum.map(0..(col_count - 1), fn col ->
        all_rows
        |> Enum.map(fn row ->
          cell = Enum.at(row, col, "")
          String.length(to_string(cell))
        end)
        |> Enum.max()
      end)

    # Print header
    header_line =
      headers
      |> Enum.zip(widths)
      |> Enum.map(fn {header, width} ->
        String.pad_trailing(to_string(header), width)
      end)
      |> Enum.join("  ")

    IO.puts("#{IO.ANSI.bright()}#{header_line}#{IO.ANSI.reset()}")

    # Print separator
    separator =
      widths
      |> Enum.map(fn width -> String.duplicate("─", width) end)
      |> Enum.join("──")

    IO.puts("#{IO.ANSI.faint()}#{separator}#{IO.ANSI.reset()}")

    # Print rows
    Enum.each(rows, fn row ->
      line =
        row
        |> Enum.zip(widths)
        |> Enum.map(fn {cell, width} ->
          String.pad_trailing(to_string(cell), width)
        end)
        |> Enum.join("  ")

      IO.puts(line)
    end)
  end

  @doc """
  Formats a byte count into a human-readable string.
  """
  def format_bytes(bytes) when is_integer(bytes) do
    cond do
      bytes >= 1_073_741_824 ->
        "#{Float.round(bytes / 1_073_741_824, 1)} GiB"

      bytes >= 1_048_576 ->
        "#{Float.round(bytes / 1_048_576, 1)} MiB"

      bytes >= 1_024 ->
        "#{Float.round(bytes / 1_024, 1)} KiB"

      true ->
        "#{bytes} B"
    end
  end

  def format_bytes(_), do: "0 B"

  @doc """
  Formats an ISO 8601 datetime string into a shorter display format.
  """
  def format_datetime(nil), do: ""
  def format_datetime(""), do: ""

  def format_datetime(iso_string) do
    case DateTime.from_iso8601(iso_string) do
      {:ok, dt, _} ->
        Calendar.strftime(dt, "%Y-%m-%d %H:%M:%S")

      _ ->
        iso_string
    end
  end
end
