defmodule ExStorageServiceCli.Presenter do
  @moduledoc "Structured command data on stdout; diagnostics remain separate."
  alias ExStorageServiceCli.{Error, Query, YAML}
  alias ExStorageServiceCli.CLI.OutputSession
  alias ExStorageServiceCli.Presentation.{Text, Table}
  alias ExStorageServiceCli.Presentation.JSON, as: Json

  def write(data, options \\ %{}), do: present(data, options, false)

  @doc "Emit one queried paginator page as one YAML list item without buffering later pages."
  def write_stream_page(data, options),
    do: present(data, Map.put(options, :output, "yaml-stream"), true)

  defp present(data, options, page?) do
    with {:ok, data} <- query(normalize(remove_metadata(data)), options[:query]),
         {:ok, output} <- render(data, options, page?),
         do: OutputSession.write(options[:output_session], output)
  rescue
    _ -> failure(:invalid_presentation)
  catch
    :throw, :invalid_presentation -> failure(:invalid_presentation)
  end

  defp render(data, options, page?) do
    case options[:output] || "json" do
      "off" ->
        {:ok, ""}

      format when format in ["json", "yaml", "yaml-stream"] and data == %{} and not page? ->
        {:ok, ""}

      "json" ->
        {:ok, Json.render(data)}

      "ndjson" ->
        {:ok, JSON.encode!(data) <> "\n"}

      "text" ->
        {:ok, Text.render(data)}

      "table" ->
        {:ok, Table.render(data, options)}

      "yaml" ->
        YAML.encode(data)

      "yaml-stream" ->
        YAML.encode([data])

      _ ->
        failure(:unsupported_output)
    end
  end

  def normalize(%DateTime{} = value),
    do: DateTime.to_iso8601(value) |> String.replace_suffix("Z", "+00:00")

  def normalize(value) when is_map(value),
    do:
      Map.new(value, fn {key, value} ->
        {if(is_atom(key), do: Atom.to_string(key), else: key), normalize(value)}
      end)

  def normalize(value) when is_list(value), do: Enum.map(value, &normalize/1)

  def normalize(value) when is_atom(value) and value not in [true, false, nil],
    do: Atom.to_string(value)

  def normalize(value), do: value
  defp remove_metadata(data) when is_map(data), do: Map.delete(data, "ResponseMetadata")
  defp remove_metadata(data), do: data
  defp query(data, nil), do: Query.search("@", data)
  defp query(data, expression), do: Query.search(expression, data)

  defp failure(code),
    do: {:error, Error.new(:client, "Structured output cannot be rendered", code: code)}
end
