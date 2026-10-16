defmodule ExStorageServiceCli.CLI.NativeSQL do
  @moduledoc "Bounded native S3 Select serialization and raw Records output."
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.CLI.RawOutput
  alias ExStorageServiceCli.S3.{Capabilities, Input, Select}
  alias ExStorageServiceCli.Transfer.{Inventory, Manifest, Sink}

  @csv_input %{
    "cc" => "Comments",
    "comments" => "Comments",
    "fh" => "FileHeaderInfo",
    "fileheader" => "FileHeaderInfo",
    "qrd" => "AllowQuotedRecordDelimiter",
    "quotedrecorddelimiter" => "AllowQuotedRecordDelimiter",
    "rd" => "RecordDelimiter",
    "recorddelimiter" => "RecordDelimiter",
    "fd" => "FieldDelimiter",
    "fielddelimiter" => "FieldDelimiter",
    "qc" => "QuoteCharacter",
    "quotechar" => "QuoteCharacter",
    "qec" => "QuoteEscapeCharacter",
    "quoteescchar" => "QuoteEscapeCharacter"
  }
  @csv_output Map.take(
                @csv_input,
                ~w(rd recorddelimiter fd fielddelimiter qc quotechar qec quoteescchar)
              )
              |> Map.merge(%{"qf" => "QuoteFields", "quotefields" => "QuoteFields"})
  @json_input %{"type" => "Type"}
  @json_output %{"rd" => "RecordDelimiter", "recorddelimiter" => "RecordDelimiter"}

  def run(invocation, target, context, opts \\ [])

  def run(invocation, %Target{kind: :remote, bucket: bucket} = target, context, opts)
      when is_binary(bucket) and bucket != "" do
    with {:ok, settings} <- settings(invocation),
         :ok <- Capabilities.check(context, "SelectObjectContent"),
         {:ok, inventory} <- inventory(target, context, settings, opts) do
      try do
        with {:ok, plan} <- plan(inventory, settings, opts) do
          try do
            output(plan, context, target, settings, opts)
          after
            Manifest.cleanup(plan)
          end
        end
      after
        Manifest.cleanup(inventory)
      end
    end
  end

  def run(_invocation, _target, _context, _opts),
    do: invalid("SQL requires one remote object, bucket or prefix")

  def stdout?(%{args: [_]}), do: true
  def stdout?(_), do: false

  defp settings(invocation) do
    options = invocation.options

    with {:ok, expression, filename} <- arguments(invocation.args, options),
         true <-
           is_binary(expression) and String.valid?(expression) and
             byte_size(expression) in 1..262_144,
         true <- is_integer(options[:limit] || 1000) and (options[:limit] || 1000) in 0..10_000,
         :ok <- output_mode(options, filename),
         {:ok, input} <- serialization(options, :input),
         {:ok, output} <- serialization(options, :output),
         {:ok, header} <- header(options, output),
         {:ok, compression} <- compression(options[:compression]) do
      {:ok,
       %{
         expression: expression,
         filename: filename,
         input: input,
         output: output,
         header: header,
         compression: compression,
         recursive: options[:recursive] == true,
         limit: options[:limit] || 1000
       }}
    else
      false ->
        invalid(
          "SQL expression must be bounded and nonempty; object limit must be within 0..10000"
        )

      error ->
        error
    end
  end

  defp arguments([_target], options),
    do: {:ok, options[:sql_query] || "select * from s3object", nil}

  defp arguments([_target, expression, filename], options) do
    if Map.has_key?(options, :sql_query),
      do:
        invalid("Choose --query with one target or the legacy target expression outfile syntax"),
      else: {:ok, expression, filename}
  end

  defp arguments(_, _),
    do: invalid("SQL accepts one target or legacy target expression outfile operands")

  defp output_mode(options, nil) do
    if options[:output] not in [nil, "off"] or options[:query] != nil,
      do:
        invalid(
          "SQL Records use raw stdout; use serialization options instead of structured output"
        ),
      else: :ok
  end

  defp output_mode(_options, _filename), do: :ok

  defp serialization(options, direction) do
    {full, csv, json} =
      if direction == :input,
        do: {:input_serialization, :csv_input, :json_input},
        else: {:output_serialization, :csv_output, :json_output}

    cond do
      Map.has_key?(options, csv) and Map.has_key?(options, json) ->
        invalid("Choose one CSV or JSON serialization per direction")

      Map.has_key?(options, full) and
          (Map.has_key?(options, csv) or Map.has_key?(options, json) or
             (direction == :input and Map.has_key?(options, :compression))) ->
        invalid(
          "Full JSON serialization cannot be combined with convenience serialization options"
        )

      Map.has_key?(options, full) ->
        document(options[full], direction)

      direction == :input and (options[csv] == "" or options[json] == "") ->
        {:ok, nil}

      Map.has_key?(options, csv) ->
        with {:ok, fields} <-
               pairs(options[csv], if(direction == :input, do: @csv_input, else: @csv_output)),
             do: {:ok, %{"CSV" => fields}}

      Map.has_key?(options, json) ->
        with {:ok, fields} <-
               pairs(options[json], if(direction == :input, do: @json_input, else: @json_output)),
             do: {:ok, %{"JSON" => fields}}

      direction == :output and options[:json] == true ->
        {:ok, %{"JSON" => %{}}}

      true ->
        {:ok, nil}
    end
  end

  defp document(value, direction) when is_binary(value) and byte_size(value) <= 1_048_576 do
    with {:ok, map} when is_map(map) <- JSON.decode(value),
         formats = if(direction == :input, do: ["CSV", "JSON", "Parquet"], else: ["CSV", "JSON"]),
         true <- Enum.count(formats, &Map.has_key?(map, &1)) == 1,
         input = if(direction == :input, do: map, else: %{"CSV" => %{}}),
         output = if(direction == :output, do: map, else: %{"CSV" => %{}}),
         {:ok, _} <-
           Input.validate("SelectObjectContent", %{
             "Bucket" => "sql-validation",
             "Key" => "key",
             "ExpressionType" => "SQL",
             "Expression" => "select * from s3object",
             "InputSerialization" => input,
             "OutputSerialization" => output
           }) do
      {:ok, map}
    else
      {:error, %Error{}} = error ->
        error

      _ ->
        invalid("Select serializations require exactly one modeled CSV, JSON or Parquet format")
    end
  end

  defp document(_, _), do: invalid("Select serializations must be bounded JSON objects")

  defp pairs(value, names) when is_binary(value) and byte_size(value) <= 65_536 do
    # A separator comma must precede the next key=value. Thus fd=,,rd=\n
    # retains a literal comma delimiter without accumulating object data.
    parts = if value == "", do: [], else: Regex.split(~r/,(?=[^,=]+=)/, value)

    if length(parts) > 16 do
      invalid("Too many SQL serialization fields")
    else
      Enum.reduce_while(parts, {:ok, %{}}, fn part, {:ok, fields} ->
        case String.split(part, "=", parts: 2) do
          [key, value] ->
            key = names[String.downcase(key)]

            with true <- key != nil and not Map.has_key?(fields, key),
                 {:ok, value} <- field(key, unescape(value)) do
              {:cont, {:ok, Map.put(fields, key, value)}}
            else
              _ -> {:halt, invalid("Invalid or repeated SQL serialization field")}
            end

          _ ->
            {:halt, invalid("SQL serialization options require key=value pairs")}
        end
      end)
    end
  end

  defp pairs(_, _), do: invalid("SQL serialization options must be bounded strings")

  defp unescape(value),
    do:
      value
      |> String.replace("\\n", "\n")
      |> String.replace("\\r", "\r")
      |> String.replace("\\t", "\t")

  defp field(key, value) when key in ["FileHeaderInfo", "QuoteFields", "Type"] do
    value = String.upcase(value)

    choices =
      case key do
        "FileHeaderInfo" -> ~w(NONE IGNORE USE)
        "QuoteFields" -> ~w(ALWAYS ASNEEDED)
        "Type" -> ~w(DOCUMENT LINES)
      end

    if value in choices, do: {:ok, value}, else: invalid("Invalid SQL serialization enum")
  end

  defp field("AllowQuotedRecordDelimiter", value) do
    case String.downcase(value) do
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> invalid("QuotedRecordDelimiter requires true or false")
    end
  end

  defp field("RecordDelimiter", value) do
    if String.valid?(value) and byte_size(value) in 1..16,
      do: {:ok, value},
      else: invalid("SQL record delimiter must contain 1..16 bytes")
  end

  defp field(_key, value) do
    if String.valid?(value) and byte_size(value) in 1..16 and String.length(value) == 1,
      do: {:ok, value},
      else: invalid("SQL delimiters require one character")
  end

  defp header(options, output) do
    case Map.fetch(options, :csv_output_header) do
      :error ->
        {:ok, ""}

      {:ok, ""} ->
        invalid(
          "Empty CSV output header inference is unavailable; provide an explicit comma-separated header"
        )

      {:ok, value} when is_binary(value) and byte_size(value) <= 65_536 ->
        if (output == nil or Map.has_key?(output, "CSV")) and String.valid?(value) and
             not String.contains?(value, ["\r", "\n"]),
           do: {:ok, value <> "\n"},
           else: invalid("CSV output header requires CSV output and a single bounded line")

      _ ->
        invalid("CSV output header must be a bounded string")
    end
  end

  defp compression(nil), do: {:ok, nil}

  defp compression(value) when is_binary(value) do
    value = String.upcase(value)

    if value in ~w(NONE GZIP BZIP2),
      do: {:ok, value},
      else: invalid("SQL compression requires NONE, GZIP or BZIP2")
  end

  defp compression(_), do: invalid("SQL compression requires NONE, GZIP or BZIP2")

  defp inventory(target, context, _settings, opts) do
    if target.key in [nil, ""] or String.ends_with?(target.key, "/"),
      do: Inventory.remote(context, target, opts),
      else: Manifest.build([%{"name" => target.key, "key" => target.key, "single" => true}], opts)
  end

  defp plan(inventory, settings, opts) do
    entries =
      Manifest.stream(inventory)
      |> Stream.flat_map(fn entry ->
        if not settings.recursive and entry["single"] != true and
             String.contains?(entry["name"], "/") do
          []
        else
          case object_input(entry["key"], settings) do
            {:ok, input, output} ->
              [
                %{
                  "name" => entry["name"],
                  "key" => entry["key"],
                  "input" => input,
                  "output" => output
                }
              ]

            :skip ->
              []

            {:error, %Error{code: :sql_format}} = error ->
              if(entry["single"] == true, do: [error], else: [])

            error ->
              [error]
          end
        end
      end)

    Manifest.build(entries, opts)
  end

  defp object_input(key, settings) do
    base = key |> String.replace(~r/\.(?:gz|bz2?|gzip)\z/, "")

    inferred =
      cond do
        String.ends_with?(base, ".csv") ->
          %{"CSV" => %{"FileHeaderInfo" => "USE"}}

        String.ends_with?(base, [".json", ".jsonl", ".ndjson"]) ->
          %{"JSON" => %{"Type" => "LINES"}}

        String.ends_with?(base, ".parquet") ->
          %{"Parquet" => %{}}

        true ->
          nil
      end

    input = settings.input || inferred

    cond do
      String.ends_with?(key, "/") ->
        :skip

      input == nil ->
        {:error,
         Error.new(:syntax, "SQL cannot infer object format; provide --csv-input or --json-input",
           code: :sql_format
         )}

      String.ends_with?(base, ".parquet") and
          (Map.has_key?(input, "CSV") or Map.has_key?(input, "JSON")) ->
        invalid("CSV/JSON input options cannot target a Parquet object")

      true ->
        compression =
          settings.compression ||
            cond do
              String.ends_with?(key, [".gz", ".gzip"]) -> "GZIP"
              String.ends_with?(key, [".bz", ".bz2"]) -> "BZIP2"
              true -> "NONE"
            end

        input =
          if Map.has_key?(input, "CompressionType"),
            do: input,
            else: Map.put(input, "CompressionType", compression)

        input = defaults(input, :input)

        output =
          settings.output ||
            if(Map.has_key?(input, "JSON"), do: %{"JSON" => %{}}, else: %{"CSV" => %{}})

        output = defaults(output, :output)

        if settings.header != "" and not Map.has_key?(output, "CSV"),
          do: invalid("CSV output header cannot accompany JSON output"),
          else: {:ok, input, output}
    end
  end

  defp defaults(%{"CSV" => csv} = value, :input),
    do:
      Map.put(
        value,
        "CSV",
        Map.put_new(csv, "RecordDelimiter", "\n") |> Map.put_new("FieldDelimiter", ",")
      )

  defp defaults(%{"CSV" => csv} = value, :output),
    do:
      Map.put(
        value,
        "CSV",
        Map.put_new(csv, "RecordDelimiter", "\n") |> Map.put_new("FieldDelimiter", ",")
      )

  defp defaults(%{"JSON" => json} = value, :input),
    do: Map.put(value, "JSON", Map.put_new(json, "Type", "LINES"))

  defp defaults(%{"JSON" => json} = value, :output),
    do: Map.put(value, "JSON", Map.put_new(json, "RecordDelimiter", "\n"))

  defp defaults(value, _), do: value

  defp output(plan, context, target, %{filename: nil} = settings, opts) do
    operation = fn ->
      execute(plan, context, target, settings, opts[:write] || (&stdout/1), opts)
    end

    if opts[:write], do: operation.(), else: RawOutput.with_stdout(operation)
  end

  defp output(plan, context, target, settings, opts) do
    with {:ok, sink} <- Sink.open(settings.filename) do
      try do
        with {:ok, summary} <-
               execute(plan, context, target, settings, &Sink.write(sink, &1), opts),
             :ok <- Sink.publish(sink),
             do: {:ok, summary}
      after
        Sink.abort(sink)
      end
    end
  end

  defp execute(plan, context, target, settings, write, opts) do
    written = :atomics.new(1, signed: false)

    write = fn bytes ->
      with :ok <- write.(bytes),
           do:
             (
               :atomics.add(written, 1, byte_size(bytes))
               :ok
             )
    end

    initial = %{queried: 0, bytes: 0, header: false, summary: nil}

    result =
      plan
      |> Manifest.stream()
      |> Stream.take(settings.limit)
      |> Enum.reduce_while({:ok, initial}, fn entry, {:ok, acc} ->
        input = %{
          "Bucket" => target.bucket,
          "Key" => entry["key"],
          "ExpressionType" => "SQL",
          "Expression" => settings.expression,
          "InputSerialization" => entry["input"],
          "OutputSerialization" => entry["output"]
        }

        reducer = fn
          %{type: "Records", payload: bytes}, state ->
            with :ok <- write_header(write, settings.header, state.header),
                 :ok <- write.(bytes),
                 do: {:ok, %{state | bytes: state.bytes + byte_size(bytes), header: true}}

          %{type: "End"}, state ->
            with :ok <- write_header(write, settings.header, state.header),
                 do: {:ok, %{state | header: true}}

          _event, state ->
            {:ok, state}
        end

        case Select.call(context, input, acc, reducer, opts) do
          {:ok, state, summary} ->
            {:cont, {:ok, %{state | queried: acc.queried + 1, summary: summary}}}

          {:error, error} ->
            {:halt,
             {:error,
              %{
                error
                | details:
                    Map.merge(error.details, %{
                      partial_output: settings.filename == nil and :atomics.get(written, 1) > 0,
                      output_bytes:
                        if(settings.filename == nil, do: :atomics.get(written, 1), else: 0),
                      queried_objects: acc.queried
                    })
              }}}
        end
      end)

    with {:ok, result} <- result do
      summary = %{
        "QueriedObjects" => result.queried,
        "MatchedObjects" => plan.count,
        "Partial" => plan.count > result.queried,
        "RecordBytes" => result.bytes
      }

      {:ok, if(settings.filename, do: Map.merge(result.summary || %{}, summary), else: summary)}
    end
  end

  defp write_header(_write, _header, true), do: :ok
  defp write_header(_write, "", _), do: :ok
  defp write_header(write, header, false), do: write.(header)

  defp stdout(bytes) do
    case IO.binwrite(:stdio, bytes) do
      :ok -> :ok
      _ -> {:error, Error.new(:transfer, "Cannot write SQL Records", code: :stdout_write)}
    end
  end

  defp invalid(message), do: {:error, Error.new(:syntax, message, code: :sql_options)}
end
