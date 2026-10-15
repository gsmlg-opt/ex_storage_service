defmodule ExStorageServiceCli.CLI.AWSInput do
  @moduledoc "Offline modeled AWS s3api input parsing with bounded JSON, YAML and shorthand inputs."
  alias ExStorageServiceCli.{Error, CLI.Shorthand, S3.Input, S3.OperationCatalog}
  alias ExStorageServiceCli.{CLI.AutoPrompt, Credentials.SharedConfig}
  @max_input 8_388_608
  @globals %{
    "profile" => {:profile, :string},
    "region" => {:region, :string},
    "endpoint-url" => {:endpoint_url, :string},
    "no-sign-request" => {:no_sign_request, :boolean},
    "no-verify-ssl" => {:no_verify_ssl, :boolean},
    "ca-bundle" => {:ca_bundle, :string},
    "debug" => {:debug, :boolean},
    "output" => {:output, :string},
    "query" => {:query, :string},
    "color" => {:color, :string},
    "cli-connect-timeout" => {:cli_connect_timeout, :integer},
    "cli-read-timeout" => {:cli_read_timeout, :integer},
    "cli-binary-format" => {:cli_binary_format, :string},
    "no-cli-pager" => {:no_cli_pager, :boolean},
    "cli-auto-prompt" => {:cli_auto_prompt, :boolean},
    "no-cli-auto-prompt" => {:no_cli_auto_prompt, :boolean},
    "no-paginate" => {:no_paginate, :boolean},
    "starting-token" => {:starting_token, :string},
    "page-size" => {:page_size, :integer},
    "max-items" => {:max_items, :integer},
    "cli-input-json" => {:cli_input_json, :string},
    "cli-input-yaml" => {:cli_input_yaml, :string},
    "generate-cli-skeleton" => {:generate_cli_skeleton, :optional},
    "help" => {:help, :boolean},
    "version" => {:version, :boolean}
  }

  def globals, do: @globals

  def flags(operation) do
    case OperationCatalog.operation(operation) do
      {:ok, operation} ->
        Map.new(members(operation), fn {member, binding} ->
          {flag(member), {member, flag_type(model(binding))}}
        end)

      {:error, _} ->
        %{}
    end
  end

  def parse(operation_name, argv, opts \\ []) do
    with {:ok, operation} <- OperationCatalog.operation(operation_name) do
      if "--help" in argv or argv == ["help"] do
        {:ok, %{input: %{}, options: %{help: true}, args: []}}
      else
        with {:ok, raw, options, args} <- scan(argv, flags(operation_name), %{}, %{}, []),
             :ok <- globals_valid(options),
             {:ok, options} <- resolve_options(options, opts),
             {:ok, json_input} <- json_input(options, opts),
             {:ok, input} <- decode_members(raw, operation, options, opts),
             input = Map.merge(json_input, input),
             {:ok, input} <- blobs(input, model(operation["input"]), format(options, opts), opts),
             {:ok, args} <-
               AutoPrompt.arguments(
                 :s3api,
                 [OperationCatalog.command_name(operation_name)],
                 args,
                 options,
                 opts
               ),
             {:ok, input} <- AutoPrompt.run(operation_name, input, options, opts),
             {:ok, options} <- body_option(input, operation, options),
             :ok <- positional(operation_name, args, options),
             {:ok, input} <- validated(operation_name, input, options) do
          {:ok, %{input: input, options: options, args: args}}
        end
      end
    end
  end

  @doc "Resolves nonsecret CLI settings before decoding blobs, without credentials or network access."
  def resolve_options(options, opts \\ []) do
    if options[:help] || options[:generate_cli_skeleton] do
      {:ok, options}
    else
      env = Keyword.get(opts, :env, System.get_env())
      profile = options[:profile] || env["AWS_PROFILE"] || env["AWS_DEFAULT_PROFILE"] || "default"

      with {:ok, settings} <- profile_settings(profile, opts),
           format =
             options[:cli_binary_format] || opts[:cli_binary_format] ||
               settings["cli_binary_format"] || "base64",
           true <- format in ~w(base64 raw-in-base64-out),
           true <-
             Enum.all?(
               ~w(cli_auto_prompt cli_pager),
               &(is_nil(settings[&1]) or is_binary(settings[&1]))
             ) do
        {:ok,
         options
         |> put_resolved(
           :cli_binary_format,
           opts[:cli_binary_format] || settings["cli_binary_format"]
         )
         |> put_resolved(
           :cli_auto_prompt_mode,
           env["AWS_CLI_AUTO_PROMPT"] || settings["cli_auto_prompt"]
         )
         |> put_resolved(:cli_pager, settings["cli_pager"])}
      else
        {:error, _} = error ->
          error

        _ ->
          {:error,
           Error.new(:configuration, "Invalid nonsecret AWS CLI settings",
             code: :invalid_shared_config
           )}
      end
    end
  end

  defp put_resolved(options, _key, nil), do: options
  defp put_resolved(options, key, value), do: Map.put_new(options, key, value)

  defp profile_settings(profile, opts) do
    if is_map(opts[:profile_settings]) do
      {:ok, opts[:profile_settings]}
    else
      with {:ok, stores} <- SharedConfig.load(Keyword.put(opts, :load_credentials, false)),
           do: {:ok, stores.config[profile] || %{}}
    end
  end

  @doc false
  def prompt_value(binding, value, options, opts) do
    shape = model(binding)
    value = if shape["type"] == "list", do: [value], else: value

    with {:ok, decoded} <- decode_value(value, shape, options, opts),
         do: blobs(decoded, shape, format(options, opts), opts)
  end

  defp scan([], _flags, input, options, args), do: {:ok, input, options, Enum.reverse(args)}

  defp scan(["--" | rest], _flags, input, options, args),
    do: {:ok, input, options, Enum.reverse(args) ++ rest}

  defp scan(["--" <> option | rest], flags, input, options, args) do
    {name, inline} =
      case String.split(option, "=", parts: 2) do
        [name] -> {name, nil}
        [name, value] -> {name, value}
      end

    case Map.get(@globals, name) do
      {key, type} ->
        with {:ok, value, remaining} <- take(type, inline, rest),
             do: scan(remaining, flags, input, Map.put(options, key, value), args)

      nil ->
        {name, negated} =
          if String.starts_with?(name, "no-"),
            do: {String.replace_prefix(name, "no-", ""), true},
            else: {name, false}

        case Map.get(flags, name) do
          {member, type} when not negated or type == :boolean ->
            with {:ok, value, remaining} <- take(type, inline, rest),
                 do:
                   scan(
                     remaining,
                     flags,
                     Map.put(input, member, if(negated, do: false, else: value)),
                     options,
                     args
                   )

          _ ->
            invalid(:unknown_option, "Unknown option for this S3 operation")
        end
    end
  end

  defp scan(["-" <> _ = value | rest], flags, input, options, args) when value == "-",
    do: scan(rest, flags, input, options, [value | args])

  defp scan(["-" <> _ | _], _flags, _input, _options, _args),
    do: invalid(:unknown_option, "Unknown S3 option")

  defp scan([arg | rest], flags, input, options, args),
    do: scan(rest, flags, input, options, [arg | args])

  defp take(:boolean, nil, rest), do: {:ok, true, rest}

  defp take(:boolean, _, _),
    do: invalid(:invalid_parameter, "Boolean options do not take a value")

  defp take(:optional, nil, [value | rest]) when value in ~w(input output yaml-input),
    do: {:ok, value, rest}

  defp take(:optional, nil, rest), do: {:ok, "input", rest}
  defp take(:optional, value, rest), do: {:ok, value, rest}

  defp take(:list, nil, rest) do
    {values, remaining} = Enum.split_while(rest, &(not String.starts_with?(&1, "--")))

    if values == [],
      do: invalid(:missing_parameter, "Modeled list option requires a value"),
      else: {:ok, values, remaining}
  end

  defp take(:list, inline, rest), do: {:ok, [inline], rest}

  defp take(type, nil, [value | rest]) do
    if String.starts_with?(value, "--"),
      do: invalid(:missing_parameter, "S3 option requires a value"),
      else: take(type, value, rest)
  end

  defp take(_type, nil, []), do: invalid(:missing_parameter, "S3 option requires a value")

  defp take(:integer, value, rest) do
    case Integer.parse(value) do
      {number, ""} -> {:ok, number, rest}
      _ -> invalid(:invalid_parameter, "S3 integer option is invalid")
    end
  end

  defp take(_type, value, rest), do: {:ok, value, rest}

  defp decode_members(raw, operation, options, opts) do
    Enum.reduce_while(raw, {:ok, %{}}, fn {name, value}, {:ok, input} ->
      shape = model(members(operation)[name])

      case decode_value(value, shape, options, opts) do
        {:ok, decoded} -> {:cont, {:ok, Map.put(input, name, decoded)}}
        error -> {:halt, error}
      end
    end)
  end

  defp decode_value(value, %{"streaming" => true, "type" => "blob"}, _options, _opts)
       when is_binary(value),
       do:
         {:ok,
          value
          |> String.replace_prefix("fileb://", "")
          |> String.replace_prefix("file://", "")
          |> expand_path()}

  defp decode_value(values, %{"type" => "list"} = shape, options, opts) when is_list(values) do
    case values do
      [one] ->
        with {:ok, loaded} <- load(one, opts) do
          if is_binary(loaded) and String.starts_with?(String.trim_leading(loaded), "[") do
            json(loaded)
          else
            decode_list(values, shape, options, opts)
          end
        end

      _ ->
        decode_list(values, shape, options, opts)
    end
  end

  defp decode_value(value, %{"type" => type} = shape, _options, opts)
       when type in ~w(structure map) do
    with {:ok, loaded} when is_binary(loaded) <- load(value, opts),
         true <- String.valid?(loaded) do
      if is_binary(loaded) and String.starts_with?(String.trim_leading(loaded), "{") do
        json(loaded)
      else
        with {:ok, parsed} <-
               Shorthand.parse(loaded, Keyword.put(opts, :resolve_file, &load(&1, opts))),
             do: coerce(shape, parsed)
      end
    else
      {:error, _} = error -> error
      _ -> invalid(:invalid_parameter, "Structured input requires UTF8 text")
    end
  end

  defp decode_value(value, shape, _options, opts) do
    with {:ok, loaded} <- load(value, opts), do: scalar(shape, loaded)
  end

  defp decode_list(values, shape, options, opts) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, items} ->
      case decode_value(value, model(shape["member"]), options, opts) do
        {:ok, item} -> {:cont, {:ok, [item | items]}}
        error -> {:halt, error}
      end
    end)
    |> reverse_result()
  end

  defp coerce(%{"type" => "structure"} = shape, value) when is_map(value) do
    map_values(value, fn name, item ->
      case shape["members"][name] do
        nil -> invalid(:invalid_parameter, "Unknown modeled shorthand member")
        binding -> coerce(model(binding), item)
      end
    end)
  end

  defp coerce(%{"type" => "map"} = shape, value) when is_map(value),
    do: map_values(value, fn _name, item -> coerce(model(shape["value"]), item) end)

  defp coerce(%{"type" => "list"} = shape, value) do
    Enum.reduce_while(List.wrap(value), {:ok, []}, fn item, {:ok, items} ->
      case coerce(model(shape["member"]), item) do
        {:ok, item} -> {:cont, {:ok, [item | items]}}
        error -> {:halt, error}
      end
    end)
    |> reverse_result()
  end

  defp coerce(shape, value), do: scalar(shape, value)

  defp scalar(%{"type" => type}, value) when type in ~w(integer long) and is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {number, ""} -> {:ok, number}
      _ -> invalid(:invalid_parameter, "Modeled integer parameter is invalid")
    end
  end

  defp scalar(%{"type" => type}, value) when type in ~w(float double) and is_binary(value) do
    case Float.parse(String.trim(value)) do
      {number, ""} -> {:ok, number}
      _ -> invalid(:invalid_parameter, "Modeled floating point parameter is invalid")
    end
  end

  defp scalar(%{"type" => "boolean"}, value) when is_binary(value),
    do: {:ok, String.downcase(value) != "false"}

  defp scalar(%{"type" => "timestamp"} = shape, {tag, value})
       when tag in [:yaml_date, :yaml_timestamp] and is_binary(value),
       do: scalar(shape, value)

  defp scalar(%{"type" => "timestamp"}, value) when is_binary(value) do
    value = String.trim(value)

    cond do
      match?({:ok, _, _}, DateTime.from_iso8601(value)) ->
        {:ok, value}

      match?({:ok, _}, NaiveDateTime.from_iso8601(value)) ->
        {:ok, naive} = NaiveDateTime.from_iso8601(value)
        {:ok, naive |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_iso8601()}

      match?({:ok, _}, Date.from_iso8601(value)) ->
        {:ok, value <> "T00:00:00Z"}

      match?({_number, ""}, Integer.parse(value)) ->
        {number, ""} = Integer.parse(value)
        {:ok, number}

      match?({_number, ""}, Float.parse(value)) ->
        {number, ""} = Float.parse(value)
        {:ok, number}

      true ->
        rfc_timestamp(value)
    end
  end

  defp scalar(_shape, value), do: {:ok, value}

  defp rfc_timestamp(value) do
    months = %{
      "Jan" => 1,
      "Feb" => 2,
      "Mar" => 3,
      "Apr" => 4,
      "May" => 5,
      "Jun" => 6,
      "Jul" => 7,
      "Aug" => 8,
      "Sep" => 9,
      "Oct" => 10,
      "Nov" => 11,
      "Dec" => 12
    }

    with [_, day, month, year, hour, minute, second] <-
           Regex.run(
             ~r/\A(?:[A-Za-z]{3},\s*)?(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\s+(\d{2}):(\d{2}):(\d{2})\s+(?:GMT|UTC)\z/,
             value
           ),
         month when is_integer(month) <- months[month],
         {:ok, date} <- Date.new(String.to_integer(year), month, String.to_integer(day)),
         {:ok, time} <-
           Time.new(String.to_integer(hour), String.to_integer(minute), String.to_integer(second)),
         {:ok, datetime} <- DateTime.new(date, time, "Etc/UTC") do
      {:ok, DateTime.to_iso8601(datetime)}
    else
      _ -> invalid(:invalid_parameter, "Timestamp must be ISO 8601, RFC1123 UTC, or Unix seconds")
    end
  end

  defp blobs(input, %{"type" => "structure"} = shape, format, opts) when is_map(input),
    do:
      map_values(input, fn name, value ->
        blobs(value, model(shape["members"][name]), format, opts)
      end)

  defp blobs(input, %{"type" => "map"} = shape, format, opts) when is_map(input),
    do:
      map_values(input, fn _name, value -> blobs(value, model(shape["value"]), format, opts) end)

  defp blobs(input, %{"type" => "list"} = shape, format, opts) when is_list(input) do
    Enum.reduce_while(input, {:ok, []}, fn value, {:ok, values} ->
      case blobs(value, model(shape["member"]), format, opts) do
        {:ok, value} -> {:cont, {:ok, [value | values]}}
        error -> {:halt, error}
      end
    end)
    |> reverse_result()
  end

  defp blobs(value, %{"type" => "blob", "streaming" => true}, _format, _opts), do: {:ok, value}
  defp blobs(value, %{"type" => "timestamp"} = shape, _format, _opts), do: scalar(shape, value)
  defp blobs({:binary_file, bytes}, %{"type" => "blob"}, _format, _opts), do: {:ok, bytes}

  defp blobs(value, %{"type" => "blob"}, "base64", _opts) when is_binary(value) do
    case Base.decode64(value) do
      {:ok, bytes} -> {:ok, bytes}
      _ -> invalid(:invalid_blob, "Modeled blob parameter must contain valid base64")
    end
  end

  defp blobs(value, _shape, _format, _opts), do: {:ok, value}

  defp json_input(options, opts) do
    case {options[:cli_input_json], options[:cli_input_yaml]} do
      {nil, nil} ->
        {:ok, %{}}

      {value, nil} ->
        with {:ok, bytes} <- input_text(value, opts),
             {:ok, input} when is_map(input) <- json(bytes),
             do: {:ok, input},
             else: (_ -> invalid(:invalid_input_json, "CLI input JSON must be an object"))

      {nil, value} ->
        with {:ok, bytes} <- input_text(value, opts),
             {:ok, input} <-
               ExStorageServiceCli.YAML.decode(bytes,
                 max_bytes: opts[:max_input_bytes] || @max_input
               ) do
          if is_map(input),
            do: {:ok, input},
            else: invalid(:invalid_input_yaml, "CLI input YAML must be an object")
        end

      _ ->
        invalid(:invalid_input, "Only one CLI input JSON or YAML option may be specified")
    end
  end

  defp input_text(value, opts) do
    case load(value, opts) do
      {:ok, {:binary_file, bytes}} -> {:ok, bytes}
      result -> result
    end
  end

  defp load("fileb://" <> path, opts) do
    with {:ok, bytes} <- read_file(path, opts), do: {:ok, {:binary_file, bytes}}
  end

  defp load("file://" <> path, opts), do: read_file(path, opts)
  defp load(value, _opts), do: {:ok, value}

  defp read_file(path, opts) do
    path = expand_path(path)
    limit = opts[:max_input_bytes] || @max_input

    with {:ok, %{type: :regular, size: size}} when size <= limit <- File.stat(path),
         {:ok, file} <- File.open(path, [:read, :binary, :raw]) do
      try do
        case :file.read(file, limit + 1) do
          {:ok, bytes} when byte_size(bytes) <= limit -> {:ok, bytes}
          :eof -> {:ok, ""}
          _ -> invalid(:input_file_limit, "Input file exceeds its bounded size")
        end
      after
        File.close(file)
      end
    else
      _ -> invalid(:input_file, "Input file is unavailable or exceeds its bounded size")
    end
  end

  defp expand_path(path) do
    path =
      Regex.replace(~r/\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)/, path, fn full,
                                                                                            braces,
                                                                                            plain ->
        System.get_env(if(braces == "", do: plain, else: braces)) || full
      end)

    if path == "-", do: path, else: Path.expand(path)
  end

  defp json(value) when is_binary(value) do
    case JSON.decode(value) do
      {:ok, value} -> {:ok, value}
      _ -> invalid(:invalid_input_json, "Invalid JSON input")
    end
  end

  defp json(_), do: invalid(:invalid_input_json, "JSON input must be text")

  defp body_option(input, operation, options) do
    body =
      Enum.find(members(operation), fn {_name, binding} ->
        binding["streaming"] == true and model(binding)["type"] == "blob"
      end)

    case body do
      {member, _} ->
        case Map.fetch(input, member) do
          {:ok, "-"} ->
            invalid(:unsupported_stdin, "Streaming stdin requires a replayable spool")

          {:ok, path} when is_binary(path) ->
            {:ok, Map.put(options, :streaming_body_path, expand_path(path))}

          :error ->
            {:ok, options}

          _ ->
            invalid(:invalid_parameter, "Streaming body must be a file path")
        end

      _ ->
        {:ok, options}
    end
  end

  defp positional(operation, args, options) do
    name = OperationCatalog.operation_name(operation)

    allowed =
      if name in ["GetObject", "GetObjectTorrent", "SelectObjectContent"] and
           is_nil(options[:generate_cli_skeleton]),
         do: 1,
         else: 0

    if length(args) == allowed,
      do: :ok,
      else: invalid(:invalid_arguments, "Invalid S3 operation positional arguments")
  end

  defp validated(_operation, input, %{generate_cli_skeleton: "input"}), do: {:ok, input}
  defp validated(_operation, input, %{generate_cli_skeleton: "yaml-input"}), do: {:ok, input}
  defp validated(operation, input, _options), do: Input.validate(operation, input)

  defp globals_valid(options) do
    choices = [
      output: ~w(json text table yaml yaml-stream),
      color: ~w(on off auto),
      cli_binary_format: ~w(base64 raw-in-base64-out),
      generate_cli_skeleton: ~w(input output yaml-input)
    ]

    invalid? =
      Enum.any?(choices, fn {key, values} ->
        Map.has_key?(options, key) and options[key] not in values
      end) or
        Enum.any?([:cli_read_timeout, :cli_connect_timeout, :page_size, :max_items], fn key ->
          Map.has_key?(options, key) and options[key] < 0
        end)

    if invalid?, do: invalid(:invalid_global_option, "AWS global option is invalid"), else: :ok
  end

  defp format(options, opts),
    do: options[:cli_binary_format] || opts[:cli_binary_format] || "base64"

  defp members(operation), do: (model(operation["input"]) || %{})["members"] || %{}
  defp model(nil), do: nil
  defp model(binding), do: Map.merge(OperationCatalog.shape(binding["shape"]) || %{}, binding)
  defp flag_type(%{"type" => "boolean"}), do: :boolean
  defp flag_type(%{"type" => "list"}), do: :list
  defp flag_type(_shape), do: :modeled

  defp flag(member),
    do:
      member
      |> String.replace(~r/([A-Z]+)([A-Z][a-z])/, "\\1-\\2")
      |> String.replace(~r/([a-z0-9])([A-Z])/, "\\1-\\2")
      |> String.downcase()

  defp map_values(map, function) do
    Enum.reduce_while(map, {:ok, %{}}, fn {name, value}, {:ok, mapped} ->
      case function.(name, value) do
        {:ok, value} -> {:cont, {:ok, Map.put(mapped, name, value)}}
        error -> {:halt, error}
      end
    end)
  end

  defp reverse_result({:ok, items}), do: {:ok, Enum.reverse(items)}
  defp reverse_result(error), do: error
  defp invalid(code, message), do: {:error, Error.new(:syntax, message, code: code)}
end
