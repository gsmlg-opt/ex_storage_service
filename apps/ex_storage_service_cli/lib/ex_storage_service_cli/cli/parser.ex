defmodule ExStorageServiceCli.CLI.Parser do
  @moduledoc "Strict, pure argument parsing. Help never loads configuration or credentials."
  alias ExStorageServiceCli.{Error, CLI.Invocation, CLI.Registry}

  def parse(argv) do
    if frontend_hint(argv) == :s3api, do: parse_api(argv), else: parse_common(argv)
  end

  defp parse_common(argv) do
    frontend = frontend_hint(argv)

    with {:ok, words, options} <- scan(argv, Registry.options(frontend), [], [], false, frontend),
         {:ok, invocation} <- command(words, options),
         {:ok, invocation} <- prepare(invocation),
         :ok <- validate(invocation, options) do
      {:ok, invocation}
    else
      {:error, %Error{} = error} ->
        {:error, %{error | details: Map.put(error.details, :frontend, frontend_hint(argv))}}
    end
  end

  defp prepare(%Invocation{frontend: :s3, help: false} = invocation) do
    alias ExStorageServiceCli.CLI.{AWSInput, AutoPrompt}
    {arity, _, _} = Registry.spec(:s3, invocation.command)
    missing = length(invocation.args) < arity.first

    with {:ok, options} <- AWSInput.resolve_options(invocation.options),
         {:ok, mode} <- AutoPrompt.mode(options),
         {:ok, args} <- AutoPrompt.arguments(:s3, invocation.command, invocation.args, options),
         :ok <-
           if(mode == "on" or (mode == "on-partial" and missing),
             do: AutoPrompt.confirm("s3 " <> Enum.join(invocation.command, " ")),
             else: :ok
           ) do
      {:ok, %{invocation | options: options, args: args}}
    end
  end

  defp prepare(%Invocation{frontend: :native, help: false} = invocation) do
    with {:ok, options} <- ExStorageServiceCli.CLI.NativeGlobals.prepare(invocation.options),
         do: {:ok, %{invocation | options: options}}
  end

  defp prepare(invocation), do: {:ok, invocation}

  defp parse_api(argv) do
    alias ExStorageServiceCli.{CLI.AWSInput, S3.OperationCatalog}
    {prefix, remaining} = split_api(argv, [])
    prefix = Enum.map(prefix, fn value -> if value == "help", do: "--help", else: value end)

    case remaining do
      [] ->
        {:ok, %Invocation{frontend: :s3api, help: true}}

      ["help"] ->
        {:ok, %Invocation{frontend: :s3api, help: true}}

      ["wait"] ->
        {:ok, %Invocation{frontend: :s3api, command: ["wait"], help: true}}

      ["wait", waiter | args] ->
        model =
          Enum.find(OperationCatalog.waiters(), fn {name, _} -> waiter_name(name) == waiter end)

        if model do
          {name, model} = model

          api_input(AWSInput.parse(model["operation"], prefix ++ args), ["wait", waiter], %{
            waiter: name
          })
        else
          api_error("Unknown S3 waiter")
        end

      [command | args] ->
        api_input(AWSInput.parse(command, prefix ++ args), [command], %{})
    end
  end

  defp api_input({:ok, parsed}, command, extra) do
    options = parsed.options |> Map.merge(extra) |> Map.put(:api_input, parsed.input)

    {:ok,
     %Invocation{
       frontend: :s3api,
       command: command,
       options: options,
       args: parsed.args,
       help: options[:help] || false
     }}
  end

  defp api_input({:error, error}, _command, _extra),
    do: {:error, %{error | details: Map.put(error.details, :frontend, :s3api)}}

  defp api_error(message), do: {:error, Error.new(:syntax, message, details: %{frontend: :s3api})}

  defp waiter_name(name),
    do: name |> String.replace(~r/([a-z0-9])([A-Z])/, "\\1-\\2") |> String.downcase()

  defp split_api(["s3api" | rest], prefix), do: {Enum.reverse(prefix), rest}

  defp split_api(["--" <> flag = word, value | rest], prefix) do
    [name | inline] = String.split(flag, "=", parts: 2)

    case Registry.globals(:s3api)[name] do
      {_key, type} when type != :boolean and inline == [] ->
        split_api(rest, [value, word | prefix])

      _ ->
        split_api([value | rest], [word | prefix])
    end
  end

  defp split_api([word | rest], prefix), do: split_api(rest, [word | prefix])
  defp split_api([], prefix), do: {Enum.reverse(prefix), []}

  defp scan([], _table, words, options, _ended, _frontend),
    do: {:ok, Enum.reverse(words), Enum.reverse(options)}

  defp scan(["--" | rest], table, words, options, false, frontend),
    do: scan(rest, table, words, options, true, frontend)

  defp scan([word | rest], table, words, options, true, frontend),
    do: scan(rest, table, [word | words], options, true, frontend)

  defp scan(["-" | rest], table, words, options, false, frontend),
    do: scan(rest, table, ["-" | words], options, false, frontend)

  defp scan(["--" <> flag | rest], table, words, options, false, frontend) do
    {name, inline} =
      case String.split(flag, "=", parts: 2) do
        [name] -> {name, nil}
        [name, value] -> {name, value}
      end

    with {:ok, key, type, negated} <- option(table, name),
         {:ok, value, remaining} <- value(type, name, inline, rest, negated),
         :ok <- if(frontend == :s3, do: :ok, else: conflict(options, key, type, value, name)) do
      scan(remaining, table, words, [{name, key, value} | options], false, frontend)
    end
  end

  defp scan(["-" <> short | rest], table, words, options, false, frontend) do
    with {:ok, expanded} <- expand_short(short, table) do
      scan(expanded ++ rest, table, words, options, false, frontend)
    end
  end

  defp scan([word | rest], table, words, options, false, frontend),
    do: scan(rest, table, [word | words], options, false, frontend)

  defp option(table, name) do
    case Map.fetch(table, name) do
      {:ok, {key, type}} ->
        {:ok, key, type, false}

      :error ->
        case String.replace_prefix(name, "no-", "") |> then(&Map.fetch(table, &1)) do
          {:ok, {key, :boolean}} when name != "" ->
            if String.starts_with?(name, "no-"),
              do: {:ok, key, :boolean, true},
              else: invalid("Unknown option --#{safe_name(name)}")

          _ ->
            invalid("Unknown option --#{safe_name(name)}")
        end
    end
  end

  defp value(:boolean, _name, nil, rest, negated), do: {:ok, not negated, rest}

  defp value(:boolean, _name, value, rest, false) when value in ["true", "false"],
    do: {:ok, value == "true", rest}

  defp value(:boolean, name, _inline, _rest, _negated),
    do: invalid("Option --#{safe_name(name)} does not take a value")

  defp value({:flag, value}, _name, nil, rest, false), do: {:ok, value, rest}

  defp value({:flag, _}, name, _, _, _),
    do: invalid("Option --#{safe_name(name)} does not take a value")

  defp value({:optional_string, default, choices}, name, nil, rest, negated) do
    case rest do
      [next | remaining] ->
        if String.starts_with?(next, "-") and next != "-",
          do: {:ok, default, rest},
          else: value({:enum, choices}, name, next, remaining, negated)

      [] ->
        {:ok, default, []}
    end
  end

  defp value({:optional_string, _, choices}, name, inline, rest, negated),
    do: value({:enum, choices}, name, inline, rest, negated)

  defp value(:string_list, name, nil, rest, _) do
    {values, remaining} = Enum.split_while(rest, &(not String.starts_with?(&1, "-") or &1 == "-"))

    if values == [],
      do: invalid("Missing value for --#{safe_name(name)}"),
      else: {:ok, values, remaining}
  end

  defp value(:string_list, _name, inline, rest, _), do: {:ok, [inline], rest}

  defp value(type, name, nil, [value | rest], negated) do
    if String.starts_with?(value, "--"),
      do: invalid("Missing value for --#{safe_name(name)}"),
      else: value(type, name, value, rest, negated)
  end

  defp value(_type, name, nil, [], _negated),
    do: invalid("Missing value for --#{safe_name(name)}")

  defp value(type, _name, value, rest, _negated) when type in [:string, :repeated_string],
    do: {:ok, value, rest}

  defp value({:enum, choices}, name, value, rest, _) do
    if choices == [] or value in choices,
      do: {:ok, value, rest},
      else: invalid("Invalid choice for --#{safe_name(name)}")
  end

  defp value(:integer, name, value, rest, _negated) do
    case Integer.parse(value) do
      {number, ""} -> {:ok, number, rest}
      _ -> invalid("Invalid integer for --#{safe_name(name)}")
    end
  end

  defp conflict(_options, _key, :repeated_string, _value, _name), do: :ok

  defp conflict(options, key, _type, value, name) do
    if Enum.any?(options, fn {_, existing_key, existing_value} ->
         existing_key == key and existing_value != value
       end),
       do: invalid("Conflicting values for --#{safe_name(name)}"),
       else: :ok
  end

  defp command(["help" | rest], options), do: command(rest, [{"help", :help, true} | options])

  defp command(words, options) do
    opts = Map.new(options, fn {_, key, value} -> {key, value} end)
    headers = for {_, :custom_headers, value} <- options, do: value
    opts = if headers == [], do: opts, else: Map.put(opts, :custom_headers, headers)
    resolutions = for {_, :resolve, value} <- options, do: value
    opts = if resolutions == [], do: opts, else: Map.put(opts, :resolve, resolutions)
    storage_classes = for {_, :exclude_storageclass, value} <- options, do: value

    opts =
      if storage_classes == [],
        do: opts,
        else: Map.put(opts, :exclude_storageclass, storage_classes)

    {frontend, remaining} =
      case words do
        ["s3" | rest] -> {:s3, rest}
        ["s3api" | rest] -> {:s3api, rest}
        rest -> {:native, rest}
      end

    {remaining, opts} =
      if frontend != :native and List.last(remaining) == "help",
        do: {Enum.drop(remaining, -1), Map.put(opts, :help, true)},
        else: {remaining, opts}

    remaining = if opts[:version] && remaining == [], do: ["version"], else: remaining

    resolved =
      if remaining == ["version"] and opts[:version],
        do: {["version"], [], Registry.spec(frontend, ["version"])},
        else: Registry.resolve(frontend, remaining)

    case resolved do
      nil when remaining == [] ->
        {:ok, %Invocation{frontend: frontend, options: opts, help: true}}

      nil ->
        if opts[:help] == true and Registry.group?(frontend, remaining),
          do:
            {:ok, %Invocation{frontend: frontend, command: remaining, options: opts, help: true}},
          else: invalid("Unknown command; use ess help")

      {path, args, _spec} ->
        {:ok,
         %Invocation{
           frontend: frontend,
           command: path,
           args: args,
           options: opts,
           filters:
             for({_, key, value} <- options, key in [:include, :exclude], do: {key, value}),
           help: opts[:help] || false
         }}
    end
  end

  defp validate(invocation, options) do
    {arity, command_options, _summary} =
      Registry.spec(invocation.frontend, invocation.command)

    allowed = Map.merge(Registry.globals(invocation.frontend), command_options)
    allowed_keys = for {_, {key, _}} <- allowed, do: key

    invalid_option =
      Enum.find(options, fn {name, key, _value} ->
        key not in allowed_keys or
          (not Map.has_key?(allowed, name) and not String.starts_with?(name, "no-"))
      end)

    cond do
      invalid_option != nil ->
        {name, _, _} = invalid_option
        invalid("Option --#{safe_name(name)} is not valid for this command")

      invocation.help ->
        :ok

      invalid_resources?(invocation.options) ->
        invalid("Resource bounds must be positive integers")

      tls_pair_missing?(invocation.options) ->
        invalid("Client certificate and key must be specified together")

      invocation.options[:json] == true and invocation.options[:output] not in [nil, "json"] ->
        invalid("Legacy JSON flag conflicts with the selected output")

      invocation.options[:cli_auto_prompt] == true and
          invocation.options[:no_cli_auto_prompt] == true ->
        invalid("Conflicting CLI auto-prompt options")

      length(invocation.args) not in arity ->
        invalid("Invalid number of command arguments; use ess help")

      true ->
        :ok
    end
  end

  defp invalid_resources?(options),
    do:
      Enum.any?([:max_requests, :memory_budget, :max_concurrency], fn key ->
        value = options[key]
        value != nil and (not is_integer(value) or value <= 0)
      end)

  defp tls_pair_missing?(options),
    do: options[:client_cert] == nil != (options[:client_key] == nil)

  defp expand_short("", _table), do: {:ok, []}

  defp expand_short(<<letter, rest::binary>>, table) do
    case Map.fetch(Registry.short_options(), <<letter>>) do
      {:ok, name} ->
        case table[name] do
          {_key, :boolean} ->
            with {:ok, remaining} <- expand_short(rest, table),
                 do: {:ok, ["--" <> name | remaining]}

          {_key, {:flag, _}} ->
            with {:ok, remaining} <- expand_short(rest, table),
                 do: {:ok, ["--" <> name | remaining]}

          {_key, _type} ->
            {:ok, ["--" <> name] ++ if(rest == "", do: [], else: [rest])}

          nil ->
            invalid("Unknown short option")
        end

      :error ->
        invalid("Unknown short option")
    end
  end

  defp frontend_hint(["help" | rest]), do: frontend_hint(rest)
  defp frontend_hint(["s3" | _]), do: :s3
  defp frontend_hint(["s3api" | _]), do: :s3api

  defp frontend_hint(["--" <> flag | rest]) when flag != "" do
    [name | inline] = String.split(flag, "=", parts: 2)

    case option(Registry.all_options(), name) do
      {:ok, _key, type, _negated} when inline == [] ->
        if takes_value?(type), do: frontend_hint(Enum.drop(rest, 1)), else: frontend_hint(rest)

      _ ->
        frontend_hint(rest)
    end
  end

  defp frontend_hint(["-" <> short | rest]) when short != "" do
    case expand_short(short, Registry.all_options()) do
      {:ok, expanded} -> frontend_hint(expanded ++ rest)
      _ -> :native
    end
  end

  defp frontend_hint(_argv), do: :native

  defp takes_value?(:boolean), do: false
  defp takes_value?({:flag, _}), do: false
  defp takes_value?(_), do: true

  defp safe_name(name), do: String.replace(name, ~r/[^a-zA-Z0-9-]/, "?") |> String.slice(0, 80)
  defp invalid(message), do: {:error, Error.new(:syntax, message)}
end
