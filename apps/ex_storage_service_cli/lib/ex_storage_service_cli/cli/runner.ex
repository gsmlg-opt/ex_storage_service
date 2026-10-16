defmodule ExStorageServiceCli.CLI.Runner do
  @moduledoc "Owns invocation dispatch; handlers return and never terminate the BEAM."
  alias ExStorageServiceCli.{Aliases, Config, Context, Error, Output, Presenter}

  alias ExStorageServiceCli.CLI.{
    HighLevel,
    Invocation,
    Management,
    NativeUtility,
    NativeGlobals,
    OptionHelp,
    Parser,
    Registry,
    Resolver,
    Runtime
  }

  alias ExStorageServiceCli.Commands

  @legacy %{
    "configure" => Commands.Configure,
    "mb" => Commands.Bucket,
    "rb" => Commands.Bucket,
    "ls" => Commands.Ls,
    "tree" => Commands.Tree,
    "cp" => Commands.Cp,
    "rm" => Commands.Rm,
    "mv" => Commands.Mv,
    "presign" => Commands.Presign,
    "info" => Commands.Info
  }

  def run(argv) do
    with {:ok, invocation} <- Parser.parse(argv), do: execute(invocation)
  end

  def execute(%Invocation{frontend: :native} = invocation),
    do: Output.with_options(invocation.options, fn -> execute_with_options(invocation) end)

  def execute(invocation), do: execute_with_options(invocation)

  defp execute_with_options(invocation) do
    NativeGlobals.warn(invocation)

    case invocation do
      %Invocation{frontend: :native, help: false, options: %{output: "ndjson"}} ->
        ExStorageServiceCli.CLI.NativeEvents.with_invocation(invocation, &execute_invocation/1)

      _ ->
        execute_invocation(invocation)
    end
  end

  defp execute_invocation(%Invocation{help: true} = invocation), do: help(invocation)

  defp execute_invocation(%Invocation{frontend: :s3api} = invocation),
    do: ExStorageServiceCli.CLI.API.run(invocation)

  defp execute_invocation(%Invocation{frontend: :s3} = invocation), do: high_level(invocation)

  defp execute_invocation(%Invocation{command: ["version"], options: options}) do
    cond do
      options[:output] not in [nil, "off"] ->
        present_native(%{version: ExStorageServiceCli.version()}, options)

      options[:json] ->
        IO.puts(JSON.encode!(%{version: ExStorageServiceCli.version()}))

      options[:output] == "off" ->
        :ok

      true ->
        IO.puts("ess version #{ExStorageServiceCli.version()}")
    end
  end

  defp execute_invocation(%Invocation{frontend: :native, command: ["completion"], args: [shell]}) do
    with {:ok, script} <- ExStorageServiceCli.CLI.Completion.generate(shell), do: IO.puts(script)
  end

  defp execute_invocation(%Invocation{frontend: :native, command: ["alias", action]} = invocation) do
    alias_command(action, invocation.args, invocation.options)
  end

  defp execute_invocation(%Invocation{frontend: :native, command: ["sso", action]} = invocation) do
    Runtime.with_invocation(
      fn _runtime ->
        ExStorageServiceCli.CLI.CredentialCommands.run(action, invocation.options)
      end,
      Map.to_list(invocation.options)
    )
  end

  defp execute_invocation(
         %Invocation{frontend: :native, command: ["transfer", action]} = invocation
       ) do
    Runtime.with_invocation(
      fn runtime ->
        with {:ok, data} <-
               ExStorageServiceCli.CLI.TransferCommands.run(
                 action,
                 invocation.args,
                 invocation.options,
                 runtime
               ),
             do: present_native(data, invocation.options)
      end,
      Map.to_list(invocation.options)
    )
  end

  defp execute_invocation(
         %Invocation{frontend: :native, command: ["share", "list", kind]} = invocation
       ) do
    with {:ok, data} <- ExStorageServiceCli.CLI.NativeShare.list(kind, invocation.options),
         do: present_native(data, invocation.options)
  end

  defp execute_invocation(%Invocation{frontend: :native, command: ["share", _]} = invocation),
    do: utility(invocation)

  defp execute_invocation(
         %Invocation{frontend: :native, command: ["admin", "info"]} = invocation
       ),
       do: utility(invocation)

  defp execute_invocation(%Invocation{frontend: :native, command: [name]} = invocation)
       when name in ~w(doctor ping ready sql presign),
       do: utility(invocation)

  defp execute_invocation(%Invocation{frontend: :native, command: ["watch"]} = invocation),
    do: watch(invocation)

  defp execute_invocation(
         %Invocation{frontend: :native, command: ["ls"], options: %{versions: true}} = invocation
       ),
       do: high_level(invocation)

  defp execute_invocation(%Invocation{frontend: :native, command: [name]} = invocation)
       when name in ["ls", "tree"] do
    if Map.has_key?(invocation.options, :rewind) or
         (name == "tree" and
            (Map.has_key?(invocation.options, :depth) or Map.has_key?(invocation.options, :files))) do
      high_level(invocation)
    else
      if Enum.any?(
           [:output, :limit, :incomplete, :summarize, :storage_class],
           &Map.has_key?(invocation.options, &1)
         ),
         do: listing(invocation),
         else:
           Runtime.with_invocation(
             fn runtime -> legacy_execute(name, invocation, runtime) end,
             Map.to_list(invocation.options)
           )
    end
  end

  defp execute_invocation(
         %Invocation{frontend: :native, command: ["mb"], options: options} = invocation
       )
       when is_map_key(options, :ignore_existing) or is_map_key(options, :with_lock) or
              is_map_key(options, :with_versioning),
       do: high_level(invocation)

  defp execute_invocation(
         %Invocation{frontend: :native, command: [name], options: %{output: _}} = invocation
       )
       when name in ["mb", "rb", "rm"],
       do: high_level(invocation)

  defp execute_invocation(
         %Invocation{frontend: :native, command: [family, _action | _rest]} = invocation
       )
       when family in [
              "version",
              "tag",
              "policy",
              "anonymous",
              "acl",
              "cors",
              "ilm",
              "replicate",
              "event",
              "encrypt",
              "ownership",
              "public-access",
              "lock",
              "retention",
              "legalhold"
            ],
       do: management(invocation)

  defp execute_invocation(%Invocation{frontend: :native, command: ["undo"]} = invocation),
    do: management(invocation)

  defp execute_invocation(%Invocation{frontend: :native, command: ["configure"]} = invocation) do
    context = %Context{
      profile: invocation.options[:profile] || "default",
      json: invocation.options[:json] || false,
      options: invocation.options
    }

    with {:ok, args} <- legacy_arguments(invocation) do
      Commands.Configure.run("configure", args, Map.from_struct(context)) |> normalize()
    end
  end

  defp execute_invocation(%Invocation{frontend: :native, command: [name]} = invocation)
       when name in [
              "cp",
              "mv",
              "get",
              "put",
              "cat",
              "head",
              "stat",
              "du",
              "find",
              "sync",
              "mirror",
              "diff",
              "pipe"
            ] do
    high_level(invocation)
  end

  defp execute_invocation(%Invocation{frontend: :native, command: [name]} = invocation) do
    if name == "rm" and
         Enum.any?(
           [:recursive, :version_id, :all_versions, :dry_run, :include, :exclude],
           &Map.has_key?(invocation.options, &1)
         ) do
      high_level(invocation)
    else
      Runtime.with_invocation(
        fn runtime -> legacy_execute(name, invocation, runtime) end,
        Map.to_list(invocation.options)
      )
    end
  end

  defp utility(invocation) do
    Runtime.with_invocation(
      fn runtime ->
        target_invocation = %{invocation | args: Enum.take(invocation.args, 1)}

        with {:ok, resolved} <- Resolver.resolve(target_invocation, runtime: runtime) do
          context = hd(resolved.contexts)
          target = List.first(resolved.targets)

          case invocation.command do
            ["admin", "info"] ->
              if context.capabilities[:provider] == "ess",
                do: Commands.Info.run("info", [], Map.from_struct(context)) |> normalize(),
                else:
                  {:error,
                   Error.new(:client, "Admin info requires a declared ESS server",
                     code: :provider_unsupported
                   )}

            ["sql"] ->
              with {:ok, data} <- NativeUtility.run(invocation, target, context) do
                if ExStorageServiceCli.CLI.NativeSQL.stdout?(invocation) do
                  if data["Partial"],
                    do:
                      IO.puts(
                        :stderr,
                        "SQL output is partial: queried #{data["QueriedObjects"]} of #{data["MatchedObjects"]} matching objects"
                      )

                  :ok
                else
                  present_native(data, invocation.options)
                end
              end

            ["presign"] ->
              with {:ok, data} <- NativeUtility.run(invocation, target, context) do
                cond do
                  invocation.options[:output] not in [nil, "off"] ->
                    present_native(data, invocation.options)

                  invocation.options[:output] == "off" ->
                    :ok

                  invocation.options[:json] ->
                    IO.puts(JSON.encode!(Map.delete(data, "effective_expires")))

                  true ->
                    IO.puts(data["url"])
                end
              end

            _ ->
              with {:ok, data} <- NativeUtility.run(invocation, target, context),
                   do: present_native(data, invocation.options)
          end
        end
      end,
      Map.to_list(invocation.options)
    )
  end

  defp present_native(data, %{output: "ndjson"} = options),
    do: ExStorageServiceCli.CLI.NativeEvents.item(data, options)

  defp present_native(data, options),
    do:
      Presenter.write(
        data,
        Map.put_new(options, :output, if(options[:json], do: "json", else: "yaml"))
      )

  defp listing(invocation) do
    Runtime.with_invocation(
      fn runtime ->
        with {:ok, resolved} <- Resolver.resolve(invocation, runtime: runtime) do
          ExStorageServiceCli.CLI.NativeListing.run(
            invocation,
            List.first(resolved.targets),
            hd(resolved.contexts),
            task_supervisor: runtime.tasks
          )
        end
      end,
      Map.to_list(invocation.options)
    )
  end

  defp high_level(invocation) do
    Runtime.with_invocation(
      fn runtime ->
        with {:ok, resolved} <- Resolver.resolve(invocation, runtime: runtime),
             :ok <- raw_output(invocation, resolved.targets) do
          contexts = resolved.contexts
          context = Enum.find(contexts, &(&1 != nil))

          options =
            if context,
              do: Map.merge(context.options, invocation.options),
              else: invocation.options

          options =
            if invocation.frontend == :native and structured?(options),
              do: Map.put(options, :progress, false),
              else: options

          invocation = %{invocation | options: options}
          opts = [task_supervisor: runtime.tasks, stderr: &IO.puts(:stderr, &1)]

          opts =
            if options[:tmp_dir], do: Keyword.put(opts, :tmp_dir, options[:tmp_dir]), else: opts

          opts =
            if invocation.frontend == :native and structured?(options),
              do:
                Keyword.merge(opts,
                  emit: fn _ -> :ok end,
                  on_result: &legacy_transfer_result(&1, invocation)
                ),
              else: opts

          result =
            if invocation.command == ["mirror"] and options[:watch] == true do
              ExStorageServiceCli.CLI.Watch.run(
                invocation,
                hd(resolved.targets),
                hd(contexts),
                Keyword.merge(opts,
                  coordinator: runtime.coordinator,
                  emit: fn _event -> :ok end,
                  on_reconcile: fn ->
                    HighLevel.run(invocation, resolved.targets, contexts, opts)
                  end
                )
              )
            else
              HighLevel.run(invocation, resolved.targets, contexts, opts)
            end

          case result do
            {:ok, data} ->
              present_native(
                data,
                Map.put_new(options, :output, if(options[:json], do: "json", else: "table"))
              )

            :ok when invocation.command == ["rm"] ->
              if structured?(options),
                do:
                  present_native(
                    %{
                      status: "ok",
                      operation: "remove",
                      target: hd(invocation.args),
                      dry_run: options[:dry_run] == true
                    },
                    options
                  ),
                else: :ok

            result ->
              result
          end
        end
      end,
      Map.to_list(invocation.options)
    )
  end

  defp watch(invocation) do
    Runtime.with_invocation(
      fn runtime ->
        with {:ok, %{targets: [target], contexts: [context]}} <-
               Resolver.resolve(invocation, runtime: runtime) do
          opts = [coordinator: runtime.coordinator]

          opts =
            if invocation.options[:tmp_dir],
              do: Keyword.put(opts, :tmp_dir, invocation.options[:tmp_dir]),
              else: opts

          opts =
            if invocation.options[:quiet] || invocation.options[:output] == "off",
              do: Keyword.put(opts, :emit, fn _ -> :ok end),
              else: opts

          ExStorageServiceCli.CLI.Watch.run(invocation, target, context, opts)
        end
      end,
      Map.to_list(invocation.options)
    )
  end

  defp management(invocation) do
    Runtime.with_invocation(
      fn runtime ->
        target_invocation = %{invocation | args: [Management.target_argument(invocation)]}

        with {:ok, %{targets: [target], contexts: [context]}} <-
               Resolver.resolve(target_invocation, runtime: runtime),
             {:ok, result} <- Management.run(invocation, target, context) do
          present_native(
            result,
            Map.put_new(
              invocation.options,
              :output,
              if(
                invocation.options[:json] == true or
                  List.last(invocation.command) in ~w(export get-json),
                do: "json",
                else: "yaml"
              )
            )
          )
        end
      end,
      Map.to_list(invocation.options)
    )
  end

  defp raw_output(invocation, targets) do
    if structured?(invocation.options) and
         (invocation.command in [["cat"], ["head"]] or
            (List.last(targets) && List.last(targets).kind == :stdio)),
       do: {:error, Error.new(:syntax, "Raw stdout cannot be combined with structured output")},
       else: :ok
  end

  defp structured?(options), do: options[:json] == true or options[:output] not in [nil, "off"]

  defp legacy_transfer_result(result, invocation) do
    operation =
      cond do
        result.command == "mv" -> "move"
        result.source.kind in [:local, :stdio] -> "upload"
        result.destination.kind in [:local, :stdio] -> "download"
        true -> "copy"
      end

    destination =
      if operation in ["copy", "move"] and result.destination.kind == :remote,
        do: "s3://" <> result.destination.bucket <> "/" <> result.destination.key,
        else: Enum.at(invocation.args, 1)

    data = %{
      status: "ok",
      operation: operation,
      source: hd(invocation.args),
      destination: destination
    }

    data =
      if operation in ["upload", "download"], do: Map.put(data, :size, result.bytes), else: data

    data =
      if operation == "upload",
        do: Map.put(data, :etag, result[:etag] && String.trim(result.etag, "\"")),
        else: data

    cond do
      invocation.options[:output] == "ndjson" ->
        ExStorageServiceCli.CLI.NativeEvents.item(data, invocation.options)

      invocation.options[:json] ->
        IO.puts(JSON.encode!(data))

      true ->
        Presenter.write(data, invocation.options)
    end
  end

  defp legacy_execute(name, invocation, runtime) do
    with {:ok, module} <- legacy_module(name),
         {:ok, resolved} <- Resolver.resolve(invocation, runtime: runtime),
         {:ok, context} <- legacy_context(resolved),
         {:ok, args} <- legacy_arguments(%{invocation | args: resolved.legacy_args}) do
      module.run(name, args, Map.from_struct(context)) |> normalize()
    end
  end

  defp legacy_module(name) do
    case Map.fetch(@legacy, name) do
      {:ok, module} -> {:ok, module}
      :error -> {:error, Error.new(:syntax, "Unknown native command")}
    end
  end

  defp legacy_context(resolved) do
    contexts = Enum.reject(resolved.contexts, &is_nil/1)

    identities =
      Enum.map(
        contexts,
        &Map.take(&1, [
          :endpoint,
          :region,
          :access_key_id,
          :secret_access_key,
          :session_token,
          :unsigned
        ])
      )

    cond do
      Enum.any?(resolved.targets, &(&1.arn != nil)) ->
        {:error,
         Error.new(
           :client,
           "ARN operations require the special-endpoint implementation milestone"
         )}

      length(Enum.uniq(identities)) > 1 ->
        {:error,
         Error.new(:client, "Cross-endpoint transfers require the streaming transfer milestone")}

      contexts == [] ->
        with {:ok, data} <-
               Config.read(NativeGlobals.config_options(resolved.invocation.options)),
             do: Context.native(nil, resolved.invocation.options, data)

      true ->
        {:ok, hd(contexts)}
    end
  end

  defp validate_endpoint(endpoint), do: Context.validate_endpoint(endpoint)

  defp legacy_arguments(%Invocation{command: [name], args: args, options: options}) do
    {_arity, flags, _summary} = Map.fetch!(Registry.commands(:native), [name])

    flags =
      if name == "configure", do: Map.put(flags, "profile", {:profile, :string}), else: flags

    serialized =
      Enum.flat_map(flags, fn {flag, {key, type}} ->
        case Map.fetch(options, key) do
          :error -> []
          {:ok, true} when type == :boolean -> ["--" <> flag]
          {:ok, false} when type == :boolean -> ["--no-" <> flag]
          {:ok, value} -> ["--" <> flag, to_string(value)]
        end
      end)

    {:ok, serialized ++ ["--"] ++ args}
  end

  defp help(%Invocation{frontend: :native, command: [name]} = invocation)
       when is_map_key(@legacy, name) do
    @legacy[name].help(name)
    {_arity, flags, _summary} = Registry.spec(invocation.frontend, invocation.command)
    IO.puts("\nCOMMAND OPTIONS")

    flags
    |> Enum.sort()
    |> Enum.each(fn {flag, {_key, type}} ->
      IO.puts("    --#{flag}#{OptionHelp.suffix(type)}")
    end)

    print_global_options(invocation.frontend)
    :ok
  end

  defp help(invocation) do
    suffix = if invocation.frontend == :native, do: "", else: " #{invocation.frontend}"

    suffix =
      suffix <>
        if(invocation.command == [], do: "", else: " " <> Enum.join(invocation.command, " "))

    IO.puts(
      "ess#{suffix} — ExStorageService CLI v#{ExStorageServiceCli.version()}\n\nUSAGE\n    ess#{suffix} [options]"
    )

    commands = Registry.commands(invocation.frontend)

    if invocation.command == [] or Registry.group?(invocation.frontend, invocation.command) do
      IO.puts("\nCOMMANDS")

      commands
      |> Enum.filter(fn {path, _spec} ->
        Enum.take(path, length(invocation.command)) == invocation.command
      end)
      |> Enum.sort()
      |> Enum.each(fn {path, {_arity, _flags, summary}} ->
        IO.puts("    #{Enum.join(path, " ")}  #{summary}")
      end)
    else
      {_arity, flags, summary} = Registry.spec(invocation.frontend, invocation.command)
      IO.puts("\n#{summary}\n\nCOMMAND OPTIONS")

      flags
      |> Enum.sort()
      |> Enum.each(fn {name, {_key, type}} ->
        IO.puts("    --#{name}#{OptionHelp.suffix(type)}")
      end)
    end

    print_global_options(invocation.frontend)
    :ok
  end

  defp print_global_options(frontend) do
    IO.puts("\nGLOBAL OPTIONS")

    Registry.globals(frontend)
    |> Enum.sort()
    |> Enum.each(fn {name, {_key, type}} ->
      IO.puts("    --#{name}#{OptionHelp.suffix(type)}")
    end)
  end

  defp alias_command("set", [name, endpoint], options) do
    with :ok <- validate_endpoint(endpoint) do
      fields = %{
        "endpoint" => endpoint,
        "profile" => options[:profile] || "default",
        "provider" => options[:provider] || "ess-profile"
      }

      fields =
        Enum.reduce(
          [
            access_key: "access_key_id",
            secret_key: "secret_access_key",
            session_token: "session_token",
            region: "region",
            service: "service",
            service_version: "service_version",
            bucket_type: "bucket_type"
          ],
          fields,
          fn {key, field}, acc ->
            if Map.has_key?(options, key), do: Map.put(acc, field, options[key]), else: acc
          end
        )

      fields =
        if options[:access_key] || options[:secret_key],
          do: fields |> Map.put("provider", "static") |> Map.delete("profile"),
          else: fields

      with :ok <- Aliases.set(name, fields, NativeGlobals.config_options(options)),
           do: alias_result(%{status: "ok", alias: name}, options)
    end
  end

  defp alias_command("list", [], options),
    do:
      with(
        {:ok, aliases} <- Aliases.list(NativeGlobals.config_options(options)),
        do: alias_result(aliases, options)
      )

  defp alias_command("list", [name], options) do
    with {:ok, aliases} <- Aliases.list(NativeGlobals.config_options(options)) do
      case Map.fetch(aliases, name) do
        {:ok, record} -> alias_result(%{name => record}, options)
        :error -> {:error, Error.new(:configuration, "Requested alias does not exist")}
      end
    end
  end

  defp alias_command("remove", [name], options),
    do:
      with(
        :ok <- Aliases.remove(name, NativeGlobals.config_options(options)),
        do: alias_result(%{status: "ok", alias: name}, options)
      )

  defp alias_command("export", names, options) do
    with {:ok, document} <-
           Aliases.export(
             NativeGlobals.config_options(options,
               include_secrets: options[:include_secrets] || false
             )
           ) do
      decoded = JSON.decode!(document)

      case names do
        [] ->
          IO.puts(document)
          :ok

        [name] ->
          if Map.has_key?(decoded["aliases"], name) do
            IO.puts(
              JSON.encode!(Map.put(decoded, "aliases", Map.take(decoded["aliases"], [name])))
            )

            :ok
          else
            {:error, Error.new(:configuration, "Requested alias does not exist")}
          end
      end
    end
  end

  defp alias_command("import", [path], options) do
    case read_import(path) do
      {:ok, document} ->
        import_options =
          NativeGlobals.config_options(options,
            overwrite: options[:replace] || false,
            allow_secrets: options[:allow_secrets] || false
          )

        if options[:dry_run] do
          with {:ok, preview} <- Aliases.preview_import(document, import_options),
               do: alias_result(%{changes: preview}, options)
        else
          with :ok <- Aliases.import(document, import_options),
               do: alias_result(%{status: "ok"}, options)
        end

      {:error, _reason} ->
        {:error, Error.new(:configuration, "Cannot read alias import document")}
    end
  end

  defp read_import(path) do
    case File.open(path, [:read, :binary], fn file -> IO.binread(file, 1_048_577) end) do
      {:ok, document} when is_binary(document) and byte_size(document) <= 1_048_576 ->
        {:ok, document}

      _ ->
        {:error, :invalid_import}
    end
  end

  defp alias_result(data, options) do
    cond do
      options[:output] -> present_native(data, options)
      options[:json] -> IO.puts(JSON.encode!(data))
      true -> IO.puts(Jason.encode!(data, pretty: true))
    end
  end

  defp normalize({:error, %Error{}} = error), do: error
  defp normalize(:ok), do: :ok
end
