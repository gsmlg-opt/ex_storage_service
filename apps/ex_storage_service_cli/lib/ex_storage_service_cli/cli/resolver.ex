defmodule ExStorageServiceCli.CLI.Resolver do
  @moduledoc "Resolves native targets before execution, preserving independent endpoint contexts."
  alias ExStorageServiceCli.{Aliases, Config, Context, Error, Target}
  alias ExStorageServiceCli.CLI.Invocation
  defstruct [:invocation, targets: [], contexts: [], legacy_args: []]

  def resolve(invocation, opts \\ [])

  def resolve(%Invocation{frontend: :s3, command: [name]} = invocation, opts) do
    with {:ok, context} <- Context.aws(invocation.options, opts),
         {:ok, targets} <- aws_targets(invocation.args, name) do
      {:ok,
       %__MODULE__{
         invocation: invocation,
         targets: targets,
         contexts:
           if(targets == [], do: [context], else: Enum.map(targets, &remote_context(&1, context))),
         legacy_args: invocation.args
       }}
    end
  end

  def resolve(%Invocation{frontend: :native, command: [name | _]} = invocation, opts) do
    opts = ExStorageServiceCli.CLI.NativeGlobals.config_options(invocation.options, opts)

    with {:ok, config} <- Config.read(opts),
         {:ok, aliases} <- Aliases.names(config) do
      Enum.reduce_while(invocation.args, {:ok, %__MODULE__{invocation: invocation}}, fn argument,
                                                                                        {:ok,
                                                                                         resolved} ->
        with {:ok, target} <-
               Target.parse(argument,
                 aliases: aliases,
                 legacy: name in ["ls", "tree", "mb", "rb", "rm", "presign"],
                 stdio: name in ["cp", "get", "put"]
               ),
             :ok <- validate_target(name, target),
             {:ok, context} <- target_context(target, invocation.options, config, opts) do
          legacy = legacy_argument(target)

          {:cont,
           {:ok,
            %{
              resolved
              | targets: [target | resolved.targets],
                contexts: [context | resolved.contexts],
                legacy_args:
                  if(legacy == nil,
                    do: resolved.legacy_args,
                    else: [legacy | resolved.legacy_args]
                  )
            }}}
        else
          error -> {:halt, error}
        end
      end)
      |> finish(invocation, config, opts)
    end
  end

  defp finish({:error, _} = error, _invocation, _config, _opts), do: error

  defp finish({:ok, %{contexts: []} = resolved}, invocation, config, opts) do
    with {:ok, context} <- Context.native(nil, invocation.options, config, opts),
         do: {:ok, %{resolved | contexts: [context]}}
  end

  defp finish({:ok, resolved}, _invocation, _config, _opts) do
    {:ok,
     %{
       resolved
       | targets: Enum.reverse(resolved.targets),
         contexts: Enum.reverse(resolved.contexts),
         legacy_args: Enum.reverse(resolved.legacy_args)
     }}
  end

  defp validate_target(name, %Target{kind: kind})
       when name in ["cp", "mv", "get", "put", "sync", "mirror", "diff", "watch"] and
              kind in [:local, :remote, :stdio],
       do: :ok

  defp validate_target("ls", %Target{kind: kind}) when kind in [:remote, :alias], do: :ok

  defp validate_target(name, %Target{kind: :alias}) when name in ["doctor", "ping", "ready"],
    do: :ok

  defp validate_target(_name, %Target{kind: :remote}), do: :ok

  defp validate_target(_name, _target),
    do: {:error, Error.new(:syntax, "This command requires a bucket or object target")}

  defp target_context(%Target{kind: kind}, _options, _config, _opts)
       when kind in [:local, :stdio],
       do: {:ok, nil}

  defp target_context(target, options, config, opts),
    do: Context.native(target, options, config, opts)

  defp legacy_argument(%Target{kind: :local, path: path}), do: path
  defp legacy_argument(%Target{kind: :stdio}), do: "-"
  defp legacy_argument(%Target{kind: :alias}), do: nil
  defp legacy_argument(%Target{bucket: bucket, key: ""}), do: "s3://" <> bucket
  defp legacy_argument(%Target{bucket: bucket, key: key}), do: "s3://" <> bucket <> "/" <> key

  defp aws_targets(arguments, name) do
    Enum.reduce_while(arguments, {:ok, []}, fn argument, {:ok, targets} ->
      with {:ok, target} <- Target.parse(argument, stdio: name in ["cp", "mv"]),
           :ok <- aws_target(name, target) do
        {:cont, {:ok, [target | targets]}}
      else
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, targets} -> {:ok, Enum.reverse(targets)}
      error -> error
    end
  end

  defp aws_target(name, %Target{kind: kind})
       when name in ["cp", "mv", "sync"] and kind in [:remote, :local, :stdio],
       do: :ok

  defp aws_target(_name, %Target{kind: :remote}), do: :ok
  defp aws_target(_, _), do: {:error, Error.new(:syntax, "This command requires an S3 target")}
  defp remote_context(%Target{kind: :remote}, context), do: context
  defp remote_context(_, _), do: nil
end
