defmodule ExStorageServiceCli.CLI.TransferCommands do
  @moduledoc "Explicit bounded journal inspection and selected multipart recovery."
  alias ExStorageServiceCli.{Config, Context, Error, Target}
  alias ExStorageServiceCli.Transfer.{Journal, Multipart, State}

  def run(action, args, options, runtime, opts \\ [])

  def run("list", [], options, _runtime, opts) do
    root = opts[:state_root] || State.transfers()

    case File.lstat(root) do
      {:error, :enoent} -> {:ok, %{entries: [], next: nil}}
      _ -> Journal.list(root, limit: options[:limit] || 50, cursor: options[:cursor] || 0)
    end
  end

  def run(action, [id], options, runtime, opts) when action in ["resume", "abort"] do
    root = opts[:state_root] || State.transfers()

    with {:ok, record} <- Journal.get(root, id),
         {:ok, context} <- context(record, options, runtime, opts),
         {:ok, input} <- input(options[:request_input]),
         {:ok, source_context} <- source_context(options[:source_alias], options, runtime, opts) do
      request_opts = [
        state_root: root,
        task_supervisor: runtime.tasks,
        input: input,
        keep_incomplete: record["options"]["keep_incomplete"] || false,
        copy: record["options"]["copy"] || false,
        part_size: record["options"]["part_size"]
      ]

      request_opts =
        if source_context,
          do: Keyword.put(request_opts, :source_context, source_context),
          else: request_opts

      if action == "resume",
        do: Multipart.resume(context, id, request_opts),
        else: Multipart.abort(context, id, request_opts)
    end
  end

  defp context(record, options, runtime, opts) do
    endpoint = record["endpoint"]
    provider = record["credential_provider"]

    options =
      options
      |> Map.put_new(:endpoint, endpoint["url"])
      |> Map.put_new(:endpoint_url, endpoint["url"])
      |> Map.put_new(:region, endpoint["region"])
      |> Map.put_new(
        :profile,
        if(provider["kind"] == "profile", do: provider["name"], else: "default")
      )
      |> Map.put_new(:no_sign_request, provider["kind"] == "anonymous")

    runtime_opts = Keyword.put(opts, :runtime, runtime)

    if options[:frontend] == "aws" do
      Context.aws(options, runtime_opts)
    else
      with {:ok, config} <-
             Config.read(ExStorageServiceCli.CLI.NativeGlobals.config_options(options, opts)),
           do: Context.native(nil, options, config, runtime_opts)
    end
  end

  defp source_context(nil, _options, _runtime, _opts), do: {:ok, nil}

  defp source_context(name, options, runtime, opts) do
    with {:ok, config} <-
           Config.read(ExStorageServiceCli.CLI.NativeGlobals.config_options(options, opts)),
         {:ok, target} <-
           Target.parse("ess://" <> name, aliases: Map.keys(config["aliases"] || %{})),
         do:
           Context.native(
             target,
             Map.drop(options, [:endpoint, :endpoint_url, :profile]),
             config,
             Keyword.put(opts, :runtime, runtime)
           )
  end

  defp input(nil), do: {:ok, %{}}

  defp input("file://" <> path) do
    case File.open(path, [:read, :binary], &IO.binread(&1, 1_048_577)) do
      {:ok, bytes} when is_binary(bytes) and byte_size(bytes) <= 1_048_576 -> input(bytes)
      _ -> invalid()
    end
  end

  defp input(value) when byte_size(value) <= 1_048_576 do
    case JSON.decode(value) do
      {:ok, input} when is_map(input) -> {:ok, input}
      _ -> invalid()
    end
  end

  defp input(_), do: invalid()

  defp invalid,
    do: {:error, Error.new(:syntax, "Resume request-input must be a bounded JSON object")}
end
