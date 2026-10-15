defmodule ExStorageServiceCli.CLI.NativeRestore do
  @moduledoc "Portable archive restore with complete bounded inventories and bounded status polling."
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.S3.{Capabilities, Input, Paginator}
  alias ExStorageServiceCli.Transfer.{Coordinator, Manifest}

  def commands do
    %{
      ["ilm", "restore"] =>
        {1..1,
         %{
           "days" => {:days, :integer},
           "recursive" => {:recursive, :boolean},
           "versions" => {:versions, :boolean},
           "version-id" => {:version_id, :string},
           "vid" => {:version_id, :string},
           "dry-run" => {:dry_run, :boolean}
         }, "Restore archived objects with Expedited retrieval and bounded completion polling"}
    }
  end

  def run(target, context, options, call, opts \\ [])

  def run(%Target{kind: :remote, bucket: bucket} = target, context, options, call, opts)
      when is_binary(bucket) and bucket != "" do
    with :ok <- options(target, options, opts),
         :ok <- Capabilities.check(context, "RestoreObject"),
         :ok <- bucket_type(context, target),
         :ok <- cancelled(context),
         {:ok, manifest} <- inventory(target, context, options, call, opts) do
      try do
        execute(manifest, target, context, options, call, opts)
      after
        Manifest.cleanup(manifest)
      end
    end
  end

  def run(_, _, _, _, _),
    do: invalid("Restore requires a remote S3 object or recursive bucket/prefix target")

  defp options(target, options, opts) do
    days = Map.get(options, :days, 1)
    version = options[:version_id]
    attempts = Keyword.get(opts, :restore_max_attempts, 40)
    delay = Keyword.get(opts, :restore_delay, 5000)

    cond do
      not is_integer(days) or days < 1 or days > 2_147_483_647 ->
        invalid("--days must be a positive 32-bit integer")

      options[:versions] == true and options[:recursive] != true ->
        invalid("--versions requires --recursive; select one version with --version-id")

      version != nil and (options[:recursive] == true or options[:versions] == true) ->
        invalid("--version-id cannot be combined with --recursive or --versions")

      version != nil and
          (not is_binary(version) or version == "" or byte_size(version) > 8192 or
             not String.valid?(version)) ->
        invalid("--version-id must be a nonempty UTF-8 version identifier within 8192 bytes")

      options[:recursive] != true and target.key in [nil, ""] ->
        invalid("A nonrecursive restore requires an object key")

      not is_integer(attempts) or attempts not in 1..40 or not is_integer(delay) or
          delay not in 0..5000 ->
        invalid("Restore polling requires 1..40 attempts and a delay within 0..5000 milliseconds")

      true ->
        :ok
    end
  end

  defp bucket_type(context, target) do
    if context.capabilities[:bucket_type] == "directory" or
         String.ends_with?(target.bucket, "--x-s3"),
       do:
         {:error,
          Error.new(:client, "Archive restore is unsupported for directory buckets",
            code: :provider_unsupported,
            operation: "RestoreObject"
          )},
       else: :ok
  end

  defp inventory(target, context, options, call, opts) do
    entries =
      if options[:recursive] == true do
        operation = if options[:versions] == true, do: "ListObjectVersions", else: "ListObjectsV2"
        field = if options[:versions] == true, do: "Versions", else: "Contents"

        Paginator.pages(
          operation,
          %{"Bucket" => target.bucket, "Prefix" => target.key || ""},
          fn operation, input -> with :ok <- cancelled(context), do: call.(operation, input) end
        )
        |> Stream.flat_map(fn
          {:error, _} = error ->
            [error]

          {:ok, page} ->
            Enum.map(page[field] || [], fn item ->
              entry(
                item["Key"],
                if(options[:versions] == true, do: item["VersionId"], else: nil),
                target,
                options
              )
            end)
        end)
      else
        [entry(target.key, options[:version_id], target, options)]
      end

    Manifest.build(entries, opts)
  end

  defp entry(key, version, target, options) do
    valid_key =
      is_binary(key) and key != "" and String.valid?(key) and byte_size(key) <= 1024 and
        String.starts_with?(key, target.key || "")

    valid_version =
      if options[:versions] == true or options[:version_id] != nil,
        do:
          is_binary(version) and version != "" and String.valid?(version) and
            byte_size(version) <= 8192,
        else: version == nil

    if valid_key and valid_version do
      record = %{"name" => JSON.encode!([key, version]), "key" => key, "version_id" => version}

      with {:ok, _} <- Input.validate("RestoreObject", request(record, target, options)),
           do: {:ok, record}
    else
      invalid("Restore inventory contains an invalid key or immutable version identifier")
    end
  end

  defp execute(manifest, target, context, %{dry_run: true} = options, _call, _opts) do
    with :ok <- cancelled(context),
         do:
           {:ok,
            %{
              "DryRun" => true,
              "Operation" => "RestoreObject",
              "Objects" => manifest.count,
              "Versions" => options[:versions] == true,
              "RestoreRequest" => restore_request(options),
              "Bucket" => target.bucket
            }}
  end

  defp execute(manifest, target, context, options, call, opts) do
    with {:ok, requested} <- submit(manifest, target, context, options, call),
         {:ok, restored} <- wait(manifest, target, context, options, call, opts, requested) do
      {:ok,
       %{
         "Requested" => requested,
         "Restored" => restored,
         "Versions" => options[:versions] == true
       }}
    end
  end

  defp submit(manifest, target, context, options, call) do
    Enum.reduce_while(Manifest.stream(manifest), {:ok, 0}, fn entry, {:ok, count} ->
      with :ok <- cancelled(context),
           {:ok, _} <- call.("RestoreObject", request(entry, target, options)) do
        {:cont, {:ok, count + 1}}
      else
        {:error, error} -> {:halt, partial(error, count, 0)}
      end
    end)
  end

  defp wait(manifest, target, context, _options, call, opts, requested) do
    Enum.reduce_while(Manifest.stream(manifest), {:ok, 0}, fn entry, {:ok, count} ->
      input = identity(entry, target)

      case poll(input, context, call, opts, 1) do
        :ok -> {:cont, {:ok, count + 1}}
        {:error, error} -> {:halt, partial(error, requested, count)}
      end
    end)
  end

  defp poll(input, context, call, opts, attempt) do
    with :ok <- cancelled(context),
         {:ok, response} <- call.("HeadObject", input),
         {:ok, state} <- status(response) do
      cond do
        state == :complete ->
          :ok

        attempt >= Keyword.get(opts, :restore_max_attempts, 40) ->
          {:error,
           Error.new(:waiter, "Archive restore did not complete within its polling budget",
             code: :restore_exhausted,
             operation: "HeadObject",
             attempt: attempt
           )}

        true ->
          with :ok <- (opts[:sleep] || (&sleep/1)).(Keyword.get(opts, :restore_delay, 5000)),
               do: poll(input, context, call, opts, attempt + 1)
      end
    end
  end

  defp status(%{"Restore" => value}) when is_binary(value) do
    case Regex.run(~r/^\s*ongoing-request="(true|false)"(?:\s*,|\s*$)/, value) do
      [_, "true"] -> {:ok, :pending}
      [_, "false"] -> {:ok, :complete}
      _ -> missing_status()
    end
  end

  defp status(_), do: missing_status()

  defp missing_status,
    do:
      {:error,
       Error.new(:client, "The provider returned no recognized archive restore status",
         code: :restore_status,
         operation: "HeadObject"
       )}

  defp request(entry, target, options),
    do: Map.put(identity(entry, target), "RestoreRequest", restore_request(options))

  defp restore_request(options),
    do: %{
      "Days" => Map.get(options, :days, 1),
      "GlacierJobParameters" => %{"Tier" => "Expedited"}
    }

  defp identity(entry, target) do
    input = %{"Bucket" => target.bucket, "Key" => entry["key"]}

    if entry["version_id"] != nil,
      do: Map.put(input, "VersionId", entry["version_id"]),
      else: input
  end

  defp cancelled(context) do
    with :ok <- Coordinator.check(context.coordinator) do
      receive do
        {:ess_cancel, signal} -> interrupted(signal)
      after
        0 -> :ok
      end
    end
  end

  defp sleep(delay) do
    receive do
      {:ess_cancel, signal} -> interrupted(signal)
    after
      delay -> :ok
    end
  end

  defp interrupted({_reference, signal}), do: interrupted(signal)
  defp interrupted(:sigterm), do: {:error, Error.new(:terminated, "Archive restore terminated")}
  defp interrupted(_), do: {:error, Error.new(:interrupted, "Archive restore interrupted")}

  defp partial(error, requested, restored),
    do:
      {:error,
       %{
         error
         | details:
             Map.merge(
               error.details,
               %{requested: requested, restored: restored}
             )
       }}

  defp invalid(message), do: {:error, Error.new(:syntax, message)}
end
