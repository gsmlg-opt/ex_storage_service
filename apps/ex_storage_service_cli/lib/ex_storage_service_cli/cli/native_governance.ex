defmodule ExStorageServiceCli.CLI.NativeGovernance do
  @moduledoc "Portable object governance and tagging over complete, bounded, identity-checked inventories."
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.S3.{Capabilities, Input}
  alias ExStorageServiceCli.Transfer.{Coordinator, Inventory, Manifest, VersionSelection}

  def commands do
    for family <- ~w(legalhold retention), action <- ~w(set clear info), into: %{} do
      flags = %{
        "version-id" => {:version_id, :string},
        "vid" => {:version_id, :string},
        "recursive" => {:recursive, :boolean},
        "versions" => {:versions, :boolean},
        "rewind" => {:rewind, :string},
        "limit" => {:limit, :integer}
      }

      flags =
        if action == "info", do: flags, else: Map.put(flags, "dry-run", {:dry_run, :boolean})

      flags =
        if family == "retention", do: Map.put(flags, "default", {:default, :boolean}), else: flags

      flags =
        if family == "retention" and action != "info",
          do:
            Map.merge(flags, %{
              "bypass" => {:bypass_governance_retention, :boolean},
              "bypass-governance-retention" => {:bypass_governance_retention, :boolean}
            }),
          else: flags

      arity =
        case {family, action} do
          {"legalhold", "set"} -> 1..2
          {"retention", "set"} -> 2..3
          _ -> 1..1
        end

      {[family, action],
       {arity, flags, "#{String.capitalize(action)} portable #{family} settings"}}
    end
  end

  def handles?(%{command: [family, action], args: args}) when family in ~w(legalhold retention) do
    action in ~w(clear info) or
      (action == "set" and
         ((family == "legalhold" and length(args) == 1) or
            (family == "retention" and length(args) == 3)))
  end

  def handles?(_), do: false

  def document_options(%{command: [family, "set"], args: [_, _], options: options})
      when family in ~w(legalhold retention) do
    if Enum.any?([:recursive, :versions, :rewind, :default, :limit], &Map.has_key?(options, &1)),
      do:
        invalid(
          "Full-document governance set does not accept recursive, versions, rewind, default or limit options"
        ),
      else: :ok
  end

  def document_options(_), do: :ok

  def run(
        family,
        action,
        args,
        %Target{kind: :remote, bucket: bucket} = target,
        context,
        options,
        call,
        opts
      )
      when is_binary(bucket) and bucket != "" do
    operation = operation(family, action)

    with :ok <- options(target, options),
         {:ok, document} <- document(family, action, args, opts),
         :ok <- Capabilities.check(context, operation),
         :ok <- bucket_type(context, target),
         :ok <- cancelled(context) do
      if options[:default] == true do
        bucket_default(action, target, document, context, options, call)
      else
        objects(operation, action, document, target, context, options, call, opts)
      end
    end
  end

  def run(_, _, _, _, _, _, _, _),
    do: invalid("Governance requires a remote bucket or object target")

  defp options(target, options) do
    version = options[:version_id]

    selection =
      options[:recursive] == true or options[:versions] == true or options[:rewind] != nil

    cond do
      (options[:limit] || 1000) not in 0..10_000 ->
        invalid("Object selection output limit must be within 0..10000")

      version != nil and
          (not is_binary(version) or version == "" or byte_size(version) > 8192 or
             not String.valid?(version)) ->
        invalid("Version ID must be nonempty UTF-8 within 8192 bytes")

      version != nil and selection ->
        invalid("An explicit version conflicts with recursive, versions and rewind selection")

      options[:default] == true and
          (selection or version != nil or options[:bypass_governance_retention] == true) ->
        invalid("Bucket defaults conflict with object selection and governance bypass")

      options[:default] == true and target.key not in [nil, ""] ->
        invalid("Bucket defaults require a bucket target")

      options[:default] != true and options[:recursive] != true and target.key in [nil, ""] ->
        invalid("An object key or --recursive is required")

      true ->
        :ok
    end
  end

  defp bucket_type(context, target) do
    if context.capabilities[:bucket_type] == "directory" or
         String.ends_with?(target.bucket, "--x-s3"),
       do:
         {:error,
          Error.new(:client, "Object governance is unsupported for directory buckets",
            code: :provider_unsupported
          )},
       else: :ok
  end

  defp operation("legalhold", "info"), do: "GetObjectLegalHold"
  defp operation("legalhold", _), do: "PutObjectLegalHold"
  defp operation("retention", "info"), do: "GetObjectRetention"
  defp operation("retention", _), do: "PutObjectRetention"
  defp operation("tag", "info"), do: "GetObjectTagging"
  defp operation("tag", "set"), do: "PutObjectTagging"
  defp operation("tag", "clear"), do: "DeleteObjectTagging"

  defp document("tag", "set", [_target, value], _) do
    with {:ok, tagging} <- ExStorageServiceCli.CLI.Management.object_tags(value),
         do: {:ok, %{"Tagging" => tagging}}
  end

  defp document("tag", _, _, _), do: {:ok, %{}}

  defp document("legalhold", action, _, _),
    do: {:ok, %{"LegalHold" => %{"Status" => if(action == "set", do: "ON", else: "OFF")}}}

  defp document("retention", "set", [mode, duration, _], opts) do
    with true <- is_binary(mode) and String.upcase(mode) in ["GOVERNANCE", "COMPLIANCE"],
         [_, number, unit] <- Regex.run(~r/\A([0-9]{1,9})([dDyY])\z/, duration),
         {count, ""} when count > 0 <- Integer.parse(number),
         {:ok, until} <-
           retain_until(opts[:now] || DateTime.utc_now(), count, String.downcase(unit)) do
      {:ok,
       %{
         "Retention" => %{"Mode" => String.upcase(mode), "RetainUntilDate" => until},
         "DefaultRetention" => %{
           "Mode" => String.upcase(mode),
           if(String.downcase(unit) == "d", do: "Days", else: "Years") => count
         }
       }}
    else
      _ ->
        invalid(
          "Retention requires governance or compliance and a positive Nd/Ny duration within the supported calendar"
        )
    end
  end

  defp document("retention", _, _, _), do: {:ok, %{"Retention" => %{}}}

  defp retain_until(now, count, "d") do
    finish_date(Date.add(DateTime.to_date(now), count), now)
  rescue
    _ -> :error
  end

  defp retain_until(now, count, "y") do
    date = DateTime.to_date(now)

    case Date.new(date.year + count, date.month, date.day) do
      {:ok, date} ->
        finish_date(date, now)

      {:error, :invalid_date} when date.month == 2 and date.day == 29 ->
        with {:ok, date} <- Date.new(date.year + count, 3, 1), do: finish_date(date, now)

      _ ->
        :error
    end
  end

  defp finish_date(%Date{year: year} = date, now) when year in 0..9999 do
    with {:ok, value} <-
           DateTime.new(date, Time.truncate(DateTime.to_time(now), :second), "Etc/UTC"),
         do: {:ok, value}
  end

  defp finish_date(_, _), do: :error

  defp bucket_default(action, target, document, context, options, call) do
    get = %{"Bucket" => target.bucket}

    with :ok <- Capabilities.check(context, "GetObjectLockConfiguration"),
         :ok <- cancelled(context),
         {:ok, response} <- call.("GetObjectLockConfiguration", get),
         config when is_map(config) <- response["ObjectLockConfiguration"] do
      if action == "info" do
        {:ok, response}
      else
        with {:ok, _} <-
               Input.validate(
                 "PutObjectLockConfiguration",
                 Map.put(get, "ObjectLockConfiguration", config)
               ),
             true <- config["ObjectLockEnabled"] == "Enabled",
             config =
               if(action == "clear",
                 do: Map.delete(config, "Rule"),
                 else:
                   put_in(
                     config,
                     [Access.key("Rule", %{}), "DefaultRetention"],
                     document["DefaultRetention"]
                   )
               ),
             input = Map.put(get, "ObjectLockConfiguration", config),
             {:ok, input} <- Input.validate("PutObjectLockConfiguration", input),
             :ok <- cancelled(context) do
          if options[:dry_run] == true,
            do:
              {:ok,
               %{"DryRun" => true, "Operation" => "PutObjectLockConfiguration", "Input" => input}},
            else: call.("PutObjectLockConfiguration", input)
        else
          false ->
            invalid("Enable bucket Object Lock deliberately before editing default retention")

          error ->
            error
        end
      end
    else
      {:error, _} = error -> error
      _ -> invalid("Provider returned no complete object-lock configuration")
    end
  end

  defp objects(operation, action, document, target, context, options, call, opts) do
    with {:ok, inventory} <- inventory(target, context, options, call, opts) do
      try do
        if inventory.count == 0 do
          invalid("No eligible object versions matched the selection")
        else
          selected =
            if action == "info",
              do: Manifest.stream(inventory) |> Stream.take(options[:limit] || 1000),
              else: Manifest.stream(inventory)

          entries =
            selected
            |> Stream.map(
              &preflight(&1, target, context, options, action, operation, document, call)
            )

          with {:ok, ready} <- Manifest.build(entries, opts) do
            try do
              apply_objects(
                ready,
                operation,
                action,
                document,
                target,
                context,
                Map.put(options, :matched_objects, inventory.count),
                call
              )
            after
              Manifest.cleanup(ready)
            end
          end
        end
      after
        Manifest.cleanup(inventory)
      end
    end
  end

  defp inventory(target, context, options, call, opts) do
    opts =
      Keyword.put(opts, :fetch, fn operation, input ->
        with :ok <- cancelled(context), do: call.(operation, input)
      end)

    opts = Keyword.put(opts, :exact, options[:recursive] != true)

    cond do
      options[:versions] == true ->
        with {:ok, cutoff} <- VersionSelection.cutoff(options[:rewind] || "0", opts),
             {:ok, versions} <- Inventory.versions(context, target, opts) do
          try do
            entries =
              Manifest.stream(versions)
              |> Stream.flat_map(fn entry ->
                cond do
                  not is_integer(entry["modified"]) or entry["modified"] <= 0 ->
                    [invalid("Version inventory requires complete modification timestamps")]

                  entry["delete_marker"] or entry["modified"] * 1000 >= cutoff ->
                    []

                  true ->
                    [Map.put(entry, "listed", true)]
                end
              end)

            Manifest.build(entries, opts)
          after
            Manifest.cleanup(versions)
          end
        end

      options[:rewind] != nil ->
        with {:ok, selected} <-
               VersionSelection.inventory(context, target, options[:rewind], opts) do
          try do
            Manifest.build(
              Manifest.stream(selected) |> Stream.map(&Map.put(&1, "listed", true)),
              opts
            )
          after
            Manifest.cleanup(selected)
          end
        end

      options[:recursive] == true ->
        with {:ok, current} <- Inventory.remote(context, target, opts) do
          try do
            Manifest.build(
              Manifest.stream(current) |> Stream.map(&Map.put(&1, "listed", true)),
              opts
            )
          after
            Manifest.cleanup(current)
          end
        end

      true ->
        Manifest.build(
          [%{"name" => target.key, "key" => target.key, "version_id" => options[:version_id]}],
          opts
        )
    end
  end

  defp preflight(entry, target, context, options, action, operation, document, call) do
    with :ok <- cancelled(context),
         {:ok, head} <- call.("HeadObject", identity(entry, target)),
         :ok <- confirm(entry, head),
         :ok <- immutable(entry, head, options, action),
         entry = Map.put(entry, "version_id", head["VersionId"] || entry["version_id"]),
         {:ok, _} <-
           Input.validate(operation, request(entry, target, operation, document, options)) do
      {:ok, entry}
    end
  end

  defp confirm(entry, head) do
    if is_binary(head["ETag"]) and head["ETag"] != "" and is_integer(head["ContentLength"]) and
         head["ContentLength"] >= 0 and
         (entry["version_id"] == nil or head["VersionId"] == entry["version_id"]) and
         (entry["listed"] != true or
            (head["ETag"] == entry["etag"] and head["ContentLength"] == entry["size"] and
               modified(head["LastModified"]) == entry["modified"])),
       do: :ok,
       else:
         {:error,
          Error.new(:client, "Provider did not confirm the selected object identity",
            code: :inventory_changed
          )}
  end

  defp immutable(entry, head, options, action) do
    selected = entry["listed"] == true or options[:rewind] != nil or options[:version_id] != nil

    if action != "info" and selected and
         (head["VersionId"] || entry["version_id"]) in [nil, "", "null"],
       do:
         invalid(
           "Selected object mutations require an immutable version; the API has no conditional identity fence for null versions"
         ),
       else: :ok
  end

  defp apply_objects(manifest, operation, action, document, target, context, options, call) do
    limit = options[:limit] || 1000
    matched = options[:matched_objects] || manifest.count

    entries =
      if action == "info",
        do: Manifest.stream(manifest) |> Stream.take(limit),
        else: Manifest.stream(manifest)

    result =
      Enum.reduce_while(entries, {:ok, 0, []}, fn entry, {:ok, count, rows} ->
        input = request(entry, target, operation, document, options)

        outcome =
          with :ok <- cancelled(context) do
            if options[:dry_run] == true,
              do: {:ok, %{"DryRun" => true, "Input" => input}},
              else: call.(operation, input)
          end

        case outcome do
          {:ok, data} ->
            row = %{"Key" => entry["key"], "VersionId" => entry["version_id"], "Result" => data}
            {:cont, {:ok, count + 1, if(count < limit, do: [row | rows], else: rows)}}

          {:error, error} ->
            {:halt,
             {:error,
              %{
                error
                | details:
                    Map.merge(error.details || %{}, %{processed: count, matched_objects: matched})
              }}}
        end
      end)

    with {:ok, count, rows} <- result,
         do:
           {:ok,
            %{
              "Entries" => Enum.reverse(rows),
              "Processed" => count,
              "MatchedObjects" => matched,
              "Returned" => min(count, limit),
              "Partial" => matched > limit,
              "DryRun" => options[:dry_run] == true
            }}
  end

  defp identity(entry, target) do
    input = %{"Bucket" => target.bucket, "Key" => entry["key"]}
    if entry["version_id"], do: Map.put(input, "VersionId", entry["version_id"]), else: input
  end

  defp request(entry, target, operation, document, options) do
    input = identity(entry, target)

    input =
      cond do
        operation == "PutObjectLegalHold" -> Map.put(input, "LegalHold", document["LegalHold"])
        operation == "PutObjectRetention" -> Map.put(input, "Retention", document["Retention"])
        operation == "PutObjectTagging" -> Map.put(input, "Tagging", document["Tagging"])
        true -> input
      end

    if operation == "PutObjectRetention" and options[:bypass_governance_retention] == true,
      do: Map.put(input, "BypassGovernanceRetention", true),
      else: input
  end

  defp modified(%DateTime{} = value), do: DateTime.to_unix(value, :microsecond)

  defp modified(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, date, _} -> modified(date)
      _ -> nil
    end
  end

  defp modified(_), do: nil

  defp cancelled(context) do
    receive do
      {:ess_cancel, _} ->
        {:error, Error.new(:interrupted, "Governance interrupted", code: :cancelled)}
    after
      0 -> Coordinator.check(context.coordinator)
    end
  end

  defp invalid(message), do: {:error, Error.new(:syntax, message, code: :governance_options)}
end
