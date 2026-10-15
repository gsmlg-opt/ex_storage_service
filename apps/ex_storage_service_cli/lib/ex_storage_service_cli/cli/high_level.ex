defmodule ExStorageServiceCli.CLI.HighLevel do
  @moduledoc "Shared bounded high-level S3 operations and fail-closed synchronization."
  alias ExStorageServiceCli.{Context, Error, Target}

  alias ExStorageServiceCli.CLI.{
    HighLevelInput,
    NativeTransferSelection,
    S3Options,
    S3PathResolver
  }

  alias ExStorageServiceCli.S3.{Client, Encoder, Endpoint, OperationCatalog, Paginator}

  alias ExStorageServiceCli.Transfer.{
    Copy,
    Engine,
    Filter,
    Inventory,
    Manifest,
    Mime,
    Progress,
    Sink,
    Source,
    Spool,
    SyncPolicy,
    VersionSelection
  }

  def run(invocation, targets, contexts, opts \\ []) do
    [command] = invocation.command

    options =
      if invocation.frontend == :s3,
        do: Map.merge(S3Options.defaults(command), invocation.options),
        else: invocation.options

    with :ok <- supported_flags(command, options),
         {:ok, filters} <- Filter.compile(invocation.filters),
         {:ok, selection} <- NativeTransferSelection.compile(invocation, opts),
         {:ok, input} <-
           HighLevelInput.decode(
             if(command == "presign", do: Map.drop(options, [:expires]), else: options),
             opts
           ),
         :ok <- S3PathResolver.validate(invocation, targets, contexts, options, opts) do
      settings = %{
        invocation: invocation,
        options: options,
        filters: filters,
        selection: selection,
        opts: Keyword.put(opts, :skipped_archives, :atomics.new(1, signed: false)),
        input: input
      }

      result =
        if opts[:write] == nil and
             (command in ["cat", "head"] or match?(%Target{kind: :stdio}, List.last(targets))) do
          ExStorageServiceCli.CLI.RawOutput.with_stdout(fn ->
            dispatch(command, targets, contexts, settings)
          end)
        else
          dispatch(command, targets, contexts, settings)
        end

      result
      |> frontend_error(invocation, targets)
    end
  rescue
    _ -> failure(:highlevel_failed, "High-level operation could not complete")
  end

  defp dispatch("ls", [target], [context], %{options: %{rewind: _}} = settings),
    do:
      ExStorageServiceCli.CLI.NativeInspection.ls(
        target,
        source_region(context, settings),
        settings.options,
        settings.opts
      )

  defp dispatch("ls", [target], [context], %{options: %{versions: true}} = settings),
    do:
      ExStorageServiceCli.CLI.NativeListing.versions(
        settings.invocation,
        target,
        source_region(context, settings),
        settings.opts
      )

  defp dispatch("ls", targets, contexts, settings), do: list(targets, contexts, settings)

  defp dispatch("mb", [target], [context], settings) do
    input = %{"Bucket" => target.bucket}

    input =
      if context.region == "us-east-1",
        do: input,
        else:
          Map.put(input, "CreateBucketConfiguration", %{"LocationConstraint" => context.region})

    with :ok <- bucket_target(target) do
      if settings.invocation.frontend == :native and
           Enum.any?(
             [:ignore_existing, :with_lock, :with_versioning],
             &Map.has_key?(settings.options, &1)
           ) do
        native_make_bucket(context, target, input, settings)
      else
        with {:ok, _} <- Client.call(context, "CreateBucket", input, settings.opts) do
          emit(settings, "make_bucket: " <> target.bucket)
          :ok
        end
      end
    end
  end

  defp dispatch("website", [target], [context], settings) do
    options = settings.options
    configuration = %{}

    configuration =
      if options[:index_document],
        do: Map.put(configuration, "IndexDocument", %{"Suffix" => options[:index_document]}),
        else: configuration

    configuration =
      if options[:error_document],
        do: Map.put(configuration, "ErrorDocument", %{"Key" => options[:error_document]}),
        else: configuration

    if configuration == %{} do
      failure(:website_configuration, "Website requires an index or error document")
    else
      with :ok <- bucket_target(target),
           {:ok, _} <-
             Client.call(
               context,
               "PutBucketWebsite",
               %{"Bucket" => target.bucket, "WebsiteConfiguration" => configuration},
               settings.opts
             ),
           do: :ok
    end
  end

  defp dispatch("presign", [target], [context], settings), do: presign(context, target, settings)

  defp dispatch("rb", [target], [context], settings) do
    with :ok <- bucket_target(target),
         :ok <- remove_bucket_objects(context, target, settings),
         {:ok, _} <-
           Client.call(context, "DeleteBucket", %{"Bucket" => target.bucket}, settings.opts) do
      emit(settings, "remove_bucket: " <> target.bucket)
      :ok
    end
  end

  defp dispatch("rm", [target], [context], settings) do
    cond do
      settings.options[:all_versions] -> remove_versions(context, target, settings)
      settings.options[:recursive] -> remove_recursive(context, target, settings)
      true -> remove_object(context, target, settings)
    end
  end

  defp dispatch(command, [source, destination], [source_context, destination_context], settings)
       when command in ["cp", "mv", "sync", "mirror", "diff", "get", "put"] do
    source_context = source_region(source_context, settings)

    with :ok <- selected_version_source(source, settings),
         :ok <-
           distinct(
             source,
             source_context,
             destination,
             destination_context,
             command in ["sync", "mirror", "diff"] or settings.options[:recursive] == true
           ) do
      if command in ["sync", "mirror", "diff"] or settings.options[:recursive] do
        synchronize(command, source, source_context, destination, destination_context, settings)
      else
        with {:ok, destination} <- destination(source, destination),
             :ok <-
               one(
                 command,
                 source,
                 source_context,
                 destination,
                 destination_context,
                 settings,
                 nil,
                 nil
               ),
             do: :ok
      end
    end
  end

  defp dispatch("stat", [target], [context], settings) do
    ExStorageServiceCli.CLI.NativeInspection.stat(
      target,
      source_region(context, settings),
      settings.options,
      input_opts(settings, "HeadObject")
    )
  end

  defp dispatch("pipe", [target], [context], settings) do
    if settings.options[:dryrun] == true or settings.options[:dry_run] == true do
      emit(settings, "(dryrun) upload: - to " <> label(target, settings))
      :ok
    else
      Spool.with_source(
        context,
        Keyword.put(settings.opts, :expected_size, settings.options[:expected_size]),
        fn source ->
          with {:ok, _} <-
                 Progress.with(settings.options, settings.opts, fn opts ->
                   Engine.upload_source(
                     context,
                     target.bucket,
                     target.key,
                     source,
                     input_opts(%{settings | opts: opts}, "PutObject")
                   )
                 end),
               do: :ok
        end
      )
    end
  end

  defp dispatch("cat", [target], [context], settings) do
    ExStorageServiceCli.CLI.NativeInspection.cat(
      target,
      source_region(context, settings),
      settings.options,
      input_opts(settings, "GetObject")
    )
  end

  defp dispatch("head", [target], [context], settings),
    do: raw_head(target, source_region(context, settings), settings)

  defp dispatch("find", [target], [context], settings),
    do: ExStorageServiceCli.CLI.Find.run(target, source_region(context, settings), settings)

  defp dispatch("tree", [target], [context], settings),
    do:
      ExStorageServiceCli.CLI.NativeInspection.tree(
        target,
        source_region(context, settings),
        settings.options,
        settings.opts
      )

  defp dispatch("du", [target], [context], %{options: options} = settings)
       when is_map_key(options, :recursive) or is_map_key(options, :depth) or
              is_map_key(options, :versions) or is_map_key(options, :rewind),
       do:
         ExStorageServiceCli.CLI.NativeInspection.du(
           target,
           source_region(context, settings),
           options,
           settings.opts
         )

  defp dispatch(command, [target], [context], settings) when command == "du" do
    name_rules =
      if settings.options[:name],
        do: [exclude: "*", include: settings.options[:name]],
        else: []

    with {:ok, name_filter} <- Filter.compile(name_rules),
         {:ok, inventory} <- inventory(target, source_region(context, settings), settings) do
      try do
        limit = settings.options[:limit] || 1000

        selected =
          Manifest.stream(inventory)
          |> Stream.filter(&Filter.include?(settings.filters, &1["name"]))
          |> Stream.filter(fn entry ->
            basename = entry["name"] |> String.split("/") |> List.last()
            Filter.include?(name_filter, basename)
          end)

        summary =
          Enum.reduce(selected, %{count: 0, returned: 0, bytes: 0, entries: []}, fn entry, acc ->
            if acc.returned < limit do
              %{
                acc
                | count: acc.count + 1,
                  returned: acc.returned + 1,
                  bytes: acc.bytes + entry["size"],
                  entries: if(command == "find", do: [entry["name"] | acc.entries], else: [])
              }
            else
              %{acc | count: acc.count + 1}
            end
          end)

        common = %{
          "Partial" => summary.count > summary.returned,
          "MatchedObjects" => summary.count
        }

        data =
          if command == "du",
            do: %{"Objects" => summary.returned, "Bytes" => summary.bytes},
            else: %{"Entries" => Enum.reverse(summary.entries), "Returned" => summary.returned}

        {:ok, Map.merge(common, data)}
      after
        Manifest.cleanup(inventory)
      end
    end
  end

  defp dispatch(_, _, _, _),
    do: failure(:highlevel_route, "This high-level target combination is not available")

  defp list([], [context], settings),
    do: list([%Target{kind: :remote, bucket: nil}], [context], settings)

  defp list([%Target{bucket: nil}], [context], settings) do
    input =
      %{}
      |> optional("Prefix", settings.options[:bucket_name_prefix])
      |> optional("BucketRegion", settings.options[:bucket_region])

    pages =
      Paginator.pages("ListBuckets", input, &Client.call(context, &1, &2, settings.opts),
        page_size: settings.options[:page_size] || 1000
      )

    Enum.reduce_while(pages, :ok, fn
      {:ok, page}, :ok ->
        Enum.each(page["Buckets"] || [], fn bucket ->
          emit(settings, date(bucket["CreationDate"]) <> " " <> bucket["Name"])
        end)

        {:cont, :ok}

      error, :ok ->
        {:halt, error}
    end)
  end

  defp list([target], [context], settings) do
    input = %{"Bucket" => target.bucket, "Prefix" => target.key || ""}
    input = if settings.options[:recursive], do: input, else: Map.put(input, "Delimiter", "/")
    input = Map.merge(input, fields("ListObjectsV2", settings.input.input))

    pages =
      Paginator.pages("ListObjectsV2", input, &Client.call(context, &1, &2, settings.opts),
        page_size: settings.options[:page_size] || 1000
      )

    result =
      Enum.reduce_while(pages, {:ok, 0, 0}, fn
        {:ok, page}, {:ok, count, bytes} ->
          Enum.each(page["CommonPrefixes"] || [], fn item ->
            emit(
              settings,
              "                           PRE " <> display_key(item["Prefix"], target, settings)
            )
          end)

          {count, bytes} =
            Enum.reduce(page["Contents"] || [], {count, bytes}, fn item, {count, bytes} ->
              size = item["Size"] || 0

              text =
                if settings.options[:human_readable],
                  do: human_size(size),
                  else: Integer.to_string(size)

              emit(
                settings,
                date(item["LastModified"]) <>
                  " " <>
                  String.pad_leading(text, 10) <>
                  " " <> display_key(item["Key"], target, settings)
              )

              {count + 1, bytes + size}
            end)

          {:cont, {:ok, count, bytes}}

        error, _ ->
          {:halt, error}
      end)

    case result do
      {:ok, count, bytes} ->
        if settings.options[:summarize] do
          emit(settings, "\nTotal Objects: " <> Integer.to_string(count))

          emit(
            settings,
            "   Total Size: " <>
              if(settings.options[:human_readable],
                do: human_size(bytes),
                else: Integer.to_string(bytes)
              )
          )
        end

        :ok

      error ->
        error
    end
  end

  defp display_key(key, target, settings) do
    key =
      if settings.options[:recursive],
        do: key,
        else: String.replace_prefix(key, target.key || "", "")

    ExStorageServiceCli.Presentation.Scalar.safe(key)
  end

  defp date(%DateTime{} = value), do: Calendar.strftime(value, "%Y-%m-%d %H:%M:%S")
  defp date(_), do: "                   "

  defp human_size(1), do: "1 Byte"
  defp human_size(size) when size < 1024, do: Integer.to_string(size) <> " Bytes"

  defp human_size(size), do: human_size(size / 1024, ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"])

  defp human_size(size, [unit | rest]) do
    if round(size) < 1024 or rest == [],
      do: :erlang.float_to_binary(size / 1, decimals: 1) <> " " <> unit,
      else: human_size(size / 1024, rest)
  end

  defp optional(map, _, nil), do: map
  defp optional(map, key, value), do: Map.put(map, key, value)

  defp presign(context, target, settings) do
    expires = settings.options[:expires_in] || settings.options[:expires] || 3600
    input = %{"Bucket" => target.bucket, "Key" => target.key}

    with true <- is_integer(expires) and expires in 1..604_800,
         {:ok, context} <- Context.refresh(context),
         {:ok, request} <- Encoder.encode("GetObject", input),
         {:ok, url, _endpoint} <- Endpoint.resolve(context, request, input, settings.opts) do
      cond do
        context.unsigned ->
          emit(settings, url)

        true ->
          with {:ok, result} <-
                 ExStorageServiceCli.S3.Presign.url(
                   context,
                   target,
                   "GET",
                   Keyword.put(settings.opts, :expires, expires)
                 ),
               do: emit(settings, result["url"])
      end
    else
      false -> failure(:presign_expiry, "Presign expiry must be between 1 and 604800 seconds")
      error -> error
    end
  end

  defp synchronize(command, source, source_context, destination, destination_context, settings) do
    with :ok <- directory_targets(source, destination),
         {:ok, source_inventory} <- source_inventory(source, source_context, settings) do
      try do
        with :ok <- local_mapping(source_inventory, destination, settings),
             {:ok, destination_inventory} <-
               destination_inventory(command, destination, destination_context, settings) do
          try do
            with {:ok, plan} <-
                   plan(
                     command,
                     source_inventory,
                     destination_inventory,
                     source,
                     destination,
                     settings
                   ) do
              try do
                with :ok <- conflicts(plan),
                     :ok <-
                       copies(
                         command,
                         plan,
                         source,
                         source_context,
                         destination,
                         destination_context,
                         settings
                       ),
                     :ok <-
                       deletions(
                         command,
                         plan,
                         source,
                         source_context,
                         destination,
                         destination_context,
                         settings
                       ),
                     do: :ok
              after
                Manifest.cleanup(plan)
              end
            end
          after
            Manifest.cleanup(destination_inventory)
          end
        end
      after
        Manifest.cleanup(source_inventory)
      end
    end
  end

  defp source_inventory(source, context, %{options: %{rewind: value}} = settings),
    do: VersionSelection.inventory(context, source, value, history_opts(settings))

  defp source_inventory(source, context, settings), do: inventory(source, context, settings)

  defp history_opts(settings),
    do:
      Keyword.merge(settings.opts,
        page_size: settings.options[:page_size] || 1000,
        input: fields("ListObjectVersions", settings.input.input)
      )

  defp inventory(%Target{kind: :local, path: path}, _, settings),
    do:
      Inventory.local(
        path,
        Keyword.merge(settings.opts,
          follow_symlinks: Map.get(settings.options, :follow_symlinks, true)
        )
      )

  defp inventory(%Target{kind: :remote} = target, context, settings),
    do:
      Inventory.remote(
        context,
        target,
        Keyword.merge(settings.opts,
          page_size: settings.options[:page_size] || 1000,
          input: Map.take(settings.input.input, ["RequestPayer"])
        )
      )

  defp destination_inventory(command, _target, _context, settings)
       when command in ["cp", "mv", "get", "put"],
       do: Manifest.build([], settings.opts)

  defp destination_inventory(
         _command,
         %Target{kind: :local, path: path} = target,
         _context,
         settings
       ) do
    case File.lstat(path) do
      {:error, :enoent} -> Manifest.build([], settings.opts)
      _ -> Inventory.local(target.path, Keyword.put(settings.opts, :follow_symlinks, false))
    end
  end

  defp destination_inventory(_command, target, context, settings),
    do: inventory(target, context, settings)

  defp plan(command, source_inventory, destination_inventory, source, destination, settings) do
    policy = if command == "mirror", do: :mirror, else: :aws
    direction = direction(source, destination)

    delete? =
      (command == "sync" and settings.options[:delete] == true) or
        (command == "mirror" and settings.options[:remove] == true)

    records =
      Manifest.pairs(source_inventory, destination_inventory)
      |> Stream.map(fn {a, b} ->
        name = if a, do: a["name"], else: b["name"]

        selected =
          if Filter.include?(settings.filters, name),
            do: NativeTransferSelection.include?(settings.selection, a || b),
            else: {:ok, false}

        with {:ok, selected?} <- selected do
          {action, reason} =
            cond do
              not selected? -> {:skip, :excluded}
              a == nil and delete? -> {:delete, :extraneous}
              a == nil -> {:skip, :destination_only}
              command in ["cp", "mv", "get", "put"] -> {:copy, :requested}
              true -> SyncPolicy.compare(policy, direction, a, b, settings.options)
            end

          %{
            "name" => name,
            "action" => Atom.to_string(action),
            "reason" => Atom.to_string(reason),
            "source" => a,
            "destination" => b
          }
        end
      end)

    Manifest.build(records, settings.opts)
  end

  defp conflicts(plan) do
    if Enum.any?(Manifest.stream(plan), &(&1["action"] == "conflict")),
      do: failure(:mirror_conflict, "Mirror destination differs; explicit overwrite is required"),
      else: :ok
  end

  defp copies("mv", plan, source, source_context, destination, destination_context, settings) do
    if settings.options[:dryrun] == true or settings.options[:dry_run] == true do
      ordinary_copies(
        "mv",
        plan,
        source,
        source_context,
        destination,
        destination_context,
        settings
      )
    else
      move_copies(plan, source, source_context, destination, destination_context, settings)
    end
  end

  defp copies(command, plan, source, source_context, destination, destination_context, settings),
    do:
      ordinary_copies(
        command,
        plan,
        source,
        source_context,
        destination,
        destination_context,
        settings
      )

  defp move_copies(plan, source, source_context, destination, destination_context, settings) do
    deferred = %{settings | opts: Keyword.put(settings.opts, :defer_move, true)}

    records =
      Manifest.stream(plan)
      |> Stream.filter(&(&1["action"] == "copy"))
      |> Stream.map(fn entry ->
        with {:ok, a} <- member(source, entry["name"]),
             {:ok, b} <- member(destination, entry["name"]),
             {:ok, record} <-
               one(
                 "mv",
                 a,
                 source_context,
                 b,
                 destination_context,
                 deferred,
                 entry["source"],
                 nil
               ) do
          {:ok, Map.put(record, "name", entry["name"])}
        else
          :ok ->
            failure(
              :move_source_skipped,
              "A required source was skipped; every source was preserved"
            )

          error ->
            error
        end
      end)

    with {:ok, completed} <- Manifest.build(records, settings.opts) do
      try do
        with :ok <-
               verify_moves(
                 completed,
                 source,
                 source_context,
                 destination,
                 destination_context,
                 settings
               ) do
          Manifest.stream(completed)
          |> Enum.reduce_while(:ok, fn record, :ok ->
            with {:ok, a} <- member(source, record["name"]),
                 {:ok, b} <- member(destination, record["name"]),
                 :ok <- verify_move(a, source_context, b, destination_context, record, settings),
                 :ok <- delete_move_source(a, source_context, record["source"], settings) do
              emit(settings, "move: #{label(a, settings)} to #{label(b, settings)}")
              {:cont, :ok}
            else
              error -> {:halt, error}
            end
          end)
        end
      after
        Manifest.cleanup(completed)
      end
    end
  end

  defp verify_moves(completed, source, source_context, destination, destination_context, settings) do
    Manifest.stream(completed)
    |> Enum.reduce_while(:ok, fn record, :ok ->
      with {:ok, a} <- member(source, record["name"]),
           {:ok, b} <- member(destination, record["name"]),
           :ok <- verify_move(a, source_context, b, destination_context, record, settings) do
        {:cont, :ok}
      else
        error -> {:halt, error}
      end
    end)
  end

  defp verify_move(source, source_context, destination, destination_context, record, settings) do
    with :ok <-
           verify_move_identity(
             destination,
             destination_context,
             record["destination"],
             settings.input.input,
             settings
           ),
         do:
           verify_move_identity(
             source,
             source_context,
             record["source"],
             settings.input.source_input,
             settings
           )
  end

  defp verify_move_identity(%Target{kind: :local}, _, identity, _, _),
    do: Inventory.verify_local(identity)

  defp verify_move_identity(target, context, identity, input, settings) do
    input =
      fields("HeadObject", input)
      |> Map.merge(%{
        "Bucket" => target.bucket,
        "Key" => target.key,
        "IfMatch" => identity["ETag"]
      })
      |> optional("VersionId", identity["VersionId"])

    with {:ok, head} <- Client.call(context, "HeadObject", input, settings.opts) do
      if Map.take(head, ~w(ContentLength ETag VersionId)) == identity,
        do: :ok,
        else:
          failure(
            :move_identity_changed,
            "A copied source or destination changed; source preserved"
          )
    end
  end

  defp delete_move_source(%Target{kind: :local}, _, identity, _) do
    with :ok <- safe_existing_path(identity["path"]),
         :ok <- Inventory.verify_local(identity),
         do: local_delete(identity)
  end

  defp delete_move_source(target, context, identity, settings) do
    with {:ok, _} <-
           Copy.delete_selected(
             context,
             target,
             identity,
             Keyword.put(settings.opts, :source_input, settings.input.source_input)
           ),
         do: :ok
  end

  defp ordinary_copies(
         command,
         plan,
         source,
         source_context,
         destination,
         destination_context,
         settings
       ) do
    Manifest.stream(plan)
    |> Enum.reduce_while({:ok, nil}, fn entry, {:ok, skipped} ->
      if entry["action"] == "copy" do
        with {:ok, a} <- member(source, entry["name"]),
             {:ok, b} <- member(destination, entry["name"]) do
          result =
            if command == "diff" do
              emit(settings, "different: " <> entry["name"])
              :ok
            else
              one(
                command,
                a,
                source_context,
                b,
                destination_context,
                %{
                  settings
                  | opts:
                      Keyword.put(
                        settings.opts,
                        :expect_absent,
                        command in ["sync", "mirror", "diff"]
                      )
                },
                entry["source"],
                entry["destination"]
              )
            end

          case result do
            :ok -> {:cont, {:ok, skipped}}
            {:error, %Error{category: :skipped}} = error -> {:cont, {:ok, skipped || error}}
            error -> {:halt, error}
          end
        else
          error -> {:halt, error}
        end
      else
        {:cont, {:ok, skipped}}
      end
    end)
    |> case do
      {:ok, nil} -> :ok
      {:ok, skipped} -> skipped
      error -> error
    end
  end

  defp deletions(
         command,
         plan,
         source,
         source_context,
         destination,
         destination_context,
         settings
       ) do
    if :atomics.get(settings.opts[:skipped_archives], 1) > 0 do
      :ok
    else
      apply_deletions(
        command,
        plan,
        source,
        source_context,
        destination,
        destination_context,
        settings
      )
    end
  end

  defp apply_deletions(
         command,
         plan,
         source,
         source_context,
         destination,
         destination_context,
         settings
       ) do
    Manifest.stream(plan)
    |> Enum.reduce_while(:ok, fn entry, :ok ->
      if entry["action"] == "delete" and command != "diff" do
        result =
          with {:ok, target} <- member(destination, entry["name"]),
               :ok <- source_absent(source, source_context, entry["name"], settings),
               :ok <-
                 delete_candidate(target, destination_context, entry["destination"], settings),
               do: :ok

        case result do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      else
        {:cont, :ok}
      end
    end)
  end

  defp one(
         command,
         source,
         source_context,
         destination,
         destination_context,
         settings,
         observed,
         old_destination
       ) do
    operation = direction(source, destination)

    cond do
      observed == nil and not Filter.include?(settings.filters, basename(source)) ->
        :ok

      true ->
        with {:ok, observed} <- historical_source(source, source_context, observed, settings),
             {:ok, observed} <- selection_source(source, source_context, observed, settings),
             {:ok, true} <- NativeTransferSelection.include?(settings.selection, observed || %{}) do
          one_selected(
            command,
            operation,
            source,
            source_context,
            destination,
            destination_context,
            settings,
            observed,
            old_destination
          )
        else
          {:ok, false} -> :ok
          :skip -> :ok
          error -> error
        end
    end
  end

  defp historical_source(source, context, nil, %{options: %{rewind: value}} = settings),
    do: VersionSelection.single(context, source, value, history_opts(settings))

  defp historical_source(_source, _context, observed, _settings), do: {:ok, observed}

  defp selection_source(source, context, nil, settings) do
    if NativeTransferSelection.age?(settings.selection) do
      with {:ok, selected} <- capture_source(source, context, nil, settings) do
        if source.kind == :remote do
          {:ok,
           %{
             "key" => source.key,
             "size" => selected["ContentLength"],
             "etag" => selected["ETag"],
             "modified" => modified(selected["LastModified"]),
             "version_id" => selected["VersionId"],
             "storage_class" => selected["StorageClass"]
           }}
        else
          {:ok, selected}
        end
      end
    else
      {:ok, nil}
    end
  end

  defp selection_source(_source, _context, observed, _settings), do: {:ok, observed}

  defp one_selected(
         command,
         operation,
         source,
         source_context,
         destination,
         destination_context,
         settings,
         observed,
         old_destination
       ) do
    cond do
      settings.options[:dryrun] == true or settings.options[:dry_run] == true ->
        with :ok <- dryrun_source(source, source_context, observed, settings) do
          emit(
            settings,
            "(dryrun) #{verb(command, operation)}: #{label(source, settings)} to #{label(destination, settings)}"
          )

          :ok
        end

      true ->
        with {:ok, selected} <- capture_source(source, source_context, observed, settings),
             :ok <- glacier(selected, settings),
             :ok <-
               observable_destination(destination, destination_context, old_destination, settings),
             {:ok, result} <-
               Progress.with(settings.options, settings.opts, fn opts ->
                 transfer(
                   command,
                   operation,
                   source,
                   source_context,
                   destination,
                   destination_context,
                   %{settings | opts: Keyword.put(opts, :selected_source, selected)}
                 )
               end),
             :ok <- preserve_time(operation, destination, observed, result),
             {:ok, move} <-
               if(settings.opts[:defer_move] == true,
                 do: move_record(source, destination, selected, result),
                 else: {:ok, nil}
               ) do
          output =
            if settings.opts[:on_result],
              do:
                settings.opts[:on_result].(
                  Map.merge(result, %{command: command, source: source, destination: destination})
                ),
              else: :ok

          case output do
            {:error, _} = error ->
              error

            _ ->
              if operation != :stdout and settings.opts[:defer_move] != true,
                do:
                  emit(
                    settings,
                    "#{verb(command, operation)}: #{label(source, settings)} to #{label(destination, settings)}"
                  )

              if settings.opts[:defer_move] == true,
                do: {:ok, move},
                else: :ok
          end
        else
          :skip -> :ok
          error -> error
        end
    end
  end

  defp move_record(source, destination, selected, result) do
    source_identity =
      if source.kind == :local, do: {:ok, selected}, else: move_identity(source, selected)

    with {:ok, source_identity} <- source_identity,
         {:ok, destination_identity} <-
           move_identity(destination, Map.put(result.result, "ContentLength", result.bytes)) do
      {:ok, %{"source" => source_identity, "destination" => destination_identity}}
    end
  end

  defp move_identity(%Target{kind: :local, path: path}, _),
    do: Inventory.local_entry(path, Path.basename(path))

  defp move_identity(_, %{"ETag" => etag, "ContentLength" => length} = selected)
       when is_binary(etag) and etag != "" and is_integer(length) and length >= 0,
       do: {:ok, Map.take(selected, ~w(ContentLength ETag VersionId))}

  defp move_identity(_, _),
    do:
      failure(
        :move_identity_missing,
        "Copied destination lacks a confirmed identity; source preserved"
      )

  defp transfer(command, :upload, source, _, destination, context, settings) do
    input = mime(settings.input.input, source.path, settings.options)
    opts = settings.opts |> Keyword.put(:input, fields("PutObject", input))

    with {:ok, descriptor} <- Source.file(source.path, coordinator: context.coordinator),
         {:ok, result} <-
           Engine.upload_source(context, destination.bucket, destination.key, descriptor, opts),
         :ok <- local_move(move_command(command, settings), descriptor),
         do: {:ok, result}
  end

  defp transfer(command, :download, source, context, destination, _, settings) do
    selected = settings.opts[:selected_source]
    input = Map.merge(fields("GetObject", settings.input.input), settings.input.source_input)

    input =
      input
      |> optional("IfMatch", selected["ETag"])
      |> optional("VersionId", selected["VersionId"])

    with {:ok, result} <-
           Engine.download_file(
             context,
             source.bucket,
             source.key,
             destination.path,
             settings.opts
             |> Keyword.put(:input, input)
             |> Keyword.put(
               :verify_selection,
               settings.options[:rewind] != nil or selected["VersionId"] != nil or
                 selected["ETag"] != nil
             )
             |> Keyword.put(:expected_bytes, selected["ContentLength"])
           ),
         :ok <- remote_move(move_command(command, settings), context, source, selected, settings) do
      {:ok, result}
    end
  end

  defp transfer(_, :stdin, _, _, destination, context, settings) do
    Spool.with_source(
      context,
      Keyword.put(settings.opts, :expected_size, settings.options[:expected_size]),
      fn source ->
        Engine.upload_source(
          context,
          destination.bucket,
          destination.key,
          source,
          input_opts(settings, "PutObject")
        )
      end
    )
  end

  defp transfer(_, :stdout, source, context, _, _, settings) do
    selected = settings.opts[:selected_source]

    input =
      Map.merge(fields("GetObject", settings.input.input), settings.input.source_input)
      |> optional("IfMatch", selected["ETag"])
      |> optional("VersionId", selected["VersionId"])

    Engine.download(
      context,
      source.bucket,
      source.key,
      settings.opts[:write] || fn bytes -> IO.binwrite(bytes) end,
      settings.opts
      |> Keyword.put(:input, input)
      |> Keyword.put(
        :verify_selection,
        settings.options[:rewind] != nil or selected["VersionId"] != nil or
          selected["ETag"] != nil
      )
      |> Keyword.put(:expected_bytes, selected["ContentLength"])
    )
  end

  defp transfer(
         command,
         :copy,
         source,
         source_context,
         destination,
         destination_context,
         settings
       ) do
    source_context =
      if settings.options[:source_region],
        do: %{source_context | region: settings.options[:source_region]},
        else: source_context

    selected = settings.opts[:selected_source]

    source_input =
      settings.input.source_input
      |> optional("IfMatch", selected["ETag"])
      |> optional("VersionId", selected["VersionId"])

    opts =
      settings.opts
      |> Keyword.merge(Map.to_list(settings.input))
      |> Keyword.put(:source_input, source_input)

    if move_command(command, settings) == "mv",
      do: Copy.move(source_context, source, destination_context, destination, opts),
      else: Copy.remote(source_context, source, destination_context, destination, opts)
  end

  defp transfer(_, _, _, _, _, _, _),
    do: failure(:transfer_direction, "Transfers require at least one S3 target")

  defp move_command("mv", %{opts: opts}), do: if(opts[:defer_move], do: "cp", else: "mv")
  defp move_command(command, _), do: command

  defp selected_version_source(source, %{options: options}) do
    cond do
      options[:rewind] != nil and options[:version_id] != nil ->
        failure(:version_selection, "Choose --rewind or --version-id")

      options[:rewind] != nil and source.kind != :remote ->
        failure(:version_selection, "Rewind requires an S3 source")

      options[:rewind] != nil ->
        with {:ok, _} <- VersionSelection.cutoff(options[:rewind]), do: :ok

      options[:version_id] != nil and
          (source.kind != :remote or source.bucket in [nil, ""] or source.key in [nil, ""]) ->
        failure(:version_selection, "A selected version requires one S3 source object")

      true ->
        :ok
    end
  end

  defp local_move("mv", source) do
    with :ok <- Source.validate(source) do
      case File.rm(source.path) do
        :ok ->
          :ok

        _ ->
          failure(:move_source_delete, "Upload completed but local source could not be removed")
      end
    end
  end

  defp local_move(_, _), do: :ok

  defp remote_move("mv", context, target, selected, settings) do
    with {:ok, _} <- Copy.delete_selected(context, target, selected, settings.opts), do: :ok
  end

  defp remote_move(_, _, _, _, _), do: :ok

  defp preserve_time(:download, destination, observed, result) do
    date = if observed, do: observed["modified"], else: modified(result.result["LastModified"])

    if is_integer(date) and date > 0 do
      case File.touch(destination.path, div(date, 1_000_000)) do
        :ok -> :ok
        _ -> failure(:timestamp_restore, "Download completed but timestamp could not be restored")
      end
    else
      :ok
    end
  end

  defp preserve_time(_, _, _, _), do: :ok

  defp remove_bucket_objects(context, target, settings),
    do:
      if(settings.options[:force],
        do: remove_recursive(context, %{target | key: ""}, settings),
        else: :ok
      )

  defp remove_recursive(context, target, settings) do
    with {:ok, inventory} <- inventory(target, context, settings) do
      try do
        Manifest.stream(inventory)
        |> Stream.filter(&Filter.include?(settings.filters, &1["name"]))
        |> Stream.chunk_every(100)
        |> Enum.reduce_while(:ok, fn batch, :ok ->
          case remove_batch(context, target, batch, settings, false) do
            :ok -> {:cont, :ok}
            error -> {:halt, error}
          end
        end)
      after
        Manifest.cleanup(inventory)
      end
    end
  end

  defp remove_versions(context, target, settings) do
    options =
      Keyword.merge(settings.opts,
        exact: settings.options[:recursive] != true,
        page_size: settings.options[:page_size] || 1000,
        input: fields("ListObjectVersions", settings.input.input)
      )

    with {:ok, inventory} <- Inventory.versions(context, target, options) do
      try do
        Manifest.stream(inventory)
        |> Stream.filter(&Filter.include?(settings.filters, &1["object_name"]))
        |> Stream.chunk_every(100)
        |> Enum.reduce_while(:ok, fn batch, :ok ->
          case remove_batch(context, target, batch, settings, true) do
            :ok -> {:cont, :ok}
            error -> {:halt, error}
          end
        end)
      after
        Manifest.cleanup(inventory)
      end
    end
  end

  defp remove_batch(context, target, batch, settings, versions) do
    if settings.options[:dryrun] == true or settings.options[:dry_run] == true do
      Enum.each(batch, fn entry ->
        emit(
          settings,
          "(dryrun) delete: " <>
            label(%{target | key: entry["key"]}, settings) <> version_label(entry)
        )
      end)

      :ok
    else
      with {:ok, objects} <- delete_identifiers(context, target, batch, settings, versions),
           {:ok, result} <-
             Client.call(
               context,
               "DeleteObjects",
               Map.merge(fields("DeleteObjects", settings.input.input), %{
                 "Bucket" => target.bucket,
                 "Delete" => %{"Objects" => objects, "Quiet" => false}
               }),
               settings.opts
             ),
           :ok <- batch_result(objects, result) do
        Enum.each(batch, fn entry ->
          emit(
            settings,
            "delete: " <> label(%{target | key: entry["key"]}, settings) <> version_label(entry)
          )
        end)

        :ok
      end
    end
  end

  defp delete_identifiers(_context, _target, batch, _settings, true),
    do: {:ok, Enum.map(batch, &%{"Key" => &1["key"], "VersionId" => &1["version_id"]})}

  defp delete_identifiers(context, target, batch, settings, false) do
    if context.frontend == :aws or context.capabilities[:conditional_delete] == true do
      Enum.reduce_while(batch, {:ok, []}, fn entry, {:ok, objects} ->
        case Inventory.verify_remote(
               context,
               target,
               entry,
               Keyword.put(settings.opts, :input, fields("HeadObject", settings.input.input))
             ) do
          {:ok, head} ->
            # Current-object removal intentionally creates a delete marker on versioned buckets.
            {:cont, {:ok, [%{"Key" => entry["key"], "ETag" => head["ETag"]} | objects]}}

          error ->
            {:halt, error}
        end
      end)
      |> case do
        {:ok, objects} -> {:ok, Enum.reverse(objects)}
        error -> error
      end
    else
      failure(
        :conditional_delete_unavailable,
        "Provider has no verified conditional batch deletion contract; objects were preserved"
      )
    end
  end

  defp batch_result(objects, result) do
    errors = result["Errors"] || []
    deleted = result["Deleted"] || []
    expected = MapSet.new(objects, &{&1["Key"], &1["VersionId"]})
    actual = MapSet.new(deleted, &{&1["Key"], &1["VersionId"]})

    cond do
      errors != [] ->
        {:error,
         Error.new(:transfer, "Batch deletion contains per-object failures",
           code: :batch_delete_failed,
           details: %{
             failed: length(errors),
             codes: errors |> Enum.map(& &1["Code"]) |> Enum.uniq()
           }
         )}

      expected != actual ->
        failure(:batch_delete_unconfirmed, "Provider did not confirm every selected deletion")

      true ->
        :ok
    end
  end

  defp version_label(%{"version_id" => version}),
    do: " (version " <> ExStorageServiceCli.Presentation.Scalar.safe(version) <> ")"

  defp version_label(_), do: ""

  defp remove_object(context, target, settings) do
    cond do
      not Filter.include?(settings.filters, basename(target)) ->
        :ok

      settings.options[:dryrun] == true or settings.options[:dry_run] == true ->
        emit(settings, "(dryrun) delete: " <> label(target, settings))
        :ok

      true ->
        with {:ok, _} <-
               Client.call(
                 context,
                 "DeleteObject",
                 Map.merge(
                   %{"Bucket" => target.bucket, "Key" => target.key},
                   fields("DeleteObject", settings.input.input)
                 )
                 |> optional("VersionId", settings.options[:version_id]),
                 settings.opts
               ) do
          emit(settings, "delete: " <> label(target, settings))
          :ok
        end
    end
  end

  defp delete_candidate(target, context, entry, settings) do
    if settings.options[:dryrun] == true or settings.options[:dry_run] do
      emit(settings, "(dryrun) delete: " <> label(target, settings))
      :ok
    else
      case target.kind do
        :remote ->
          with {:ok, head} <- Inventory.verify_remote(context, target, entry, settings.opts),
               {:ok, _} <- Copy.delete_selected(context, target, head, settings.opts) do
            emit(settings, "delete: " <> label(target, settings))
            :ok
          end

        :local ->
          with :ok <- safe_existing_path(entry["path"]),
               :ok <- Inventory.verify_local(entry),
               :ok <- local_delete(entry) do
            emit(settings, "delete: " <> label(target, settings))
            :ok
          end
      end
    end
  end

  defp local_delete(entry) do
    case File.lstat(entry["path"], time: :posix) do
      {:ok, %{type: :regular}} ->
        case File.rm(entry["path"]) do
          :ok -> :ok
          _ -> failure(:delete_failed, "Cannot remove selected local file")
        end

      _ ->
        failure(:inventory_changed, "Local deletion candidate is no longer a regular file")
    end
  end

  defp source_absent(%Target{kind: :local} = source, _, name, _) do
    with {:ok, path} <- Sink.map(source.path, name) do
      case File.lstat(path) do
        {:error, :enoent} -> :ok
        _ -> failure(:inventory_changed, "Source changed before deletion; candidate preserved")
      end
    end
  end

  defp source_absent(source, context, name, settings) do
    with {:ok, target} <- member(source, name) do
      case Client.call(
             context,
             "HeadObject",
             %{"Bucket" => target.bucket, "Key" => target.key},
             settings.opts
           ) do
        {:error, %Error{http_status: 404}} ->
          :ok

        _ ->
          failure(
            :inventory_changed,
            "Source is present or cannot be revalidated; deletion candidate preserved"
          )
      end
    end
  end

  defp dryrun_source(source, context, observed, %{options: %{rewind: _}} = settings) do
    case capture_source(source, context, observed, settings) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp dryrun_source(
         %Target{kind: :remote} = source,
         context,
         nil,
         %{invocation: %{frontend: :s3}} = settings
       ) do
    case capture_source(source, context, nil, settings) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp dryrun_source(_source, _context, _observed, _settings), do: :ok

  defp capture_source(%Target{kind: :stdio}, _, _, _), do: {:ok, %{}}

  defp capture_source(%Target{kind: :local} = source, _, observed, settings) do
    with :ok <- single_symlink(source.path, settings),
         {:ok, entry} <- Inventory.local_entry(source.path, basename(source)),
         :ok <- if(observed, do: Inventory.verify_local(observed), else: :ok),
         do: {:ok, entry}
  end

  defp capture_source(target, context, nil, settings) do
    input =
      Map.merge(fields("HeadObject", settings.input.input), settings.input.source_input)
      |> Map.merge(%{"Bucket" => target.bucket, "Key" => target.key})

    with {:ok, result} <- Client.call(context, "HeadObject", input, settings.opts) do
      if input["VersionId"] != nil and result["VersionId"] != input["VersionId"],
        do:
          failure(
            :source_version_mismatch,
            "Provider did not confirm the selected source version"
          ),
        else: {:ok, result}
    end
  end

  defp capture_source(target, context, entry, settings),
    do:
      Inventory.verify_remote(
        context,
        target,
        entry,
        Keyword.put(
          settings.opts,
          :input,
          Map.merge(fields("HeadObject", settings.input.input), settings.input.source_input)
        )
      )

  defp single_symlink(path, settings) do
    case File.lstat(path) do
      {:ok, %{type: :symlink}}
      when is_map_key(settings.options, :follow_symlinks) and
             settings.options.follow_symlinks == false ->
        :skip

      {:error, _} ->
        failure(:local_inventory, "Cannot inspect local source")

      _ ->
        :ok
    end
  end

  defp glacier(selected, settings) do
    if selected["StorageClass"] in ["GLACIER", "DEEP_ARCHIVE"] and
         settings.options[:force_glacier_transfer] != true do
      if settings.options[:ignore_glacier_warnings] do
        :atomics.add(settings.opts[:skipped_archives], 1, 1)
        :skip
      else
        warning(
          settings,
          "warning: Archived object requires restoration or --force-glacier-transfer"
        )

        {:error,
         Error.new(:skipped, "Archived object was skipped",
           code: :glacier_object,
           details: %{warning_emitted: true}
         )}
      end
    else
      :ok
    end
  end

  defp warning(settings, message) do
    if settings.opts[:stderr],
      do: settings.opts[:stderr].(message),
      else: IO.puts(:stderr, message)

    :ok
  end

  defp source_region(nil, _), do: nil

  defp source_region(context, settings) do
    if settings.options[:source_region],
      do: %{context | region: settings.options[:source_region]},
      else: context
  end

  defp raw_head(target, context, settings) do
    mode = if settings.options[:bytes] != nil, do: :bytes, else: :lines
    count = settings.options[mode] || 10

    if count == 0 do
      :ok
    else
      reference = make_ref()
      remaining = :atomics.new(1, signed: false)
      :atomics.put(remaining, 1, count)
      write = settings.opts[:write] || fn bytes -> IO.binwrite(bytes) end

      consumer = fn chunk ->
        left = :atomics.get(remaining, 1)

        {bytes, rest, finished} =
          if mode == :bytes do
            length = min(left, byte_size(chunk))
            {binary_part(chunk, 0, length), left - length, length == left}
          else
            line_prefix(chunk, left, 0)
          end

        with :ok <- write.(bytes) do
          :atomics.put(remaining, 1, rest)
          if finished, do: throw({:head_complete, reference})
          :ok
        end
      end

      try do
        with {:ok, opts} <-
               ExStorageServiceCli.CLI.NativeInspection.source(
                 target,
                 context,
                 settings.options,
                 input_opts(settings, "GetObject")
               ),
             {:ok, _} <-
               Engine.download(
                 context,
                 target.bucket,
                 target.key,
                 consumer,
                 opts
               ),
             do: :ok
      catch
        {:head_complete, ^reference} -> :ok
      end
    end
  end

  defp line_prefix(chunk, remaining, offset) do
    case :binary.match(chunk, "\n", scope: {offset, byte_size(chunk) - offset}) do
      {position, 1} when remaining == 1 -> {binary_part(chunk, 0, position + 1), 0, true}
      {position, 1} -> line_prefix(chunk, remaining - 1, position + 1)
      :nomatch -> {chunk, remaining, false}
    end
  end

  defp observable_destination(target, context, nil, settings) do
    if settings.opts[:expect_absent], do: target_absent(target, context, settings), else: :ok
  end

  defp observable_destination(%Target{kind: :local}, _, entry, _),
    do: Inventory.verify_local(entry)

  defp observable_destination(target, context, entry, settings) do
    case Inventory.verify_remote(context, target, entry, settings.opts) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp target_absent(%Target{kind: :local, path: path}, _, _) do
    case File.lstat(path) do
      {:error, :enoent} -> :ok
      _ -> failure(:inventory_changed, "Destination appeared after inventory")
    end
  end

  defp target_absent(target, context, settings) do
    case Client.call(
           context,
           "HeadObject",
           %{"Bucket" => target.bucket, "Key" => target.key},
           settings.opts
         ) do
      {:error, %Error{http_status: 404}} -> :ok
      _ -> failure(:inventory_changed, "Destination appeared or cannot be revalidated")
    end
  end

  defp destination(source, %Target{kind: :remote, key: key} = target) do
    if target.trailing_slash or key == "",
      do: {:ok, %{target | key: key <> basename(source)}},
      else: {:ok, target}
  end

  defp destination(source, %Target{kind: :local} = target) do
    if target.trailing_slash or File.dir?(target.path),
      do:
        with(
          {:ok, path} <- Sink.map(target.path, basename(source)),
          do: {:ok, %{target | path: path}}
        ),
      else: {:ok, target}
  end

  defp destination(_, target), do: {:ok, target}

  defp member(%Target{kind: :remote} = target, name),
    do: {:ok, %{target | key: Inventory.prefix(target.key) <> name, trailing_slash: false}}

  defp member(%Target{kind: :local} = target, name),
    do:
      with(
        {:ok, path} <- Sink.map(target.path, name),
        do: {:ok, %{target | path: path, trailing_slash: false}}
      )

  defp basename(%Target{kind: :stdio}), do: ""
  defp basename(%Target{kind: :local, path: path}), do: Path.basename(path)
  defp basename(%Target{key: key}), do: key |> String.split("/") |> List.last()
  defp direction(%Target{kind: :stdio}, %Target{kind: :remote}), do: :stdin
  defp direction(%Target{kind: :remote}, %Target{kind: :stdio}), do: :stdout
  defp direction(%Target{kind: :local}, %Target{kind: :remote}), do: :upload
  defp direction(%Target{kind: :remote}, %Target{kind: :local}), do: :download
  defp direction(%Target{kind: :remote}, %Target{kind: :remote}), do: :copy
  defp direction(_, _), do: :invalid

  defp directory_targets(source, destination),
    do:
      if(source.kind in [:local, :remote] and destination.kind in [:local, :remote],
        do: :ok,
        else:
          failure(
            :directory_target,
            "Recursive operations require directory or bucket-prefix targets"
          )
      )

  defp local_mapping(inventory, %Target{kind: :local} = destination, settings) do
    with :ok <- Inventory.collision_check(inventory, settings.opts) do
      Enum.reduce_while(Manifest.stream(inventory), :ok, fn entry, :ok ->
        with {:ok, path} <- Sink.map(destination.path, entry["name"]),
             :ok <- safe_existing_path(path) do
          {:cont, :ok}
        else
          error -> {:halt, error}
        end
      end)
    end
  end

  defp local_mapping(_, _, _), do: :ok

  defp safe_existing_path(path) do
    parts = Path.expand(path) |> Path.split()

    Enum.with_index(parts)
    |> Enum.reduce_while("", fn {part, index}, previous ->
      current = if previous == "", do: part, else: Path.join(previous, part)

      case File.lstat(current) do
        {:error, :enoent} ->
          {:halt, :ok}

        {:ok, %{type: :directory}} when index < length(parts) - 1 ->
          {:cont, current}

        {:ok, %{type: :regular}} when index == length(parts) - 1 ->
          {:halt, :ok}

        _ ->
          {:halt,
           failure(:unsafe_destination, "Destination contains a symlink or incompatible entry")}
      end
    end)
    |> case do
      value when is_binary(value) -> :ok
      result -> result
    end
  end

  defp distinct(source, a, destination, b, recursive) do
    same =
      cond do
        source.kind == :local and destination.kind == :local ->
          left = Path.expand(source.path)
          right = Path.expand(destination.path)

          left == right or
            (recursive and
               (String.starts_with?(left, right <> "/") or String.starts_with?(right, left <> "/")))

        source.kind == :remote and destination.kind == :remote and a.endpoint == b.endpoint and
            source.bucket == destination.bucket ->
          source.key == destination.key or
            (recursive and
               (String.starts_with?(
                  Inventory.prefix(source.key),
                  Inventory.prefix(destination.key)
                ) or
                  String.starts_with?(
                    Inventory.prefix(destination.key),
                    Inventory.prefix(source.key)
                  )))

        true ->
          false
      end

    if same,
      do:
        failure(
          :overlapping_targets,
          "Source and destination select the same or overlapping tree"
        ),
      else: :ok
  end

  defp native_make_bucket(context, target, input, settings) do
    input =
      if settings.options[:with_lock],
        do: Map.put(input, "ObjectLockEnabledForBucket", true),
        else: input

    with {:ok, created} <- native_bucket_create(context, input, settings),
         :ok <- native_bucket_versioning(context, target, created, settings) do
      {:ok,
       %{
         "Bucket" => target.bucket,
         "Created" => created,
         "AlreadyExists" => not created,
         "ObjectLockEnabledOnCreate" => created and settings.options[:with_lock] == true,
         "VersioningEnabled" =>
           settings.options[:with_versioning] == true or
             (created and settings.options[:with_lock] == true)
       }}
    end
  end

  defp native_bucket_create(context, input, settings) do
    case Client.call(context, "CreateBucket", input, settings.opts) do
      {:ok, _} ->
        {:ok, true}

      {:error, %Error{code: code}} = error
      when code in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"] ->
        if settings.options[:ignore_existing], do: {:ok, false}, else: error

      error ->
        error
    end
  end

  defp native_bucket_versioning(context, target, created, settings) do
    if settings.options[:with_versioning] do
      input = %{"Bucket" => target.bucket, "VersioningConfiguration" => %{"Status" => "Enabled"}}

      case Client.call(context, "PutBucketVersioning", input, settings.opts) do
        {:ok, _} ->
          :ok

        {:error, %Error{} = error} ->
          state = if created, do: "Bucket created", else: "Existing bucket retained"

          {:error,
           %{
             error
             | message: state <> "; enabling versioning failed: " <> error.message,
               details:
                 Map.merge(error.details, %{
                   bucket_created: created,
                   bucket_already_exists: not created
                 })
           }}
      end
    else
      :ok
    end
  end

  defp bucket_target(%Target{kind: :remote, key: ""}), do: :ok
  defp bucket_target(_), do: failure(:bucket_target, "This command requires a bucket target")

  defp fields(operation, input) do
    {:ok, operation} = OperationCatalog.operation(operation)
    members = OperationCatalog.shape(operation["input"]["shape"])["members"] || %{}
    Map.take(input, Map.keys(members))
  end

  defp input_opts(settings, operation) do
    input = fields(operation, settings.input.input)

    input =
      if operation in ["GetObject", "HeadObject"],
        do: optional(input, "VersionId", settings.options[:version_id]),
        else: input

    Keyword.put(settings.opts, :input, input)
  end

  defp mime(input, _path, %{guess_mime_type: false}), do: input
  defp mime(%{"ContentType" => _} = input, _, _), do: input

  defp mime(input, path, _) do
    type = Mime.guess(path)
    if type, do: Map.put(input, "ContentType", type), else: input
  end

  defp modified(%DateTime{} = date), do: DateTime.to_unix(date, :microsecond)
  defp modified(_), do: nil

  defp label(%Target{kind: :local, path: path}, %{invocation: %{frontend: :s3}}) do
    absolute = Path.expand(path)
    directory = Path.relative_to(Path.dirname(absolute), File.cwd!(), force: true)
    ExStorageServiceCli.Presentation.Scalar.safe(directory <> "/" <> Path.basename(absolute))
  end

  defp label(target, _settings), do: label(target)
  defp label(%Target{kind: :stdio}), do: "-"

  defp label(%Target{kind: :local, path: path}),
    do: ExStorageServiceCli.Presentation.Scalar.safe(path)

  defp label(%Target{bucket: bucket, key: key}),
    do: ExStorageServiceCli.Presentation.Scalar.safe("s3://" <> bucket <> "/" <> key)

  defp verb(_, :stdin), do: "upload"
  defp verb(_, :stdout), do: "download"
  defp verb("mv", _), do: "move"
  defp verb(_, :upload), do: "upload"
  defp verb(_, :download), do: "download"
  defp verb(_, _), do: "copy"

  defp emit(settings, text) do
    unless settings.options[:quiet] == true or settings.options[:only_show_errors] == true or
             settings.options[:output] == "off" do
      if settings.opts[:emit], do: settings.opts[:emit].(text), else: IO.puts(text)
    end

    :ok
  end

  defp pipe_size?(nil), do: true
  defp pipe_size?(value) when is_integer(value), do: value >= 0

  defp pipe_size?(value) when is_binary(value) do
    case Integer.parse(value) do
      {number, ""} -> number >= 0
      _ -> false
    end
  end

  defp pipe_size?(_), do: false

  defp supported_flags(command, options) do
    cond do
      command == "mirror" and
          Enum.any?([:size_only, :exact_timestamps, :delete], &Map.has_key?(options, &1)) ->
        failure(:mirror_option, "Mirror uses its explicit overwrite and remove policy")

      command == "diff" and Map.has_key?(options, :delete) ->
        failure(:diff_option, "Diff does not accept deletion options")

      command == "pipe" and
          Enum.any?(
            [
              :include,
              :exclude,
              :follow_symlinks,
              :guess_mime_type,
              :source_region,
              :page_size,
              :ignore_glacier_warnings,
              :force_glacier_transfer,
              :checksum_mode,
              :sse_c_copy_source,
              :sse_c_copy_source_key,
              :copy_props,
              :metadata_directive,
              :recursive,
              :version_id,
              :rewind
            ],
            &Map.has_key?(options, &1)
          ) ->
        failure(:pipe_option, "Pipe does not accept source, listing or copy-only options")

      command == "pipe" and not pipe_size?(options[:expected_size]) ->
        failure(:pipe_option, "Expected size must be a nonnegative integer")

      options[:version_id] == "" ->
        failure(:version_selection, "A selected version ID must not be empty")

      options[:progress_frequency] != nil and options[:progress_frequency] < 0 ->
        failure(:progress_frequency, "Progress frequency must be nonnegative")

      options[:all_versions] == true and options[:version_id] != nil ->
        failure(:version_selection, "Choose --all-versions or --version-id")

      options[:version_id] != nil and options[:recursive] == true ->
        failure(:version_selection, "A selected version requires one object target")

      command == "rm" and options[:versions] == true ->
        failure(
          :version_selection,
          "Use --all-versions to authorize version deletion; --versions is inspection only"
        )

      options[:bytes] != nil and options[:lines] != nil ->
        failure(:head_limit, "Choose either --bytes or --lines")

      Enum.any?([:bytes, :lines], fn key ->
        options[key] != nil and (not is_integer(options[key]) or options[key] < 0)
      end) ->
        failure(:head_limit, "Head limits must be nonnegative integers")

      options[:limit] != nil and
          (not is_integer(options[:limit]) or options[:limit] < 0 or options[:limit] > 10_000) ->
        failure(:result_limit, "Result limit must be between zero and 10000")

      true ->
        :ok
    end
  end

  defp frontend_error({:error, %Error{} = error}, %{frontend: :s3, command: [command]}, targets)
       when command in ~w(cp mv sync rm mb rb) do
    prefix =
      case command do
        "mb" -> "make_bucket failed: " <> label(hd(targets)) <> " "
        "rb" -> "remove_bucket failed: " <> label(hd(targets)) <> " "
        _ -> "fatal error: "
      end

    {:error, %{error | details: Map.put(error.details, :high_level_prefix, prefix)}}
  end

  defp frontend_error(result, _invocation, _targets), do: result

  defp failure(code, message), do: {:error, Error.new(:transfer, message, code: code)}
end
