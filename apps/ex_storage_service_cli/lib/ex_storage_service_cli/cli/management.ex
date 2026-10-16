defmodule ExStorageServiceCli.CLI.Management do
  @moduledoc "Native management adapters over the complete, shared S3 operation catalog."
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.S3.{Client, Customizations, Input, Paginator}

  # Documents are complete API structures, never reconstructed from a presenter.
  @families %{
    "policy" => {"BucketPolicy", "Policy", :bucket},
    "cors" => {"BucketCors", "CORSConfiguration", :bucket},
    "ilm" => {"BucketLifecycleConfiguration", "LifecycleConfiguration", :bucket},
    "replicate" => {"BucketReplication", "ReplicationConfiguration", :bucket},
    "event" => {"BucketNotificationConfiguration", "NotificationConfiguration", :bucket},
    "encrypt" => {"BucketEncryption", "ServerSideEncryptionConfiguration", :bucket},
    "ownership" => {"BucketOwnershipControls", "OwnershipControls", :bucket},
    "public-access" => {"PublicAccessBlock", "PublicAccessBlockConfiguration", :bucket},
    "lock" => {"ObjectLockConfiguration", "ObjectLockConfiguration", :bucket},
    "retention" => {"ObjectRetention", "Retention", :object},
    "legalhold" => {"ObjectLegalHold", "LegalHold", :object},
    "tag" => {"Tagging", "Tagging", :either},
    "acl" => {"Acl", "AccessControlPolicy", :either}
  }
  @version_flags %{"version-id" => {:version_id, :string}}
  @tag_selection_flags Map.merge(@version_flags, %{
                         "vid" => {:version_id, :string},
                         "recursive" => {:recursive, :boolean},
                         "versions" => {:versions, :boolean},
                         "rewind" => {:rewind, :string},
                         "limit" => {:limit, :integer}
                       })
  @write_flags %{"dry-run" => {:dry_run, :boolean}}

  def commands do
    families =
      for {family, {_suffix, _field, scope}} <- @families,
          action <- ~w(get set remove),
          into: %{} do
        flags = if action == "get", do: %{}, else: @write_flags
        flags = if scope in [:object, :either], do: Map.merge(flags, @version_flags), else: flags
        flags = if family == "tag", do: Map.merge(flags, @tag_selection_flags), else: flags

        flags =
          if family == "tag" and action == "set",
            do: Map.put(flags, "exclude-folders", {:exclude_folders, :boolean}),
            else: flags

        flags =
          if family == "retention" and action == "set",
            do:
              Map.put(
                flags,
                "bypass-governance-retention",
                {:bypass_governance_retention, :boolean}
              ),
            else: flags

        flags =
          if family == "policy" and action == "set",
            do: Map.put(flags, "replace", {:replace, :boolean}),
            else: flags

        {[family, action],
         {if(action == "set", do: 2..2, else: 1..1), flags,
          "#{String.capitalize(action)} the full #{family} document"}}
      end

    families
    |> Map.merge(convenience_commands())
    |> Map.merge(ExStorageServiceCli.CLI.NativeReplication.commands())
    |> Map.merge(ExStorageServiceCli.CLI.NativeAnonymous.commands())
    |> Map.merge(ExStorageServiceCli.CLI.NativeRestore.commands())
    |> Map.merge(ExStorageServiceCli.CLI.NativeGovernance.commands())
    |> Map.merge(%{
      ["version", "enable"] => {1..1, @write_flags, "Enable bucket versioning"},
      ["version", "suspend"] => {1..1, @write_flags, "Suspend bucket versioning"},
      ["version", "info"] => {1..1, %{}, "Inspect bucket versioning"},
      ["version", "list"] =>
        {1..1, %{"limit" => {:limit, :integer}, "starting-token" => {:starting_token, :string}},
         "List a bounded page of object versions and delete markers"},
      ["undo"] =>
        {1..1,
         Map.merge(
           @write_flags,
           Map.put(@version_flags, "delete-marker-id", {:delete_marker_id, :string})
         ), "Restore an explicit data version or remove an explicit delete marker"},
      ["anonymous", "get"] => {1..1, %{}, "Read the bucket policy without changing access"},
      ["anonymous", "set"] =>
        {2..2, Map.put(@write_flags, "replace", {:replace, :boolean}),
         "Plan or explicitly replace an anonymous-access policy"}
    })
    |> then(fn commands ->
      aliases =
        for {name, action} <-
              [
                {"add", "add"},
                {"edit", "edit"},
                {"ls", "list"},
                {"rm", "remove"},
                {"export", "export"},
                {"import", "import"}
              ],
            into: %{},
            do: {["ilm", name], Map.fetch!(commands, ["ilm", "rule", action])}

      commands
      |> Map.merge(aliases)
      |> Map.put(["encrypt", "clear"], Map.fetch!(commands, ["encrypt", "remove"]))
      |> Map.put(["encrypt", "info"], Map.fetch!(commands, ["encrypt", "get"]))
    end)
  end

  def target_argument(%{command: ["anonymous", "set"], args: [_mode, target]}), do: target
  def target_argument(%{command: ["anonymous", "set-json"], args: [_path, target]}), do: target

  def target_argument(%{command: ["retention", "set"], args: [_mode, _duration, target]}),
    do: target

  def target_argument(%{args: [target | _]}), do: target

  def run(invocation, %Target{kind: :remote} = target, context, opts \\ []) do
    call = opts[:call] || fn operation, input -> Client.call(context, operation, input, opts) end

    partition =
      cond do
        String.starts_with?(context.region, "cn-") -> "aws-cn"
        String.starts_with?(context.region, "us-gov-") -> "aws-us-gov"
        true -> "aws"
      end

    with :ok <- ExStorageServiceCli.CLI.NativeGovernance.document_options(invocation) do
      if ExStorageServiceCli.CLI.NativeGovernance.handles?(invocation) or
           tag_selection?(invocation) do
        [family, action] = invocation.command

        action =
          if family == "tag",
            do:
              Map.fetch!(
                %{"get" => "info", "list" => "info", "set" => "set", "remove" => "clear"},
                action
              ),
            else: action

        ExStorageServiceCli.CLI.NativeGovernance.run(
          family,
          action,
          invocation.args,
          target,
          context,
          invocation.options,
          call,
          opts
        )
      else
        case invocation.command do
          ["ilm", "restore"] ->
            ExStorageServiceCli.CLI.NativeRestore.run(
              target,
              context,
              invocation.options,
              call,
              opts
            )

          ["anonymous", action] when action in ~w(get-json set-json list links) ->
            ExStorageServiceCli.CLI.NativeAnonymous.run(
              action,
              invocation.args,
              target,
              context,
              invocation.options,
              call,
              opts
            )

          _ ->
            execute(
              invocation.command,
              invocation.args,
              invocation.options
              |> Map.put(:partition, partition)
              |> Map.put(:stdin, opts[:stdin] || :stdio),
              target,
              call
            )
        end
      end
    end
  end

  defp tag_selection?(%{command: ["tag", _], options: options}),
    do:
      Enum.any?([:recursive, :versions, :rewind, :limit], &Map.has_key?(options, &1)) or
        options[:exclude_folders] == true

  defp tag_selection?(_), do: false

  defp convenience_commands do
    fields = %{
      "prefix" => {:prefix, :string},
      "tags" => {:tags, :string},
      "size-lt" => {:size_lt, :string},
      "size-gt" => {:size_gt, :string},
      "expire-days" => {:expire_days, :integer},
      "expiry-days" => {:expire_days, :integer},
      "expiry-date" => {:expiry_date, :string},
      "expire-delete-marker" => {:expire_delete_marker, :boolean},
      "expired-object-delete-marker" => {:expire_delete_marker, :boolean},
      "transition-days" => {:transition_days, :integer},
      "transition-date" => {:transition_date, :string},
      "transition-tier" => {:transition_tier, :string},
      "tier" => {:transition_tier, :string},
      "storage-class" => {:storage_class, :string},
      "noncurrent-expire-days" => {:noncurrent_expire_days, :integer},
      "noncurrentversion-expiration-days" => {:noncurrent_expire_days, :integer},
      "noncurrent-expire-newer" => {:noncurrent_expire_newer, :integer},
      "newer-noncurrentversions-expiration" => {:noncurrent_expire_newer, :integer},
      "noncurrent-transition-days" => {:noncurrent_transition_days, :integer},
      "noncurrentversion-transition-days" => {:noncurrent_transition_days, :integer},
      "noncurrent-transition-newer" => {:noncurrent_transition_newer, :integer},
      "newer-noncurrentversions-transition" => {:noncurrent_transition_newer, :integer},
      "noncurrent-transition-tier" => {:noncurrent_transition_tier, :string},
      "noncurrentversion-transition-storage-class" => {:noncurrent_transition_tier, :string},
      "noncurrentversion-tier" => {:noncurrent_transition_tier, :string}
    }

    event = %{
      "event" => {:event, :string},
      "prefix" => {:prefix, :string},
      "suffix" => {:suffix, :string}
    }

    %{
      ["ilm", "rule", "add"] =>
        {1..1, Map.merge(fields, @write_flags), "Append one portable lifecycle rule"},
      ["ilm", "rule", "edit"] =>
        {1..1,
         Map.merge(
           fields,
           Map.merge(@write_flags, %{
             "id" => {:id, :string},
             "enable" => {:enable, :boolean},
             "disable" => {:disable, :boolean}
           })
         ), "Edit selected lifecycle rule fields"},
      ["ilm", "rule", "list"] =>
        {1..1, %{"expiry" => {:expiry, :boolean}, "transition" => {:transition, :boolean}},
         "List lifecycle rules or action projections"},
      ["ilm", "rule", "ls"] =>
        {1..1, %{"expiry" => {:expiry, :boolean}, "transition" => {:transition, :boolean}},
         "List lifecycle rules or action projections"},
      ["ilm", "rule", "remove"] =>
        {1..1,
         Map.merge(@write_flags, %{
           "id" => {:id, :string},
           "all" => {:all, :boolean},
           "force" => {:force, :boolean}
         }), "Remove a selected rule or all rules with explicit force"},
      ["ilm", "rule", "rm"] =>
        {1..1,
         Map.merge(@write_flags, %{
           "id" => {:id, :string},
           "all" => {:all, :boolean},
           "force" => {:force, :boolean}
         }), "Remove a selected rule or all rules with explicit force"},
      ["ilm", "rule", "export"] => {1..1, %{}, "Export complete lifecycle configuration"},
      ["ilm", "rule", "import"] =>
        {1..1, @write_flags, "Replace lifecycle configuration from bounded JSON stdin"},
      ["event", "add"] =>
        {2..2,
         Map.merge(event, Map.put(@write_flags, "ignore-existing", {:ignore_existing, :boolean})),
         "Append a portable notification destination"},
      ["event", "list"] =>
        {1..2, %{}, "List notification configuration, optionally filtered by ARN"},
      ["event", "remove"] =>
        {1..2, Map.merge(event, Map.put(@write_flags, "force", {:force, :boolean})),
         "Remove selected notifications or the full document"},
      ["tag", "list"] =>
        {1..1, @tag_selection_flags, "List bucket or selected object-version tags"}
    }
  end

  defp execute(["ilm", action], args, options, target, call)
       when action in ~w(add edit ls rm export import),
       do: execute(["ilm", "rule", action], args, options, target, call)

  defp execute(["encrypt", "clear"], args, options, target, call),
    do: execute(["encrypt", "remove"], args, options, target, call)

  defp execute(["encrypt", "info"], args, options, target, call),
    do: execute(["encrypt", "get"], args, options, target, call)

  defp execute(["ilm", "rule", action], _args, options, target, call) do
    action =
      case action do
        "ls" -> "list"
        "rm" -> "remove"
        action -> action
      end

    with :ok <- bucket(target),
         {:ok, options} <- lifecycle_normalize(options),
         do: lifecycle(action, target, options, call)
  end

  defp execute(["replicate", action], _args, options, target, call)
       when action in ~w(add update edit list ls import export remove rm),
       do: ExStorageServiceCli.CLI.NativeReplication.run(action, target, options, call)

  defp execute(["tag", "list"], args, options, target, call),
    do: execute(["tag", "get"], args, options, target, call)

  defp execute(["event", "add"], [_target, arn], options, target, call) do
    with :ok <- bucket(target),
         {:ok, collection, arn_field} <- notification_arn(arn),
         {:ok, events} <- notification_events(options[:event]),
         entry = %{"Id" => new_id(), arn_field => arn, "Events" => events},
         entry = notification_filter(entry, options),
         {:ok, document} <-
           call.("GetBucketNotificationConfiguration", %{"Bucket" => target.bucket}) do
      entries = document[collection] || []
      duplicate = Enum.any?(entries, &(Map.delete(&1, "Id") == Map.delete(entry, "Id")))

      cond do
        duplicate and options[:ignore_existing] ->
          {:ok, %{"Unchanged" => true}}

        duplicate ->
          invalid("The exact notification already exists; use --ignore-existing")

        true ->
          notification_write(
            target,
            Map.put(document, collection, entries ++ [entry]),
            options,
            call
          )
      end
    end
  end

  defp execute(["event", "list"], args, _options, target, call) do
    with :ok <- bucket(target),
         :ok <- optional_notification_arn(Enum.at(args, 1)),
         {:ok, document} <-
           call.("GetBucketNotificationConfiguration", %{"Bucket" => target.bucket}) do
      case Enum.at(args, 1) do
        nil ->
          {:ok, document}

        arn ->
          {:ok,
           Map.new(document, fn {field, entries} ->
             if field in notification_collections(),
               do: {field, Enum.filter(entries, &notification_matches?(&1, arn))},
               else: {field, entries}
           end)}
      end
    end
  end

  defp execute(["event", "remove"], [_target, arn], options, target, call) do
    with :ok <- bucket(target),
         :ok <-
           if(options[:force],
             do: invalid("--force selects all notifications; omit the ARN"),
             else: :ok
           ),
         {:ok, collection, _field} <- notification_arn(arn),
         {:ok, events} <- optional_notification_events(options[:event]),
         {:ok, document} <-
           call.("GetBucketNotificationConfiguration", %{"Bucket" => target.bucket}) do
      {removed, kept} =
        Enum.split_with(document[collection] || [], fn entry ->
          notification_matches?(entry, arn) and notification_selected?(entry, events, options)
        end)

      if removed == [],
        do: invalid("No notification matches the selected ARN/events/filter"),
        else: notification_write(target, Map.put(document, collection, kept), options, call)
    end
  end

  defp execute(["event", "remove"], [_target], options, target, call) do
    if Enum.any?([:event, :prefix, :suffix], &Map.has_key?(options, &1)),
      do: invalid("Notification filters require a selected destination ARN"),
      else: with(:ok <- bucket(target), do: notification_write(target, %{}, options, call))
  end

  defp execute(["version", action], _args, options, target, call)
       when action in ["enable", "suspend", "info"] do
    with :ok <- bucket(target) do
      if action == "info" do
        call.("GetBucketVersioning", base(target, :bucket, options))
      else
        input =
          Map.put(base(target, :bucket, options), "VersioningConfiguration", %{
            "Status" => if(action == "enable", do: "Enabled", else: "Suspended")
          })

        effect("PutBucketVersioning", input, options, call)
      end
    end
  end

  defp execute(["version", "list"], _args, options, target, call) do
    limit = options[:limit] || 1000

    if is_integer(limit) and limit in 1..10_000 do
      Paginator.collect(
        "ListObjectVersions",
        %{"Bucket" => target.bucket, "Prefix" => target.key || ""},
        call,
        max_items: limit,
        starting_token: options[:starting_token],
        page_size: min(limit, 1000)
      )
    else
      invalid("Version inspection limit must be between 1 and 10000")
    end
  end

  defp execute(["undo"], _args, options, target, call) do
    with :ok <- object(target) do
      case {options[:version_id], options[:delete_marker_id]} do
        {version, nil} when is_binary(version) and version != "" ->
          input = Map.put(base(target, :object, %{}), "VersionId", version)

          with {:ok, selected} <- call.("HeadObject", input),
               true <- selected["VersionId"] == version,
               etag when is_binary(etag) and etag != "" <- selected["ETag"] do
            effect(
              "CopyObject",
              %{
                "Bucket" => target.bucket,
                "Key" => target.key,
                "CopySource" => %{
                  "Bucket" => target.bucket,
                  "Key" => target.key,
                  "VersionId" => version
                },
                "CopySourceIfMatch" => etag,
                "MetadataDirective" => "COPY",
                "TaggingDirective" => "COPY"
              },
              options,
              call
            )
          else
            {:error, _} = error ->
              error

            false ->
              {:error,
               Error.new(:syntax, "Provider did not confirm the selected object version",
                 code: :source_version_mismatch
               )}

            _ ->
              invalid("Selected version has no confirmed object identity")
          end

        {nil, marker} when is_binary(marker) and marker != "" ->
          # A version ID is immutable, but first prove it is a delete marker for this key.
          with {:ok, true} <- delete_marker(target, marker, call) do
            effect(
              "DeleteObject",
              Map.put(base(target, :object, %{}), "VersionId", marker),
              options,
              call
            )
          else
            {:ok, false} ->
              invalid("The selected delete marker was not found in the bounded inventory")

            error ->
              error
          end

        _ ->
          invalid("Undo requires exactly one explicit version-id or delete-marker-id")
      end
    end
  end

  defp execute(["anonymous", "get"], _args, _options, target, call),
    do: with(:ok <- bucket(target), do: call.("GetBucketPolicy", %{"Bucket" => target.bucket}))

  defp execute(["anonymous", "set"], [mode, _], options, target, call) do
    with :ok <- bucket(target),
         {:ok, policy} <- anonymous_policy(mode, target, options[:partition]),
         :ok <- replace_policy(target, options, call) do
      effect(
        "PutBucketPolicy",
        %{"Bucket" => target.bucket, "Policy" => JSON.encode!(policy)},
        options,
        call
      )
    end
  end

  defp execute([family, action], args, options, target, call) do
    {suffix, field, scope} = Map.fetch!(@families, family)

    scope =
      if scope == :either,
        do: if(target.key in [nil, ""], do: :bucket, else: :object),
        else: scope

    suffix =
      if family in ["tag", "acl"],
        do: if(scope == :bucket, do: "Bucket", else: "Object") <> suffix,
        else: suffix

    with :ok <- validate_scope(scope, target),
         :ok <-
           if(scope == :bucket and options[:version_id],
             do: invalid("--version-id requires an object target"),
             else: :ok
           ),
         {:ok, operation, input} <-
           operation(action, family, suffix, field, scope, args, options, target),
         :ok <- policy_update(family, action, target, options, call) do
      if action == "get",
        do: call.(operation, input),
        else: effect(operation, input, options, call)
    end
  end

  defp operation("get", _family, suffix, _field, scope, _args, options, target),
    do: {:ok, "Get" <> suffix, base(target, scope, options)}

  defp operation("set", family, suffix, field, scope, [_target, document], options, target) do
    with {:ok, data} <- family_document(document, family, scope) do
      {:ok, "Put" <> suffix, Map.put(base(target, scope, options), field, data)}
    end
  end

  defp operation("remove", family, suffix, _field, scope, _args, options, target) do
    case family do
      "ilm" ->
        {:ok, "DeleteBucketLifecycle", base(target, scope, options)}

      "event" ->
        {:ok, "PutBucketNotificationConfiguration",
         Map.put(base(target, scope, options), "NotificationConfiguration", %{})}

      "legalhold" ->
        {:ok, "PutObjectLegalHold",
         Map.put(base(target, scope, options), "LegalHold", %{"Status" => "OFF"})}

      family when family in ["retention", "lock", "acl"] ->
        invalid("This family has no portable remove operation; set a complete permitted document")

      _ ->
        {:ok, "Delete" <> suffix, base(target, scope, options)}
    end
  end

  defp lifecycle("import", target, options, call) do
    with bytes when is_binary(bytes) <- IO.binread(options[:stdin], 8_388_609),
         {:ok, document} <- document(bytes, false),
         true <- is_list(document["Rules"]) and document["Rules"] != [] do
      lifecycle_write(target, document, options, call)
    else
      {:error, _} = error -> error
      _ -> invalid("Lifecycle import requires a nonempty Rules JSON object within 8 MiB")
    end
  end

  defp lifecycle("list", _target, %{expiry: true, transition: true}, _call),
    do: invalid("Choose only one of --expiry or --transition")

  defp lifecycle(action, target, options, call) when action in ["list", "export"] do
    with {:ok, document} <- call.("GetBucketLifecycleConfiguration", %{"Bucket" => target.bucket}) do
      fields =
        if(options[:expiry], do: ["Expiration", "NoncurrentVersionExpiration"], else: []) ++
          if(options[:transition], do: ["Transitions", "NoncurrentVersionTransitions"], else: [])

      if action == "list" and fields != [],
        do:
          {:ok,
           Map.put(
             document,
             "Rules",
             Enum.map(
               document["Rules"] || [],
               &Map.take(&1, ["ID", "Status", "Prefix", "Filter" | fields])
             )
           )},
        else: {:ok, document}
    end
  end

  defp lifecycle("add", target, options, call) do
    with {:ok, rule} <-
           lifecycle_rule(
             %{"ID" => new_id(), "Status" => "Enabled", "Filter" => %{"Prefix" => ""}},
             options
           ),
         {:ok, document} <- lifecycle_read(target, call, true),
         true <- length(document["Rules"] || []) < 1000 do
      lifecycle_write(
        target,
        Map.update(document, "Rules", [rule], &(&1 ++ [rule])),
        options,
        call
      )
    else
      false -> invalid("A lifecycle configuration may contain at most 1000 rules")
      error -> error
    end
  end

  defp lifecycle("edit", target, options, call) do
    with :ok <- selected_id(options),
         :ok <- lifecycle_options(options),
         {:ok, document} <- lifecycle_read(target, call, false),
         {:ok, selected} <- lifecycle_selected(document, options[:id]),
         {:ok, updated} <- lifecycle_rule(selected, options) do
      rules = Enum.map(document["Rules"], &if(&1["ID"] == options[:id], do: updated, else: &1))
      lifecycle_write(target, Map.put(document, "Rules", rules), options, call)
    end
  end

  defp lifecycle("remove", target, %{all: true, force: true} = options, call) do
    if options[:id],
      do: invalid("Select either --id or --all --force"),
      else: validated_effect("DeleteBucketLifecycle", %{"Bucket" => target.bucket}, options, call)
  end

  defp lifecycle("remove", target, options, call) do
    with :ok <- selected_id(options),
         false <- options[:all] == true or options[:force] == true,
         {:ok, document} <- lifecycle_read(target, call, false),
         {:ok, _} <- lifecycle_selected(document, options[:id]) do
      case Enum.reject(document["Rules"], &(&1["ID"] == options[:id])) do
        [] ->
          validated_effect("DeleteBucketLifecycle", %{"Bucket" => target.bucket}, options, call)

        rules ->
          lifecycle_write(target, Map.put(document, "Rules", rules), options, call)
      end
    else
      true -> invalid("Select either --id or --all --force")
      error -> error
    end
  end

  defp lifecycle_read(target, call, allow_missing) do
    case call.("GetBucketLifecycleConfiguration", %{"Bucket" => target.bucket}) do
      {:error, %Error{code: "NoSuchLifecycleConfiguration"}} when allow_missing ->
        {:ok, %{"Rules" => []}}

      other ->
        other
    end
  end

  defp lifecycle_selected(document, id) do
    case Enum.filter(document["Rules"] || [], &(&1["ID"] == id)) do
      [rule] -> {:ok, rule}
      _ -> invalid("Lifecycle rule ID must identify exactly one existing rule")
    end
  end

  defp selected_id(%{id: id}) when is_binary(id) and id != "", do: :ok
  defp selected_id(_), do: invalid("A nonempty --id is required")

  defp lifecycle_write(target, document, options, call) do
    input = %{
      "Bucket" => target.bucket,
      "LifecycleConfiguration" => Map.delete(document, "TransitionDefaultMinimumObjectSize")
    }

    input =
      if Map.has_key?(document, "TransitionDefaultMinimumObjectSize"),
        do:
          Map.put(
            input,
            "TransitionDefaultMinimumObjectSize",
            document["TransitionDefaultMinimumObjectSize"]
          ),
        else: input

    validated_effect("PutBucketLifecycleConfiguration", input, options, call)
  end

  defp lifecycle_rule(rule, options) do
    with :ok <- lifecycle_options(options),
         {:ok, filter} <- lifecycle_filter(rule, options),
         {:ok, rule} <- lifecycle_actions(rule, options) do
      rule = if filter, do: rule |> Map.delete("Prefix") |> Map.put("Filter", filter), else: rule

      rule =
        cond do
          Map.has_key?(options, :disable) ->
            Map.put(rule, "Status", if(options[:disable], do: "Disabled", else: "Enabled"))

          Map.has_key?(options, :enable) ->
            Map.put(rule, "Status", if(options[:enable], do: "Enabled", else: "Disabled"))

          true ->
            rule
        end

      if Enum.any?(
           [
             "Expiration",
             "Transitions",
             "NoncurrentVersionExpiration",
             "NoncurrentVersionTransitions",
             "AbortIncompleteMultipartUpload"
           ],
           &Map.has_key?(rule, &1)
         ),
         do: {:ok, rule},
         else: invalid("A lifecycle rule requires at least one action")
    end
  end

  defp lifecycle_normalize(options) do
    with {:ok, options} <- lifecycle_tiers(options),
         {:ok, options} <- lifecycle_dates(options) do
      {:ok, options}
    end
  end

  defp lifecycle_tiers(options) do
    keys = [:storage_class, :transition_tier, :noncurrent_transition_tier]

    if Enum.all?(keys, &(not Map.has_key?(options, &1) or is_binary(options[&1]))) do
      options =
        Enum.reduce(keys, options, fn key, acc ->
          if Map.has_key?(acc, key), do: Map.update!(acc, key, &String.upcase/1), else: acc
        end)

      cond do
        Map.has_key?(options, :storage_class) and Map.has_key?(options, :transition_tier) and
            options[:storage_class] != options[:transition_tier] ->
          invalid("Conflicting --storage-class and --transition-tier values")

        Map.has_key?(options, :storage_class) ->
          {:ok,
           options
           |> Map.put(:transition_tier, options[:storage_class])
           |> Map.delete(:storage_class)}

        true ->
          {:ok, options}
      end
    else
      invalid("Lifecycle storage classes must be strings")
    end
  end

  defp lifecycle_dates(options) do
    Enum.reduce_while([:expiry_date, :transition_date], {:ok, options}, fn key, {:ok, acc} ->
      if Map.has_key?(acc, key) do
        value = acc[key]

        with true <- is_binary(value) and Regex.match?(~r/^\d{4}-\d{2}-\d{2}$/, value),
             {:ok, date} <- Date.from_iso8601(value),
             true <- date.year >= 1 and value != "0001-01-01" do
          {:cont, {:ok, Map.put(acc, key, value <> "T00:00:00Z")}}
        else
          _ ->
            {:halt,
             invalid("Lifecycle dates must be valid YYYY-MM-DD calendar dates after 0001-01-01")}
        end
      else
        {:cont, {:ok, acc}}
      end
    end)
  end

  defp lifecycle_options(options) do
    positive = [
      :expire_days,
      :noncurrent_expire_days,
      :noncurrent_expire_newer,
      :noncurrent_transition_newer
    ]

    nonnegative = [:transition_days, :noncurrent_transition_days]
    classes = ~w(STANDARD_IA ONEZONE_IA INTELLIGENT_TIERING GLACIER DEEP_ARCHIVE GLACIER_IR)

    cond do
      Map.has_key?(options, :enable) and Map.has_key?(options, :disable) ->
        invalid("Choose only --enable or --disable")

      Enum.count([:expire_days, :expiry_date, :expire_delete_marker], &Map.has_key?(options, &1)) >
          1 ->
        invalid("Choose only one expiration action")

      Map.has_key?(options, :transition_days) and Map.has_key?(options, :transition_date) ->
        invalid("Choose only --transition-days or --transition-date")

      Enum.any?(
        positive,
        &(Map.has_key?(options, &1) and (not is_integer(options[&1]) or options[&1] <= 0))
      ) ->
        invalid("Expiration days and retained-version counts must be positive integers")

      Enum.any?(
        [:noncurrent_expire_newer, :noncurrent_transition_newer],
        &(Map.has_key?(options, &1) and options[&1] > 100)
      ) ->
        invalid("Retained noncurrent-version counts must be between 1 and 100")

      Enum.any?(
        nonnegative,
        &(Map.has_key?(options, &1) and (not is_integer(options[&1]) or options[&1] < 0))
      ) ->
        invalid("Transition days must be nonnegative integers")

      Enum.any?(
        [:transition_tier, :noncurrent_transition_tier],
        &(Map.has_key?(options, &1) and options[&1] not in classes)
      ) ->
        invalid(
          "Portable transition tiers must be modeled AWS storage classes; MinIO remote tiers are unsupported"
        )

      true ->
        :ok
    end
  end

  defp lifecycle_actions(rule, options) do
    expiration =
      Map.drop(rule["Expiration"] || %{}, ["Days", "Date", "ExpiredObjectDeleteMarker"])

    rule =
      cond do
        Map.has_key?(options, :expire_days) ->
          Map.put(rule, "Expiration", Map.put(expiration, "Days", options[:expire_days]))

        Map.has_key?(options, :expiry_date) ->
          Map.put(rule, "Expiration", Map.put(expiration, "Date", options[:expiry_date]))

        Map.has_key?(options, :expire_delete_marker) ->
          Map.put(
            rule,
            "Expiration",
            Map.put(expiration, "ExpiredObjectDeleteMarker", options[:expire_delete_marker])
          )

        true ->
          rule
      end

    rule =
      if Map.has_key?(options, :noncurrent_expire_days),
        do:
          Map.put(
            rule,
            "NoncurrentVersionExpiration",
            Map.merge(rule["NoncurrentVersionExpiration"] || %{}, %{
              "NoncurrentDays" => options[:noncurrent_expire_days]
            })
          ),
        else: rule

    rule =
      if Map.has_key?(options, :noncurrent_expire_newer),
        do:
          Map.update(
            rule,
            "NoncurrentVersionExpiration",
            %{"NewerNoncurrentVersions" => options[:noncurrent_expire_newer]},
            &Map.put(&1, "NewerNoncurrentVersions", options[:noncurrent_expire_newer])
          ),
        else: rule

    with true <-
           not Map.has_key?(options, :noncurrent_expire_newer) or
             Map.has_key?(rule["NoncurrentVersionExpiration"] || %{}, "NoncurrentDays"),
         {:ok, rule} <-
           lifecycle_transition(
             rule,
             options,
             "Transitions",
             :transition_days,
             :transition_tier,
             "Days",
             :transition_date,
             nil
           ),
         {:ok, rule} <-
           lifecycle_transition(
             rule,
             options,
             "NoncurrentVersionTransitions",
             :noncurrent_transition_days,
             :noncurrent_transition_tier,
             "NoncurrentDays",
             nil,
             :noncurrent_transition_newer
           ) do
      {:ok, rule}
    else
      false ->
        invalid(
          "--noncurrent-expire-newer requires existing or explicit noncurrent expiration days"
        )

      error ->
        error
    end
  end

  defp lifecycle_transition(
         rule,
         options,
         field,
         days_key,
         tier_key,
         days_field,
         date_key,
         newer_key
       ) do
    if Enum.any?([days_key, tier_key, date_key, newer_key], &Map.has_key?(options, &1)) do
      case rule[field] || [%{}] do
        [transition] ->
          transition =
            if Map.has_key?(options, days_key),
              do: transition |> Map.delete("Date") |> Map.put(days_field, options[days_key]),
              else: transition

          transition =
            if date_key && Map.has_key?(options, date_key),
              do: transition |> Map.delete(days_field) |> Map.put("Date", options[date_key]),
              else: transition

          transition =
            if Map.has_key?(options, tier_key),
              do: Map.put(transition, "StorageClass", options[tier_key]),
              else: transition

          transition =
            if newer_key && Map.has_key?(options, newer_key),
              do: Map.put(transition, "NewerNoncurrentVersions", options[newer_key]),
              else: transition

          timing =
            Map.has_key?(transition, days_field) or
              (date_key != nil and Map.has_key?(transition, "Date"))

          if timing and transition["StorageClass"],
            do: {:ok, Map.put(rule, field, [transition])},
            else: invalid("Transition date or days and storage class must both be specified")

        _ ->
          invalid(
            "Multiple transitions require a complete document edit; no transition is selected implicitly"
          )
      end
    else
      {:ok, rule}
    end
  end

  defp lifecycle_filter(rule, options) do
    if Enum.any?([:prefix, :tags, :size_lt, :size_gt], &Map.has_key?(options, &1)) do
      current = rule["Filter"] || %{"Prefix" => rule["Prefix"] || ""}
      predicates = Map.merge(Map.delete(current, "And"), current["And"] || %{})
      tags = predicates["Tags"] || if(predicates["Tag"], do: [predicates["Tag"]], else: [])

      with {:ok, tags} <-
             if(Map.has_key?(options, :tags),
               do: parse_tags(options[:tags], false, 50),
               else: {:ok, tags}
             ),
           {:ok, less} <- optional_size(options, :size_lt, predicates["ObjectSizeLessThan"]),
           {:ok, greater} <-
             optional_size(options, :size_gt, predicates["ObjectSizeGreaterThan"]),
           true <- less == nil or greater == nil or greater < less do
        predicates =
          predicates |> Map.drop(["Tag", "Tags", "ObjectSizeLessThan", "ObjectSizeGreaterThan"])

        predicates =
          if Map.has_key?(options, :prefix),
            do: Map.put(predicates, "Prefix", options[:prefix]),
            else: predicates

        predicates =
          if less != nil, do: Map.put(predicates, "ObjectSizeLessThan", less), else: predicates

        predicates =
          if greater != nil,
            do: Map.put(predicates, "ObjectSizeGreaterThan", greater),
            else: predicates

        count = map_size(predicates) + length(tags)

        filter =
          cond do
            count > 1 ->
              %{"And" => if(tags == [], do: predicates, else: Map.put(predicates, "Tags", tags))}

            tags != [] ->
              %{"Tag" => hd(tags)}

            true ->
              predicates
          end

        {:ok, filter}
      else
        false -> invalid("Lifecycle size-gt must be less than size-lt")
        error -> error
      end
    else
      {:ok, nil}
    end
  end

  defp optional_size(options, key, previous) do
    if Map.has_key?(options, key) do
      case Regex.run(~r/\A([0-9]+)(?:\s*)(B|kB|KB|MB|GB|TB|KiB|MiB|GiB|TiB)?\z/, options[key]) do
        [_, digits | unit] ->
          factor = %{
            "" => 1,
            "B" => 1,
            "kB" => 1000,
            "KB" => 1000,
            "MB" => 1_000_000,
            "GB" => 1_000_000_000,
            "TB" => 1_000_000_000_000,
            "KiB" => 1024,
            "MiB" => 1_048_576,
            "GiB" => 1_073_741_824,
            "TiB" => 1_099_511_627_776
          }

          value = String.to_integer(digits) * factor[List.first(unit) || ""]

          if value <= 9_223_372_036_854_775_807,
            do: {:ok, value},
            else: invalid("Lifecycle size exceeds signed 64-bit bytes")

        _ ->
          invalid("Lifecycle sizes require integer bytes or B/KB/MB/GB/TB/KiB/MiB/GiB/TiB units")
      end
    else
      {:ok, previous}
    end
  end

  @doc false
  def object_tags(value), do: family_document(value, "tag", :object)

  defp family_document(value, "tag", scope) do
    if String.starts_with?(String.trim_leading(value), ["{", "file://"]),
      do: document(value, false),
      else:
        with(
          {:ok, tags} <- parse_tags(value, true, if(scope == :object, do: 10, else: 50)),
          do: {:ok, %{"TagSet" => tags}}
        )
  end

  defp family_document(value, family, _scope), do: document(value, family == "policy")

  defp parse_tags(value, encoded, maximum) do
    pairs = if value == "", do: [], else: String.split(value, "&")

    if byte_size(value) > 32_768 or length(pairs) > maximum or
         (encoded and Regex.match?(~r/%(?![a-fA-F0-9]{2})/, value)) do
      invalid("Tag input exceeds its bound or contains invalid percent encoding")
    else
      result =
        Enum.reduce_while(pairs, {:ok, [], MapSet.new()}, fn pair, {:ok, tags, keys} ->
          [key | rest] = String.split(pair, "=", parts: 2)
          val = List.first(rest) || ""
          key = if encoded, do: URI.decode_www_form(key), else: key
          val = if encoded, do: URI.decode_www_form(val), else: val

          if key == "" or not String.valid?(key) or not String.valid?(val) or
               String.length(key) > 128 or String.length(val) > 256 or MapSet.member?(keys, key),
             do:
               {:halt,
                invalid(
                  "Tags require distinct nonempty keys within 128 characters and values within 256 characters"
                )},
             else: {:cont, {:ok, [%{"Key" => key, "Value" => val} | tags], MapSet.put(keys, key)}}
        end)

      case result do
        {:ok, tags, _} -> {:ok, Enum.reverse(tags)}
        error -> error
      end
    end
  end

  defp notification_collections,
    do: ["TopicConfigurations", "QueueConfigurations", "LambdaFunctionConfigurations"]

  defp optional_notification_arn(nil), do: :ok
  defp optional_notification_arn(arn), do: with({:ok, _, _} <- notification_arn(arn), do: :ok)

  defp notification_arn(arn) do
    case String.split(arn, ":", parts: 6) do
      ["arn", partition, service, region, account, resource]
      when partition in ["aws", "aws-cn", "aws-us-gov"] and region != "" and account != "" and
             resource != "" ->
        case service do
          "sns" -> {:ok, "TopicConfigurations", "TopicArn"}
          "sqs" -> {:ok, "QueueConfigurations", "QueueArn"}
          "lambda" -> {:ok, "LambdaFunctionConfigurations", "LambdaFunctionArn"}
          _ -> invalid("Notifications require a portable SNS, SQS or Lambda ARN")
        end

      _ ->
        invalid("Notifications require a complete portable destination ARN")
    end
  end

  defp optional_notification_events(nil), do: {:ok, nil}
  defp optional_notification_events(value), do: notification_events(value)

  defp notification_events(value) when is_binary(value) do
    values = String.split(value, ",")

    mapping = %{
      "put" => "s3:ObjectCreated:*",
      "delete" => "s3:ObjectRemoved:*",
      "replica" => "s3:Replication:*"
    }

    if values != [] and Enum.all?(values, &Map.has_key?(mapping, &1)),
      do: {:ok, Enum.uniq(Enum.map(values, &mapping[&1]))},
      else:
        invalid(
          "Portable notification events are put,delete,replica; get,ilm and scanner require unsupported MinIO events"
        )
  end

  defp notification_events(_),
    do:
      invalid(
        "Specify --event explicitly: the mc put,delete,get default includes unsupported MinIO access events"
      )

  defp notification_filter(entry, options) do
    rules =
      for {key, name} <- [prefix: "prefix", suffix: "suffix"],
          options[key] not in [nil, ""],
          do: %{"Name" => name, "Value" => options[key]}

    if rules == [],
      do: entry,
      else: Map.put(entry, "Filter", %{"Key" => %{"FilterRules" => rules}})
  end

  defp notification_matches?(entry, arn),
    do: Enum.any?(["TopicArn", "QueueArn", "LambdaFunctionArn"], &(entry[&1] == arn))

  defp notification_selected?(entry, events, options) do
    rules = get_in(entry, ["Filter", "Key", "FilterRules"]) || []

    (events == nil or Enum.sort(entry["Events"] || []) == Enum.sort(events)) and
      Enum.all?([prefix: "prefix", suffix: "suffix"], fn {key, name} ->
        not Map.has_key?(options, key) or
          Enum.find_value(rules, "", &if(&1["Name"] == name, do: &1["Value"])) == options[key]
      end)
  end

  defp notification_write(target, document, options, call),
    do:
      validated_effect(
        "PutBucketNotificationConfiguration",
        %{"Bucket" => target.bucket, "NotificationConfiguration" => document},
        options,
        call
      )

  defp validated_effect(operation, input, options, call) do
    with {:ok, input} <- Input.validate(operation, input),
         do: effect(operation, input, options, call)
  end

  defp new_id, do: "ess-" <> Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)

  defp base(target, scope, options) do
    input = %{"Bucket" => target.bucket}
    input = if scope == :object, do: Map.put(input, "Key", target.key), else: input

    input =
      if scope == :object and options[:version_id],
        do: Map.put(input, "VersionId", options[:version_id]),
        else: input

    if options[:bypass_governance_retention],
      do: Map.put(input, "BypassGovernanceRetention", true),
      else: input
  end

  defp effect(operation, input, %{dry_run: true}, _call) do
    with {:ok, input, _customizations} <- Customizations.input(operation, input),
         {:ok, input} <- Input.validate(operation, input),
         do: {:ok, %{"DryRun" => true, "Operation" => operation, "Input" => input}}
  end

  defp effect(operation, input, _options, call), do: call.(operation, input)

  defp policy_update("policy", "set", target, options, call),
    do: replace_policy(target, options, call)

  defp policy_update(_, _, _, _, _), do: :ok
  defp replace_policy(_target, %{replace: true}, _call), do: :ok

  defp replace_policy(target, _options, call) do
    case call.("GetBucketPolicy", %{"Bucket" => target.bucket}) do
      {:ok, _} ->
        invalid(
          "A bucket policy already exists; use --replace to deliberately replace its full document"
        )

      {:error, %Error{code: "NoSuchBucketPolicy"}} ->
        :ok

      error ->
        error
    end
  end

  defp anonymous_policy("private", target, partition),
    do: anonymous_policy("none", target, partition)

  defp anonymous_policy(mode, target, partition) when mode in ~w(none download upload public) do
    arn = "arn:" <> partition <> ":s3:::" <> target.bucket

    read = [
      %{
        "Sid" => "ESSAnonymousRead",
        "Effect" => "Allow",
        "Principal" => "*",
        "Action" => ["s3:GetObject"],
        "Resource" => [arn <> "/*"]
      }
    ]

    list = [
      %{
        "Sid" => "ESSAnonymousList",
        "Effect" => "Allow",
        "Principal" => "*",
        "Action" => ["s3:ListBucket"],
        "Resource" => [arn]
      }
    ]

    write = [
      %{
        "Sid" => "ESSAnonymousWrite",
        "Effect" => "Allow",
        "Principal" => "*",
        "Action" => ["s3:PutObject"],
        "Resource" => [arn <> "/*"]
      }
    ]

    statements =
      case mode do
        "none" -> []
        "download" -> read ++ list
        "upload" -> write
        "public" -> read ++ list ++ write
      end

    {:ok, %{"Version" => "2012-10-17", "Statement" => statements}}
  end

  defp anonymous_policy(_, _, _),
    do: invalid("Anonymous access mode must be none, private, download, upload or public")

  defp delete_marker(target, marker, call) do
    Paginator.pages(
      "ListObjectVersions",
      %{"Bucket" => target.bucket, "Prefix" => target.key},
      call,
      page_size: 1000,
      max_pages: 100
    )
    |> Enum.reduce_while({:ok, false}, fn
      {:ok, page}, _ ->
        if Enum.any?(
             page["DeleteMarkers"] || [],
             &(&1["Key"] == target.key and &1["VersionId"] == marker)
           ),
           do: {:halt, {:ok, true}},
           else: {:cont, {:ok, false}}

      error, _ ->
        {:halt, error}
    end)
  end

  defp document(value, raw) do
    with {:ok, bytes} <- document_bytes(value),
         {:ok, decoded} when is_map(decoded) <- JSON.decode(bytes) do
      {:ok, if(raw, do: bytes, else: decoded)}
    else
      _ ->
        invalid(
          "Configuration document must be a JSON object within 8 MiB; use file:// for a file"
        )
    end
  end

  defp document_bytes("file://" <> path) do
    case File.open(path, [:read, :binary], &IO.binread(&1, 8_388_609)) do
      {:ok, bytes} when is_binary(bytes) and byte_size(bytes) <= 8_388_608 -> {:ok, bytes}
      _ -> :error
    end
  end

  defp document_bytes(bytes) when is_binary(bytes) and byte_size(bytes) <= 8_388_608,
    do: {:ok, bytes}

  defp document_bytes(_), do: :error
  defp validate_scope(:bucket, target), do: bucket(target)
  defp validate_scope(:object, target), do: object(target)

  defp bucket(%Target{bucket: bucket, key: key}) when is_binary(bucket) and key in [nil, ""],
    do: :ok

  defp bucket(_), do: invalid("This operation requires a bucket target without an object key")
  defp object(%Target{key: key}) when is_binary(key) and key != "", do: :ok
  defp object(_), do: invalid("This operation requires an object key")
  defp invalid(message), do: {:error, Error.new(:syntax, message)}
end
