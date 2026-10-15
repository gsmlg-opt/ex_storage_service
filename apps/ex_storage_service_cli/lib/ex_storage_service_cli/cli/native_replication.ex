defmodule ExStorageServiceCli.CLI.NativeReplication do
  @moduledoc "Portable replication rule edits over complete modeled S3 configuration documents."
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.S3.Input
  @write %{"dry-run" => {:dry_run, :boolean}}
  @fields %{
    "id" => {:id, :string},
    "tags" => {:tags, :string},
    "priority" => {:priority, :integer},
    "remote-bucket" => {:remote_bucket, :string},
    "storage-class" => {:storage_class, :string},
    "replicate" => {:replicate, :string}
  }

  def commands do
    add =
      Map.merge(@fields, %{
        "role" => {:role, :string},
        "arn" => {:arn, :string},
        "disable" => {:disable, :boolean}
      })

    update = Map.put(@fields, "state", {:state, :string})
    remove = %{"id" => {:id, :string}, "all" => {:all, :boolean}, "force" => {:force, :boolean}}

    %{
      ["replicate", "add"] =>
        {1..1, Map.merge(add, @write),
         "Append a portable S3 replication rule; first configuration requires --role"},
      ["replicate", "update"] =>
        {1..1, Map.merge(update, @write), "Update selected replication rule fields"},
      ["replicate", "edit"] =>
        {1..1, Map.merge(update, @write), "Update selected replication rule fields"},
      ["replicate", "list"] =>
        {1..1, %{"status" => {:status, :string}}, "List complete portable replication rules"},
      ["replicate", "ls"] =>
        {1..1, %{"status" => {:status, :string}}, "List complete portable replication rules"},
      ["replicate", "export"] => {1..1, %{}, "Export the complete S3 replication configuration"},
      ["replicate", "import"] =>
        {1..1, @write, "Replace replication configuration from bounded JSON stdin"},
      ["replicate", "remove"] =>
        {1..1, Map.merge(remove, @write),
         "Remove an explicit rule ID or all rules with --all --force"},
      ["replicate", "rm"] =>
        {1..1, Map.merge(remove, @write),
         "Remove an explicit rule ID or all rules with --all --force"}
    }
  end

  def run("edit", target, options, call), do: run("update", target, options, call)
  def run("ls", target, options, call), do: run("list", target, options, call)
  def run("rm", target, options, call), do: run("remove", target, options, call)

  def run(action, %Target{kind: :remote, bucket: bucket} = target, options, call)
      when is_binary(bucket) and bucket != "" do
    with :ok <- scope(action, target),
         :ok <- options(action, options, target.key not in [nil, ""]) do
      execute(action, target, options, call)
    end
  end

  def run(_action, _target, _options, _call),
    do: invalid("Replication requires a remote bucket target")

  defp execute("add", target, options, call) do
    with {:ok, rule} <-
           build_rule(
             %{
               "ID" => options[:id] || new_id(),
               "Status" => "Enabled",
               "Destination" => %{},
               "Filter" => %{"Prefix" => ""}
             },
             target,
             options
           ),
         {:ok, document} <- read(target, call, true),
         {:ok, role} <- role(document, options),
         document =
           document |> Map.put("Role", role) |> Map.update("Rules", [rule], &(&1 ++ [rule])) do
      write(target, document, options, call)
    end
  end

  defp execute("update", target, options, call) do
    with {:ok, document} <- read(target, call),
         {:ok, selected} <- selected(document, options[:id]),
         {:ok, updated} <- build_rule(selected, target, options) do
      rules = Enum.map(document["Rules"], &if(&1["ID"] == options[:id], do: updated, else: &1))
      write(target, Map.put(document, "Rules", rules), options, call)
    end
  end

  defp execute(action, target, options, call) when action in ["list", "export"] do
    with {:ok, document} <- read(target, call) do
      if action == "list" and options[:status] not in [nil, ""],
        do:
          {:ok,
           Map.update!(
             document,
             "Rules",
             &Enum.filter(&1, fn rule ->
               String.downcase(rule["Status"]) == String.downcase(options[:status])
             end)
           )},
        else: {:ok, document}
    end
  end

  defp execute("import", target, options, call) do
    with bytes when is_binary(bytes) and byte_size(bytes) <= 2_097_152 <-
           IO.binread(options[:stdin] || :stdio, 2_097_153),
         {:ok, document} when is_map(document) <- JSON.decode(bytes) do
      write(target, document, options, call)
    else
      _ -> invalid("Replication import requires a complete JSON object within 2 MiB")
    end
  end

  defp execute("remove", target, %{all: true, force: true} = options, call),
    do: effect("DeleteBucketReplication", %{"Bucket" => target.bucket}, options, call)

  defp execute("remove", target, options, call) do
    with {:ok, document} <- read(target, call),
         {:ok, _selected} <- selected(document, options[:id]) do
      case Enum.reject(document["Rules"], &(&1["ID"] == options[:id])) do
        [] -> effect("DeleteBucketReplication", %{"Bucket" => target.bucket}, options, call)
        remaining -> write(target, Map.put(document, "Rules", remaining), options, call)
      end
    end
  end

  defp read(target, call, allow_absent \\ false) do
    case call.("GetBucketReplication", %{"Bucket" => target.bucket}) do
      {:ok, %{"ReplicationConfiguration" => document}} ->
        with :ok <- validate(target, document), do: {:ok, document}

      {:error, %Error{code: code}}
      when allow_absent and
             code in ["ReplicationConfigurationNotFoundError", "NoSuchReplicationConfiguration"] ->
        {:ok, %{"Rules" => []}}

      {:ok, _} ->
        invalid("The provider returned no complete replication configuration")

      error ->
        error
    end
  end

  defp validate(target, document) do
    with {:ok, _} <-
           Input.validate("PutBucketReplication", %{
             "Bucket" => target.bucket,
             "ReplicationConfiguration" => document
           }),
         true <- document["Rules"] != [] and length(document["Rules"]) <= 1000,
         true <- unique?(document["Rules"], "ID") and unique?(document["Rules"], "Priority") do
      :ok
    else
      false -> invalid("Replication requires 1..1000 rules with unambiguous IDs and priorities")
      error -> error
    end
  end

  defp unique?(rules, field) do
    values = rules |> Enum.map(& &1[field]) |> Enum.reject(&is_nil/1)
    length(values) == length(Enum.uniq(values))
  end

  defp selected(document, id) do
    case Enum.filter(document["Rules"], &(&1["ID"] == id)) do
      [rule] -> {:ok, rule}
      _ -> invalid("Exactly one replication rule must match --id")
    end
  end

  defp role(document, options) do
    case {document["Role"], options[:role]} do
      {nil, nil} -> invalid("The first replication rule requires an explicit --role IAM role ARN")
      {nil, role} -> {:ok, role}
      {role, nil} -> {:ok, role}
      {role, role} -> {:ok, role}
      _ -> invalid("Changing the shared Role requires a deliberate complete configuration import")
    end
  end

  defp build_rule(rule, target, options) do
    rule =
      Enum.reduce([priority: "Priority", state: "Status"], rule, fn {key, field}, acc ->
        if Map.has_key?(options, key),
          do: Map.put(acc, field, if(key == :state, do: state(options[key]), else: options[key])),
          else: acc
      end)

    rule = if options[:disable], do: Map.put(rule, "Status", "Disabled"), else: rule
    destination = rule["Destination"]

    destination =
      if Map.has_key?(options, :remote_bucket),
        do: Map.put(destination, "Bucket", options[:remote_bucket]),
        else: destination

    destination =
      if Map.has_key?(options, :storage_class),
        do: Map.put(destination, "StorageClass", options[:storage_class]),
        else: destination

    rule = Map.put(rule, "Destination", destination)

    with {:ok, rule} <- filter(rule, target, options),
         {:ok, rule} <- replication(rule, options) do
      if get_in(rule, ["DeleteMarkerReplication", "Status"]) == "Enabled" and tags(rule) != [],
        do: invalid("Tag-filtered S3 rules cannot enable delete-marker replication"),
        else: {:ok, rule}
    end
  end

  defp filter(rule, target, options) do
    if target.key not in [nil, ""] or Map.has_key?(options, :tags) or
         Map.has_key?(options, :replicate) do
      previous = rule["Filter"] || %{"Prefix" => rule["Prefix"] || ""}

      prefix =
        if target.key in [nil, ""],
          do: get_in(previous, ["And", "Prefix"]) || previous["Prefix"] || "",
          else: target.key

      with {:ok, tags} <-
             if(Map.has_key?(options, :tags),
               do: parse_tags(options[:tags]),
               else: {:ok, tags(rule)}
             ) do
        filter =
          case {prefix, tags} do
            {prefix, []} -> %{"Prefix" => prefix}
            {"", [tag]} -> %{"Tag" => tag}
            {"", tags} -> %{"And" => %{"Tags" => tags}}
            {prefix, tags} -> %{"And" => %{"Prefix" => prefix, "Tags" => tags}}
          end

        {:ok, rule |> Map.delete("Prefix") |> Map.put("Filter", filter)}
      end
    else
      {:ok, rule}
    end
  end

  defp tags(rule) do
    filter = rule["Filter"] || %{}
    get_in(filter, ["And", "Tags"]) || if(filter["Tag"], do: [filter["Tag"]], else: [])
  end

  defp replication(rule, options) do
    if Map.has_key?(options, :replicate) do
      with {:ok, values} <- modes(options[:replicate]) do
        criteria =
          Map.put(rule["SourceSelectionCriteria"] || %{}, "ReplicaModifications", %{
            "Status" => if("metadata-sync" in values, do: "Enabled", else: "Disabled")
          })

        {:ok,
         rule
         |> Map.put("DeleteMarkerReplication", %{
           "Status" => if("delete-marker" in values, do: "Enabled", else: "Disabled")
         })
         |> Map.put("SourceSelectionCriteria", criteria)}
      end
    else
      {:ok, rule}
    end
  end

  defp options(action, options, prefix_change) do
    cond do
      Enum.any?(
        [:arn, :bandwidth, :sync, :proxy, :disable_proxy, :healthcheck_seconds, :path],
        &Map.has_key?(options, &1)
      ) ->
        extension(
          "MinIO remote-target administration is outside the portable S3 replication contract"
        )

      Map.has_key?(options, :remote_bucket) and not bucket_arn?(options[:remote_bucket]) ->
        extension(
          "--remote-bucket requires an S3 bucket ARN; credential URLs and remote aliases require MinIO administration"
        )

      Map.has_key?(options, :role) and not role_arn?(options[:role]) ->
        invalid("--role requires an IAM role ARN")

      Map.has_key?(options, :id) and not valid_id?(options[:id]) ->
        invalid("--id must contain 1..255 characters")

      Map.has_key?(options, :priority) and
          (not is_integer(options[:priority]) or options[:priority] not in 0..2_147_483_647) ->
        invalid("--priority must be a nonnegative 32-bit integer")

      Map.has_key?(options, :storage_class) and
          options[:storage_class] not in ["STANDARD", "REDUCED_REDUNDANCY"] ->
        invalid(
          "Portable mc storage-class convenience accepts STANDARD or REDUCED_REDUNDANCY; import supports all modeled fields"
        )

      Map.has_key?(options, :state) and options[:state] not in ["enable", "disable"] ->
        invalid("--state must be enable or disable")

      action == "add" and
          not Enum.all?([:remote_bucket, :priority, :replicate], &Map.has_key?(options, &1)) ->
        invalid(
          "Add requires --remote-bucket, --priority and explicit --replicate; the mc default includes MinIO extensions"
        )

      action == "update" and not valid_id?(options[:id]) ->
        invalid("Update requires an explicit --id")

      action == "update" and not prefix_change and
          not Enum.any?(
            [:remote_bucket, :priority, :replicate, :tags, :storage_class, :state],
            &Map.has_key?(options, &1)
          ) ->
        invalid("Update requires at least one selected field")

      action == "remove" ->
        removal(options)

      action == "list" and options[:status] not in [nil, ""] and
          (not is_binary(options[:status]) or
             String.downcase(options[:status]) not in ["enabled", "disabled"]) ->
        invalid("--status must be enabled or disabled")

      true ->
        with {:ok, _} <-
               if(Map.has_key?(options, :replicate),
                 do: modes(options[:replicate]),
                 else: {:ok, []}
               ),
             {:ok, _} <-
               if(Map.has_key?(options, :tags), do: parse_tags(options[:tags]), else: {:ok, []}) do
          if is_binary(options[:replicate]) and
               String.contains?(options[:replicate], "delete-marker") and
               options[:tags] not in [nil, ""],
             do: invalid("Tag-filtered S3 rules cannot enable delete-marker replication"),
             else: :ok
        end
    end
  end

  defp removal(options) do
    cond do
      options[:all] == true and options[:force] == true and not Map.has_key?(options, :id) -> :ok
      options[:all] != true and options[:force] != true and valid_id?(options[:id]) -> :ok
      true -> invalid("Remove requires --id, or --all --force without --id")
    end
  end

  defp modes(""), do: {:ok, []}

  defp modes(value) when is_binary(value) do
    values =
      value
      |> String.split(",")
      |> Enum.map(
        &(String.trim(&1)
          |> String.downcase()
          |> String.replace("replica-metadata-sync", "metadata-sync"))
      )

    if Enum.all?(values, &(&1 in ["delete-marker", "metadata-sync"])),
      do: {:ok, Enum.uniq(values)},
      else:
        extension(
          "Portable --replicate modes are delete-marker,metadata-sync or an empty string; permanent delete and existing-object administration are unsupported"
        )
  end

  defp modes(_), do: invalid("--replicate requires a comma-separated string")

  defp parse_tags(value) when is_binary(value) and byte_size(value) <= 32_768 do
    pairs = if value == "", do: [], else: String.split(value, "&")
    result = Enum.map(pairs, &String.split(&1, "=", parts: 2))

    if length(result) <= 10 and
         Enum.all?(result, fn
           [key, val] ->
             key != "" and String.valid?(key) and String.valid?(val) and String.length(key) <= 128 and
               String.length(val) <= 256

           _ ->
             false
         end) and length(Enum.uniq_by(result, &hd/1)) == length(result),
       do: {:ok, Enum.map(result, fn [key, val] -> %{"Key" => key, "Value" => val} end)},
       else:
         invalid(
           "Replication tags require at most ten distinct key=value pairs within S3 tag bounds"
         )
  end

  defp parse_tags(_), do: invalid("Replication tags exceed their input bound")

  defp bucket_arn?(value) when is_binary(value),
    do:
      Regex.match?(
        ~r/\Aarn:(aws|aws-cn|aws-us-gov):s3:::[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\z/,
        value
      )

  defp bucket_arn?(_), do: false

  defp role_arn?(value) when is_binary(value),
    do:
      Regex.match?(
        ~r/\Aarn:(aws|aws-cn|aws-us-gov):iam::[0-9]{12}:role\/[A-Za-z0-9+=,.@_\/-]+\z/,
        value
      )

  defp role_arn?(_), do: false

  defp valid_id?(value),
    do: is_binary(value) and String.valid?(value) and String.length(value) in 1..255

  defp state("enable"), do: "Enabled"
  defp state("disable"), do: "Disabled"

  defp scope(action, %Target{key: key}) when action in ["add", "update"] or key in [nil, ""],
    do: :ok

  defp scope(_, _),
    do: invalid("This replication action requires a bucket target without an object prefix")

  defp write(target, document, options, call) do
    with :ok <- validate(target, document),
         do:
           effect(
             "PutBucketReplication",
             %{"Bucket" => target.bucket, "ReplicationConfiguration" => document},
             options,
             call
           )
  end

  defp effect(operation, input, options, call) do
    with {:ok, input} <- Input.validate(operation, input) do
      if options[:dry_run],
        do: {:ok, %{"DryRun" => true, "Operation" => operation, "Input" => input}},
        else: call.(operation, input)
    end
  end

  defp new_id, do: "ess-" <> Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)
  defp invalid(message), do: {:error, Error.new(:syntax, message)}

  defp extension(message),
    do: {:error, Error.new(:unsupported, message, code: :provider_extension)}
end
