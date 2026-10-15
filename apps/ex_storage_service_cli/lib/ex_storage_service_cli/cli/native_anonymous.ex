defmodule ExStorageServiceCli.CLI.NativeAnonymous do
  @moduledoc "Complete policy JSON and conservative anonymous bucket-policy inspection."
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.S3.{Encoder, Endpoint, Input, Paginator}
  alias ExStorageServiceCli.Transfer.Manifest
  @max_policy 120 * 1024

  def commands do
    %{
      ["anonymous", "get-json"] => {1..1, %{}, "Export the complete bucket policy JSON"},
      ["anonymous", "set-json"] =>
        {2..2, %{"dry-run" => {:dry_run, :boolean}, "replace" => {:replace, :boolean}},
         "Set complete bucket policy from a literal JSON filepath; existing policy requires --replace"},
      ["anonymous", "list"] =>
        {1..1, %{}, "Inspect unconditional anonymous bucket-policy grants"},
      ["anonymous", "links"] =>
        {1..1, %{"recursive" => {:recursive, :boolean}, "limit" => {:limit, :integer}},
         "List bounded unsigned URLs covered by simple anonymous read grants"}
    }
  end

  def run(action, args, target, context, options, call, opts \\ [])

  def run(
        action,
        args,
        %Target{kind: :remote, bucket: bucket} = target,
        context,
        options,
        call,
        opts
      )
      when is_binary(bucket) and bucket != "" do
    execute(action, args, target, context, options, call, opts)
  end

  def run(_, _, _, _, _, _, _), do: invalid("Anonymous policy commands require a remote bucket")

  defp execute("get-json", _, target, _, _, call, _) do
    with {:ok, document} <- read(target, call), do: {:ok, document}
  end

  defp execute("set-json", [path, _], target, _, options, call, _) do
    with true <- target.key in [nil, ""],
         {:ok, bytes} <- file(path),
         {:ok, _document} <- document(bytes),
         :ok <- replace(target, options, call),
         input = %{"Bucket" => target.bucket, "Policy" => bytes},
         {:ok, _} <- Input.validate("PutBucketPolicy", input) do
      if options[:dry_run] == true,
        do: {:ok, %{"DryRun" => true, "Operation" => "PutBucketPolicy", "Input" => input}},
        else: call.("PutBucketPolicy", input)
    else
      false ->
        invalid("set-json replaces the whole bucket policy and requires a bucket-only target")

      error ->
        error
    end
  end

  defp execute(action, _, target, context, options, call, opts)
       when action in ["list", "links"] do
    with {:ok, document} <- read(target, call),
         {rules, unclassified} <- classify(document, target) do
      if action == "list" do
        {:ok,
         %{
           "Rules" => rules,
           "UnclassifiedStatements" => unclassified,
           "PolicyOnly" => true,
           "PublicAccessVerified" => false
         }}
      else
        with true <- unclassified == [],
             limit = Map.get(options, :limit, 1000),
             true <- is_integer(limit) and limit >= 0 and limit <= 10_000 do
          links(target, context, options, call, opts, rules, limit)
        else
          false ->
            invalid("links requires a fully classified policy and --limit between 0 and 10000")
        end
      end
    end
  end

  defp read(target, call) do
    case call.("GetBucketPolicy", %{"Bucket" => target.bucket}) do
      {:ok, %{"Policy" => bytes}} -> document(bytes)
      {:ok, _} -> invalid("The provider returned no complete bucket policy")
      error -> error
    end
  end

  defp document(bytes) when is_binary(bytes) and byte_size(bytes) <= @max_policy do
    case JSON.decode(bytes) do
      {:ok, %{"Statement" => statements} = document}
      when is_map(statements) or is_list(statements) ->
        if Enum.all?(List.wrap(statements), &is_map/1),
          do: {:ok, document},
          else: invalid("Bucket policy statements must be JSON objects")

      _ ->
        invalid("A complete JSON bucket policy with Statement is required")
    end
  end

  defp document(_), do: invalid("Bucket policy must fit within 120 KiB")

  defp file(path) do
    with {:ok, %{type: :regular, size: size}} when size > 0 and size <= @max_policy <-
           File.stat(path),
         {:ok, device} <- File.open(path, [:read, :binary]) do
      try do
        case IO.binread(device, @max_policy + 1) do
          bytes when is_binary(bytes) and byte_size(bytes) <= @max_policy -> {:ok, bytes}
          _ -> invalid("Policy filepath must contain nonempty JSON within 120 KiB")
        end
      after
        File.close(device)
      end
    else
      _ -> invalid("Policy filepath must be a readable nonempty regular file within 120 KiB")
    end
  end

  defp replace(_target, %{replace: true}, _call), do: :ok

  defp replace(target, _options, call) do
    case call.("GetBucketPolicy", %{"Bucket" => target.bucket}) do
      {:error, %Error{code: "NoSuchBucketPolicy"}} ->
        :ok

      {:ok, _} ->
        invalid("A bucket policy already exists; use --replace to replace the complete document")

      error ->
        error
    end
  end

  # Deliberately not an IAM evaluator. Any ambiguous statement prevents links.
  defp classify(document, target) do
    {grants, unknown} =
      document["Statement"]
      |> List.wrap()
      |> Enum.with_index(1)
      |> Enum.reduce({[], []}, fn {statement, index}, {grants, unknown} ->
        case grant(statement, target.bucket) do
          {:ok, selected} -> {selected ++ grants, unknown}
          :ignore -> {grants, unknown}
          :unknown -> {grants, [index | unknown]}
        end
      end)

    rules =
      grants
      |> Enum.group_by(& &1.resource, & &1.action)
      |> Enum.map(fn {resource, actions} ->
        %{"resource" => resource, "allow" => permission(actions)}
      end)
      |> Enum.filter(&related?(&1["resource"], target))
      |> Enum.sort_by(& &1["resource"])

    {rules, Enum.reverse(unknown)}
  end

  defp grant(statement, bucket) do
    principal = statement["Principal"]
    public = principal == "*" or principal == %{"AWS" => "*"} or principal == %{"AWS" => ["*"]}
    actions = List.wrap(statement["Action"])
    resources = List.wrap(statement["Resource"])

    cond do
      statement["Effect"] == "Deny" ->
        :unknown

      Enum.any?(~w(Condition NotPrincipal NotAction NotResource), &Map.has_key?(statement, &1)) ->
        :unknown

      not public ->
        :unknown

      statement["Effect"] != "Allow" ->
        :unknown

      actions == [] or resources == [] ->
        :unknown

      not Enum.all?(
        actions,
        &(&1 in ~w(s3:GetObject s3:PutObject s3:ListBucket s3:GetBucketLocation))
      ) ->
        :unknown

      true ->
        with true <- Enum.all?(resources, &simple_resource?(&1, bucket)) do
          {:ok,
           for(
             resource <- resources,
             action <- actions,
             action in ~w(s3:GetObject s3:PutObject),
             object_resource?(resource, bucket),
             do: %{resource: String.replace_prefix(resource, arn(bucket), bucket), action: action}
           )}
        else
          _ -> :unknown
        end
    end
  end

  defp arn(bucket), do: "arn:aws:s3:::" <> bucket
  defp object_resource?(resource, bucket), do: String.starts_with?(resource, arn(bucket) <> "/")

  defp simple_resource?(resource, bucket) when is_binary(resource) do
    resource == arn(bucket) or
      (object_resource?(resource, bucket) and not String.contains?(resource, "?") and
         length(String.split(resource, "*")) <= 2 and
         (not String.contains?(resource, "*") or String.ends_with?(resource, "*")) and
         not String.contains?(resource, "${"))
  end

  defp simple_resource?(_, _), do: false

  defp permission(actions) do
    case {"s3:GetObject" in actions, "s3:PutObject" in actions} do
      {true, true} -> "readwrite"
      {true, false} -> "readonly"
      {false, true} -> "writeonly"
    end
  end

  defp related?(resource, target) do
    path = target.bucket <> "/" <> (target.key || "")
    prefix = String.trim_trailing(resource, "*")

    String.starts_with?(prefix, path) or
      (String.ends_with?(resource, "*") and String.starts_with?(path, prefix))
  end

  defp links(target, context, options, call, opts, rules, limit) do
    readable = Enum.filter(rules, &(&1["allow"] in ["readonly", "readwrite"]))

    if readable == [] do
      {:ok, result([], 0)}
    else
      input = %{"Bucket" => target.bucket, "Prefix" => target.key || ""}
      input = if options[:recursive] == true, do: input, else: Map.put(input, "Delimiter", "/")

      entries =
        Paginator.pages("ListObjectsV2", input, call)
        |> Stream.flat_map(fn
          {:error, _} = error ->
            [error]

          {:ok, page} ->
            Enum.map(page["Contents"] || [], & &1["Key"]) ++
              Enum.map(page["CommonPrefixes"] || [], & &1["Prefix"])
        end)
        |> Stream.map(fn
          {:error, _} = error ->
            error

          key when is_binary(key) and key != "" ->
            if String.valid?(key) and String.starts_with?(key, target.key || ""),
              do: {:ok, %{"name" => key}},
              else: invalid("Invalid object key in links inventory")

          _ ->
            invalid("Invalid object key in links inventory")
        end)

      with {:ok, manifest} <- Manifest.build(entries, opts) do
        try do
          Manifest.stream(manifest)
          |> Stream.filter(&covered?(target.bucket <> "/" <> &1["name"], readable))
          |> Enum.reduce_while({:ok, [], 0}, fn entry, {:ok, urls, count} ->
            if count < limit do
              with {:ok, request} <-
                     Encoder.encode("GetObject", %{
                       "Bucket" => target.bucket,
                       "Key" => entry["name"]
                     }),
                   {:ok, url, _contract} <-
                     Endpoint.resolve(
                       context,
                       request,
                       %{"Bucket" => target.bucket, "Key" => entry["name"]},
                       opts
                     ) do
                {:cont, {:ok, [url | urls], count + 1}}
              else
                error -> {:halt, error}
              end
            else
              {:cont, {:ok, urls, count + 1}}
            end
          end)
          |> case do
            {:ok, urls, count} -> {:ok, result(Enum.reverse(urls), count)}
            error -> error
          end
        after
          Manifest.cleanup(manifest)
        end
      end
    end
  end

  defp covered?(path, rules),
    do:
      Enum.any?(rules, fn rule ->
        resource = rule["resource"]

        if String.ends_with?(resource, "*"),
          do: String.starts_with?(path, String.trim_trailing(resource, "*")),
          else: path == resource
      end)

  defp result(urls, count),
    do: %{
      "Links" => urls,
      "Returned" => length(urls),
      "MatchedObjects" => count,
      "Partial" => length(urls) < count,
      "PolicyOnly" => true,
      "PublicAccessVerified" => false
    }

  defp invalid(message), do: {:error, Error.new(:syntax, message)}
end
