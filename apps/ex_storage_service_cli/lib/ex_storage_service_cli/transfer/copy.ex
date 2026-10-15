defmodule ExStorageServiceCli.Transfer.Copy do
  @moduledoc "Same-service copy or bounded staged bridge, followed by destination identity verification."
  alias ExStorageServiceCli.{Error, S3.Client, Target}
  alias ExStorageServiceCli.Transfer.{Engine, Limits, Multipart, Planner, RemoteSource}

  @metadata_fields ~w(CacheControl ContentDisposition ContentEncoding ContentLanguage ContentType Expires Metadata)

  def remote(
        source_context,
        %Target{} = source_target,
        destination_context,
        %Target{} = destination,
        opts \\ []
      ) do
    with :ok <- distinct(source_context, source_target, destination_context, destination),
         {:ok, source} <-
           RemoteSource.capture(source_context, source_target.bucket, source_target.key, opts),
         {:ok, result} <- copy_source(source, destination_context, destination, opts) do
      {:ok, Map.put(result, :source_identity, source.identity)}
    end
  end

  def move(source_context, source, destination_context, destination, opts \\ []) do
    with {:ok, result} <- remote(source_context, source, destination_context, destination, opts) do
      case delete_selected(source_context, source, result.source_identity, opts) do
        {:ok, _} ->
          {:ok, Map.put(result, :source_deleted, true)}

        {:error, error} ->
          {:error,
           %{
             error
             | details: Map.merge(error.details, %{copy_completed: true, source_deleted: false})
           }}
      end
    end
  end

  def delete_selected(context, target, identity, opts \\ []) do
    input =
      Map.merge(opts[:source_input] || %{}, %{"Bucket" => target.bucket, "Key" => target.key})

    version = identity["VersionId"]

    cond do
      is_binary(version) and version not in ["", "null"] ->
        Client.call(context, "DeleteObject", Map.put(input, "VersionId", version), opts)

      context.frontend == :aws or context.capabilities[:conditional_delete] == true ->
        Client.call(context, "DeleteObject", Map.put(input, "IfMatch", identity["ETag"]), opts)

      true ->
        failure(
          :conditional_delete_unavailable,
          "Copy completed; this provider has no verified conditional deletion contract, so the source was preserved"
        )
    end
  end

  defp copy_source(source, context, destination, opts) do
    with {:ok, plan} <-
           Planner.plan(
             source.length,
             opts[:limits] || Limits.for_context(context),
             Keyword.put_new(opts, :multipart_threshold, 8_388_608)
           ) do
      if same_service?(source.context, context) do
        case plan.mode do
          :single ->
            single(source, context, destination, opts)

          :multipart ->
            with {:ok, input} <- multipart_properties(source, opts) do
              Multipart.upload(
                context,
                destination.bucket,
                destination.key,
                source,
                plan,
                opts |> Keyword.put(:copy, true) |> Keyword.put(:input, input)
              )
            end
        end
      else
        with {:ok, input} <- multipart_properties(source, opts),
             opts = Keyword.put(opts, :input, input),
             {:ok, result} <-
               Engine.upload_source(context, destination.bucket, destination.key, source, opts),
             {:ok, head} <-
               verify(context, destination, source.length, result.etag, result.result, opts),
             do: {:ok, Map.merge(result, %{mode: :bridge, result: head})}
      end
    end
  end

  defp single(source, context, destination, opts) do
    input =
      single_properties(source, opts)
      |> Map.merge(RemoteSource.copy_input(source))
      |> Map.merge(%{"Bucket" => destination.bucket, "Key" => destination.key})

    with {:ok, %{"CopyObjectResult" => %{"ETag" => etag}} = result} <-
           Client.call(context, "CopyObject", input, opts),
         {:ok, head} <- verify(context, destination, source.length, etag, result, opts),
         :ok <- RemoteSource.validate(source) do
      {:ok, %{bytes: source.length, etag: etag, mode: :server_copy, result: head}}
    end
  end

  defp single_properties(source, opts) do
    input = opts[:input] || %{}

    cond do
      input["MetadataDirective"] != nil ->
        input

      opts[:copy_props] == "none" ->
        Map.merge(input, %{"MetadataDirective" => "REPLACE", "TaggingDirective" => "REPLACE"})

      true ->
        input =
          if Enum.any?(@metadata_fields, &Map.has_key?(input, &1)) do
            source.properties
            |> Map.take(@metadata_fields)
            |> Map.merge(input)
            |> Map.put("MetadataDirective", "REPLACE")
          else
            input
          end

        if opts[:copy_props] == "metadata-directive",
          do: Map.put(input, "TaggingDirective", "REPLACE"),
          else: input
    end
  end

  defp multipart_properties(source, opts) do
    explicit_directive? = Map.has_key?(opts[:input] || %{}, "MetadataDirective")

    inherited =
      if opts[:copy_props] == "none" or explicit_directive?,
        do: %{},
        else: Map.take(source.properties, @metadata_fields)

    input =
      Map.merge(inherited, opts[:input] || %{})
      |> Map.delete("MetadataDirective")
      |> Map.delete("TaggingDirective")

    if explicit_directive? or opts[:copy_props] in ["none", "metadata-directive"] or
         (opts[:input] || %{})["Tagging"] != nil do
      {:ok, input}
    else
      tagging_input =
        Map.take(
          RemoteSource.selected_input(source),
          ~w(Bucket Key VersionId RequestPayer ExpectedBucketOwner)
        )

      with {:ok, tags} <- Client.call(source.context, "GetObjectTagging", tagging_input, opts) do
        pairs = Enum.map(tags["TagSet"] || [], &{&1["Key"], &1["Value"]})
        {:ok, Map.put(input, "Tagging", ExStorageServiceCli.S3.Encoding.query(pairs))}
      end
    end
  end

  defp verify(context, destination, length, etag, result, opts) do
    input = %{"Bucket" => destination.bucket, "Key" => destination.key}

    input =
      if result["VersionId"], do: Map.put(input, "VersionId", result["VersionId"]), else: input

    input =
      Map.merge(
        Map.take(
          opts[:input] || %{},
          ~w(SSECustomerAlgorithm SSECustomerKey SSECustomerKeyMD5 RequestPayer ExpectedBucketOwner)
        ),
        input
      )

    with {:ok, head} <- Client.call(context, "HeadObject", input, opts) do
      if head["ContentLength"] == length and is_binary(etag) and etag != "" and
           tag(head["ETag"]) == tag(etag),
         do: {:ok, head},
         else:
           failure(
             :copy_verification_failed,
             "Copied destination does not match its confirmed object identity"
           )
    end
  end

  def same_service?(source, destination) do
    Map.take(source, [
      :endpoint,
      :access_key_id,
      :secret_access_key,
      :session_token,
      :unsigned
    ]) ==
      Map.take(destination, [
        :endpoint,
        :access_key_id,
        :secret_access_key,
        :session_token,
        :unsigned
      ])
  end

  defp distinct(source_context, source, destination_context, destination) do
    if source_context.endpoint == destination_context.endpoint and
         source.bucket == destination.bucket and source.key == destination.key,
       do: failure(:same_resource, "Source and destination select the same object"),
       else: :ok
  end

  defp tag(value) when is_binary(value), do: String.trim(value, "\"")
  defp tag(_), do: nil
  defp failure(code, message), do: {:error, Error.new(:transfer, message, code: code)}
end
