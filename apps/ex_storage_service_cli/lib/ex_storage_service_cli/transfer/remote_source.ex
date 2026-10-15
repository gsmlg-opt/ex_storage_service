defmodule ExStorageServiceCli.Transfer.RemoteSource do
  @moduledoc "Identity-fenced remote ranges. Bridges stage one planned part on disk before uploading it."
  alias ExStorageServiceCli.{
    Context,
    Error,
    Presenter,
    S3.Client,
    S3.OperationCatalog
  }

  alias ExStorageServiceCli.Transfer.{Engine, Source, State}

  @derive {Inspect, except: [:context, :input, :properties]}
  defstruct [
    :context,
    :bucket,
    :key,
    :identity,
    :input,
    :options_digest,
    :properties,
    offset: 0,
    length: 0,
    payload_hash: "UNSIGNED-PAYLOAD"
  ]

  def capture(context, bucket, key, opts \\ []) do
    input = Map.merge(opts[:source_input] || %{}, %{"Bucket" => bucket, "Key" => key})

    with {:ok, head} <- Client.call(context, "HeadObject", fields("HeadObject", input), opts),
         :ok <- selected_version(input, head),
         :ok <- selected_identity(opts[:selected_source], head),
         true <- is_integer(head["ContentLength"]) and head["ContentLength"] >= 0,
         true <- is_binary(head["ETag"]) and head["ETag"] != "" do
      identity =
        head |> Map.take(~w(ContentLength ETag VersionId LastModified)) |> Presenter.normalize()

      {:ok,
       %__MODULE__{
         context: context,
         bucket: bucket,
         key: key,
         input: input,
         identity: identity,
         properties: head,
         length: head["ContentLength"],
         options_digest: digest(opts[:source_input] || %{})
       }}
    else
      {:error, _} = error -> error
      _ -> failure(:invalid_remote_identity, "Remote source lacks its length or object identity")
    end
  end

  defp selected_version(input, head) do
    if input["VersionId"] != nil and head["VersionId"] != input["VersionId"],
      do:
        failure(:source_version_mismatch, "Provider did not confirm the selected source version"),
      else: :ok
  end

  defp selected_identity(nil, _head), do: :ok

  defp selected_identity(selected, head) do
    fields = ~w(ContentLength ETag VersionId LastModified)

    if Presenter.normalize(Map.take(selected, fields)) ==
         Presenter.normalize(Map.take(head, fields)),
       do: :ok,
       else: failure(:source_changed, "Remote source changed after inventory selection")
  end

  def validate(%__MODULE__{} = source) do
    input = selected_input(source) |> fields_for("HeadObject")

    with {:ok, head} <- Client.call(source.context, "HeadObject", input),
         current =
           head
           |> Map.take(~w(ContentLength ETag VersionId LastModified))
           |> Presenter.normalize() do
      if current == source.identity,
        do: :ok,
        else: failure(:source_changed, "Remote source changed during the transfer")
    end
  end

  def range(%__MODULE__{} = source, offset, length, opts \\ []) do
    if is_integer(offset) and is_integer(length) and offset >= 0 and length >= 0 and
         offset + length <= source.length do
      stage(source, offset, length, opts)
    else
      failure(:invalid_source_range, "Remote source range is invalid")
    end
  end

  defp stage(source, offset, length, opts) do
    root = opts[:staging_root] || Path.join(opts[:state_root] || State.root(), "staging")

    path =
      Path.join(
        root,
        "bridge-" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
      )

    input = selected_input(source) |> fields_for("GetObject")

    input =
      if offset == 0 and length == source.length,
        do: input,
        else: Map.put(input, "Range", "bytes=#{offset}-#{offset + length - 1}")

    with :ok <- validate(source),
         {:ok, _} <-
           Engine.download_file(
             source.context,
             source.bucket,
             source.key,
             path,
             Keyword.merge(opts, input: input, expected_bytes: length, verify_selection: true)
           ),
         {:ok, file} <- Source.file(path, coordinator: source.context.coordinator) do
      {:ok, %{file | temporary: true}}
    else
      {:error, _} = error ->
        File.rm(path)
        error
    end
  end

  def copy_input(%__MODULE__{} = source) do
    version = source.identity["VersionId"]

    input = %{
      "CopySource" =>
        %{"Bucket" => source.bucket, "Key" => source.key}
        |> then(fn input -> if version, do: Map.put(input, "VersionId", version), else: input end),
      "CopySourceIfMatch" => source.identity["ETag"]
    }

    Enum.reduce(
      ~w(SSECustomerAlgorithm SSECustomerKey SSECustomerKeyMD5),
      input,
      fn field, acc ->
        case source.input[field] do
          nil -> acc
          value -> Map.put(acc, "CopySource" <> field, value)
        end
      end
    )
  end

  def selected_input(source) do
    source.input
    |> Map.put("IfMatch", source.identity["ETag"])
    |> then(fn input ->
      if source.identity["VersionId"],
        do: Map.put(input, "VersionId", source.identity["VersionId"]),
        else: input
    end)
  end

  def record(source) do
    %{
      "kind" => "remote",
      "endpoint" => endpoint(source.context),
      "credential_provider" => provider(source.context),
      "bucket" => source.bucket,
      "key" => source.key,
      "identity" => source.identity,
      "offset" => 0,
      "length" => source.length,
      "options_digest" => source.options_digest
    }
  end

  def restore(context, record, opts) do
    context = opts[:source_context] || context

    if record["endpoint"] == endpoint(context) and
         record["credential_provider"] == provider(context) do
      with {:ok, source} <- capture(context, record["bucket"], record["key"], opts) do
        if record(source) == record,
          do: {:ok, source},
          else: failure(:resume_conflict, "Remote source identity or options changed")
      end
    else
      failure(:resume_conflict, "Resume requires the recorded remote source context")
    end
  end

  def valid_record?(record) do
    allowed =
      ~w(kind endpoint credential_provider bucket key identity offset length options_digest)

    is_map(record) and Enum.sort(Map.keys(record)) == Enum.sort(allowed) and
      record["kind"] == "remote" and record["offset"] == 0 and
      is_integer(record["length"]) and record["length"] >= 0 and
      is_binary(record["bucket"]) and record["bucket"] != "" and
      is_binary(record["key"]) and record["key"] != "" and
      is_map(record["identity"]) and record["identity"]["ContentLength"] == record["length"] and
      is_binary(record["identity"]["ETag"]) and
      Enum.all?(
        Map.keys(record["identity"]),
        &(&1 in ~w(ContentLength ETag VersionId LastModified))
      ) and
      is_binary(record["options_digest"]) and
      Regex.match?(~r/\A[0-9a-f]{64}\z/, record["options_digest"]) and
      is_map(record["endpoint"]) and is_map(record["credential_provider"]) and
      Context.validate_endpoint(record["endpoint"]["url"]) == :ok
  end

  def endpoint(context),
    do: %{
      "url" => context.endpoint,
      "region" => context.region,
      "addressing" => Atom.to_string(context.addressing),
      "service" => "s3"
    }

  def provider(%{unsigned: true}), do: %{"kind" => "anonymous", "name" => "anonymous"}

  def provider(context),
    do: %{
      "kind" => "profile",
      "name" => context.profile || "default",
      "identity" => digest(context.access_key_id || "")
    }

  defp digest(value),
    do:
      :crypto.hash(:sha256, :erlang.term_to_binary(value, [:deterministic]))
      |> Base.encode16(case: :lower)

  defp fields_for(input, operation), do: fields(operation, input)

  defp fields(operation, input) do
    {:ok, model} = OperationCatalog.operation(operation)
    members = OperationCatalog.shape(model["input"]["shape"])["members"] || %{}
    Map.take(input, Map.keys(members))
  end

  defp failure(code, message), do: {:error, Error.new(:client, message, code: code)}
end
