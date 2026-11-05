defmodule ExStorageService.CloudCache.MetadataPublication do
  @moduledoc """
  Fenced metadata deltas for upstream-authoritative gateway operations.

  Gateway versions record local metadata history, not upstream version IDs or
  hosted durability. Publication creates no CAS descriptor or blob location.
  The caller commits these deltas, its journal and the listing index together.
  """

  alias ExStorageService.Metadata.Keys

  @body_fields ~w(content_hash etag size content_type metadata system_headers checksums)a

  def snapshot(backend, bucket, key, opts \\ []) do
    with {:ok, observed} <- observe(backend, bucket, key, opts),
         :ok <- gateway_current(observed, bucket, key) do
      {:ok,
       %{
         head_revision: revision(observed.head),
         gateway_snapshot: %{
           head_revision: revision(observed.head),
           version_id: version_id(observed.head),
           version_revision: revision(observed.version)
         }
       }}
    end
  end

  def plan(backend, record, kind, metadata, local, opts \\ [])
      when kind in [:put, :delete] do
    id = publication_id(record, kind)
    version_key = Keys.object_version(record.bucket, record.key, id)
    head_key = Keys.object_head(record.bucket, record.key)

    with {:ok, observed} <- observe(backend, record.bucket, record.key, opts),
         {:ok, candidate} <- backend.get(version_key, read_opts(opts)) do
      cond do
        published?(observed, candidate, local, record, kind, metadata, id) ->
          {:ok, %{committed: true, metadata: candidate.value, compare: [], success: []}}

        candidate != nil or not matches_snapshot?(record, observed) ->
          {:error, :local_metadata_conflict}

        true ->
          version = new_version(record, kind, metadata, id)

          head = %{
            schema: 2,
            bucket: record.bucket,
            key: record.key,
            version_id: id,
            delete_marker: kind == :delete,
            deleted: false,
            etag: Map.get(version, :etag),
            gateway_operation_id: record.operation_id,
            updated_at: version.updated_at
          }

          {:ok,
           %{
             committed: false,
             metadata: version,
             compare:
               [compare(head_key, observed.head), {:exists, version_key, :==, false}] ++
                 version_compare(record, observed),
             success:
               [
                 {:put, version_key, version, %{}},
                 {:put, head_key, head, %{}}
               ] ++ local_operation(record, kind, version)
           }}
      end
    end
  end

  defp observe(backend, bucket, key, opts) do
    with {:ok, head} <- backend.get(Keys.object_head(bucket, key), read_opts(opts)),
         {:ok, version} <- selected_version(backend, bucket, key, head, opts) do
      {:ok, %{head: head, version: version}}
    end
  end

  defp selected_version(backend, bucket, key, head, opts) do
    case version_id(head) do
      id when is_binary(id) -> backend.get(Keys.object_version(bucket, key, id), read_opts(opts))
      nil -> {:ok, nil}
      _ -> {:error, :gateway_hosted_history_unsupported}
    end
  end

  defp gateway_current(%{head: nil}, _bucket, _key), do: :ok

  defp gateway_current(
         %{
           head: %{value: %{schema: 2, bucket: bucket, key: key, version_id: nil, deleted: true}}
         },
         bucket,
         key
       ),
       do: :ok

  defp gateway_current(%{head: head, version: %{value: version}}, bucket, key)
       when is_map(version) do
    if Map.get(head.value, :schema) == 2 and Map.get(head.value, :bucket) == bucket and
         Map.get(head.value, :key) == key and
         Map.get(version, :schema) == 2 and Map.get(version, :bucket) == bucket and
         Map.get(version, :key) == key and Map.get(version, :version_id) == version_id(head) and
         (Map.get(version, :cloud_backed) == true or marker?(version)) and
         marker?(head.value) == marker?(version) do
      :ok
    else
      {:error, :gateway_hosted_history_unsupported}
    end
  end

  defp gateway_current(_, _, _), do: {:error, :gateway_hosted_history_unsupported}

  defp matches_snapshot?(%{gateway_snapshot: snapshot}, observed) when is_map(snapshot) do
    snapshot == %{
      head_revision: revision(observed.head),
      version_id: version_id(observed.head),
      version_revision: revision(observed.version)
    }
  end

  # Old prepared journals did not capture the selected immutable version. They
  # retain only their original absent-head contract, never an arbitrary v2 head.
  defp matches_snapshot?(record, observed),
    do: Map.get(record, :head_revision) == nil and observed.head == nil

  defp published?(observed, %{value: version}, local, record, kind, metadata, id)
       when is_map(version) do
    observed.version != nil and observed.version.value == version and
      version_id(observed.head) == id and
      Map.get(observed.head.value, :gateway_operation_id) == record.operation_id and
      Map.get(version, :schema) == 2 and Map.get(version, :bucket) == record.bucket and
      Map.get(version, :key) == record.key and Map.get(version, :version_id) == id and
      Map.get(version, :cloud_backed) == true and
      Map.get(version, :gateway_operation_id) == record.operation_id and
      marker?(version) == (kind == :delete) and
      marker?(observed.head.value) == (kind == :delete) and
      Map.get(observed.head.value, :deleted) == false and
      Map.get(observed.head.value, :etag) == Map.get(version, :etag) and
      published_local?(kind, local, version, metadata)
  end

  defp published?(_, _, _, _, _, _, _), do: false

  defp published_local?(:put, %{value: value}, version, metadata),
    do: value == version and Map.take(version, @body_fields) == Map.take(metadata, @body_fields)

  defp published_local?(:delete, nil, _version, _metadata), do: true
  defp published_local?(_, _, _, _), do: false

  defp new_version(record, kind, metadata, id) do
    now = Map.get(metadata, :created_at, DateTime.utc_now() |> DateTime.to_iso8601())

    metadata
    |> Map.merge(%{
      schema: 2,
      bucket: record.bucket,
      key: record.key,
      version_id: id,
      parent_version_id: snapshot_parent(record),
      cloud_backed: true,
      gateway_operation_id: record.operation_id,
      delete_marker: kind == :delete,
      is_delete_marker: kind == :delete,
      object_type: :blob
    })
    |> Map.put_new(:created_at, now)
    |> Map.put_new(:updated_at, now)
  end

  defp snapshot_parent(%{gateway_snapshot: %{version_id: id}}), do: id
  defp snapshot_parent(_), do: nil

  defp version_compare(record, %{version: %{mod_revision: revision}, head: head}),
    do: [
      {:mod_revision, Keys.object_version(record.bucket, record.key, version_id(head)), :==,
       revision}
    ]

  defp version_compare(_, _), do: []

  defp local_operation(record, :put, version),
    do: [{:put, "obj:#{record.bucket}:#{record.key}", version, %{}}]

  defp local_operation(record, :delete, _version),
    do: [{:delete, {:key, "obj:#{record.bucket}:#{record.key}"}, %{}}]

  defp publication_id(record, kind) do
    identity = :erlang.term_to_binary({kind, record.bucket, record.key, record.operation_id})
    "ess-gateway-" <> Base.encode16(:crypto.hash(:sha256, identity), case: :lower)
  end

  defp marker?(metadata),
    do: Map.get(metadata, :delete_marker, Map.get(metadata, :is_delete_marker, false)) == true

  defp version_id(nil), do: nil
  defp version_id(%{value: value}) when is_map(value), do: Map.get(value, :version_id)
  defp version_id(_), do: :invalid
  defp revision(nil), do: nil
  defp revision(record), do: record.mod_revision
  defp compare(key, nil), do: {:exists, key, :==, false}
  defp compare(key, record), do: {:mod_revision, key, :==, record.mod_revision}

  defp read_opts(opts),
    do:
      opts
      |> Keyword.take([:engine, :timeout, :barrier])
      |> Keyword.put(:consistency, :linearizable)
end
