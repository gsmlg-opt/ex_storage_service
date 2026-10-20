defmodule ExStorageService.Metadata.ListIndex do
  @moduledoc """
  Transactional, generation-scoped listing indexes.

  Plans exact-key radix changes alongside the original metadata transaction.
  A successful page is bracketed by linearizable descriptor reads; changing
  descriptors cause bounded retries rather than a partially coherent response.
  Missing descriptors require explicit operator backfill, never an online scan.
  """

  alias ExStorageService.Metadata.{BucketLifecycle, Keys}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.ListIndex.{Codec, Radix}

  @attempts 4

  def root_key(bucket, generation),
    do:
      "ess:v2:list_index:#{Keys.encode_component(bucket)}:#{Keys.encode_component(generation)}:root"

  def empty(generation) do
    %{
      schema: 1,
      bucket_generation: generation,
      epoch: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false),
      state: :ready,
      legacy_complete: true,
      current_root: nil,
      versions_root: nil,
      uploads_root: nil,
      visible_count: 0,
      version_count: 0,
      active_upload_count: 0,
      commit_sequence: 0
    }
  end

  def namespace(bucket, root, kind) do
    root_key(bucket, root.bucket_generation)
    |> String.replace_suffix(":root", ":#{root.epoch}:#{kind}:node:")
  end

  @doc "Adds index changes to an uncommitted metadata specification."
  def augment(backend, bucket, spec, opts \\ []),
    do: augment_attempt(backend, bucket, spec, opts, 16)

  @doc false
  def transaction(backend, bucket, spec, opts \\ []) do
    case augment(backend, bucket, spec, opts) do
      {:ok, indexed} -> backend.transaction(indexed, opts)
      {:error, :conflict} -> {:ok, %{succeeded: false}}
      {:error, _} = error -> error
    end
  end

  defp augment_attempt(_backend, _bucket, _spec, _opts, 0), do: {:error, :conflict}

  defp augment_attempt(backend, bucket, spec, opts, attempts) do
    with {:ok, bucket_record} <- bucket_record(backend, bucket, opts),
         generation = BucketLifecycle.generation(bucket_record.value),
         key = root_key(bucket, generation),
         {:ok, before} <- backend.get(key, read_opts(opts)) do
      result = do_augment(backend, bucket, spec, opts)

      case result do
        {:ok, _} ->
          result

        {:error, _} ->
          with {:ok, after_record} <- backend.get(key, read_opts(opts)) do
            if before == after_record,
              do: result,
              else: augment_attempt(backend, bucket, spec, opts, attempts - 1)
          end
      end
    end
  end

  defp do_augment(backend, bucket, spec, opts) do
    with {:ok, bucket_record} <- bucket_record(backend, bucket, opts),
         generation = BucketLifecycle.generation(bucket_record.value),
         key = root_key(bucket, generation),
         {:ok, record} <- backend.get(key, read_opts(opts)),
         :ok <- writable(record, generation, opts),
         {:ok, changes} <- object_changes(backend, bucket, spec.success, opts),
         {:ok, planned} <- plan_objects(backend, bucket, record.value, changes, opts),
         {:ok, multipart} <-
           ExStorageService.Metadata.ListIndex.Multipart.augment(
             backend,
             bucket,
             record.value,
             spec.success,
             opts
           ) do
      root = record.value
      current = Radix.operations(planned.current)
      versions = Radix.operations(planned.versions)

      updated = %{
        root
        | current_root: current.root_id,
          versions_root: versions.root_id,
          uploads_root: multipart.root_id,
          visible_count: root.visible_count + current.count_delta,
          version_count: root.version_count + versions.count_delta,
          active_upload_count: root.active_upload_count + multipart.count_delta,
          commit_sequence: root.commit_sequence + 1
      }

      {:ok,
       %{
         spec
         | compare:
             spec.compare ++
               [{:mod_revision, key, :==, record.mod_revision}] ++
               changes.compare ++ current.compare ++ versions.compare ++ multipart.compare,
           success:
             multipart.metadata_operations ++
               current.success ++
               versions.success ++
               multipart.index_operations ++ [{:put, key, updated, %{}}]
       }}
    end
  end

  @doc "Reads a bounded page of listing summaries with a revision barrier."
  def page(bucket, kind, bound, opts \\ []) when kind in [:current, :versions, :uploads] do
    started = System.monotonic_time()

    result = do_page(bucket, kind, bound, opts)

    :telemetry.execute(
      [:ex_storage_service, :listing, :page, :stop],
      %{duration: System.monotonic_time() - started},
      %{kind: kind, result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )

    result
  end

  defp do_page(bucket, kind, bound, opts) do
    limit = Keyword.get(opts, :limit, 1000)

    if is_integer(limit) and limit in 0..1000 and is_binary(bound) do
      coherent_page(Keyword.get(opts, :backend, Backend), bucket, kind, bound, opts, @attempts)
    else
      {:error, :invalid_argument}
    end
  end

  def status(bucket, opts \\ []) do
    backend = Keyword.get(opts, :backend, Backend)

    with {:ok, bucket_record} <- bucket_record(backend, bucket, opts),
         generation = BucketLifecycle.generation(bucket_record.value),
         {:ok, record} <- backend.get(root_key(bucket, generation), read_opts(opts)) do
      case record do
        nil -> {:ok, %{state: :migration_required, bucket_generation: generation}}
        %{value: value} -> {:ok, value}
      end
    end
  end

  def mutation_ready(backend, bucket, generation, opts) do
    with {:ok, record} <- backend.get(root_key(bucket, generation), read_opts(opts)),
         do: writable(record, generation, opts)
  end

  def authoritative?(bucket, opts \\ []) do
    case status(bucket, opts) do
      {:ok, %{state: :ready, legacy_complete: true}} -> {:ok, true}
      {:ok, _} -> {:ok, false}
      {:error, _} = error -> error
    end
  end

  def ready(nil, _generation), do: {:error, :listing_index_migration_required}

  def ready(
        %{value: %{schema: 1, state: :ready, bucket_generation: generation} = value},
        generation
      ) do
    valid =
      Map.get(value, :legacy_complete) == true and
        is_binary(Map.get(value, :epoch)) and byte_size(value.epoch) in 1..128 and
        Enum.all?([:current_root, :versions_root, :uploads_root], fn key ->
          Map.has_key?(value, key) and
            (is_nil(value[key]) or (is_binary(value[key]) and byte_size(value[key]) == 16))
        end) and
        Enum.all?(
          [:visible_count, :version_count, :active_upload_count, :commit_sequence],
          fn key ->
            is_integer(value[key]) and value[key] >= 0
          end
        )

    if valid, do: :ok, else: {:error, :listing_index_corrupt}
  end

  def ready(_record, _generation), do: {:error, :listing_index_unavailable}

  defp writable(%{value: %{state: :building, build_token: token}} = record, generation, opts) do
    if Keyword.get(opts, :index_build_token) == token and is_binary(token) do
      ready(%{record | value: %{record.value | state: :ready, legacy_complete: true}}, generation)
    else
      {:error, :listing_index_unavailable}
    end
  end

  defp writable(record, generation, _opts), do: ready(record, generation)

  defp bucket_record(backend, bucket, opts) do
    case backend.get("bucket:#{bucket}", read_opts(opts)) do
      {:ok, nil} -> {:error, :bucket_not_found}
      result -> result
    end
  end

  defp object_changes(backend, bucket, operations, opts) do
    writes = Map.new(operations, &operation_entry/1)

    objects =
      operations
      |> Enum.map(&operation_entry/1)
      |> Enum.map(fn {key, _} -> object_identity(key, bucket) end)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    Enum.reduce_while(objects, {:ok, %{objects: [], compare: []}}, fn key, {:ok, acc} ->
      case observe_object(backend, bucket, key, writes, opts) do
        {:ok, change} ->
          {:cont,
           {:ok,
            %{
              objects: [change | acc.objects],
              compare: change.compare ++ acc.compare
            }}}

        error ->
          {:halt, error}
      end
    end)
  end

  defp observe_object(backend, bucket, key, writes, opts) do
    head_key = Keys.object_head(bucket, key)

    with {:ok, head} <- backend.get(head_key, read_opts(opts)) do
      old_head = value(head)
      new_head = Map.get(writes, head_key, old_head)

      ids =
        writes
        |> Map.keys()
        |> Enum.flat_map(fn record_key ->
          prefix = Keys.object_version_prefix(bucket, key)

          if String.starts_with?(record_key, prefix),
            do: [String.replace_prefix(record_key, prefix, "")],
            else: []
        end)
        |> Kernel.++([field(old_head, :version_id), field(new_head, :version_id)])
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()

      Enum.reduce_while(
        ids,
        {:ok, %{old: [], new: [], compare: [compare(head_key, head)]}},
        fn id, {:ok, acc} ->
          record_key = Keys.object_version(bucket, key, id)

          case backend.get(record_key, read_opts(opts)) do
            {:ok, record} ->
              previous = value(record)
              next = Map.get(writes, record_key, previous)
              old = if previous, do: [{id, previous} | acc.old], else: acc.old
              new = if next, do: [{id, next} | acc.new], else: acc.new

              {:cont,
               {:ok, %{old: old, new: new, compare: [compare(record_key, record) | acc.compare]}}}

            error ->
              {:halt, error}
          end
        end
      )
      |> case do
        {:ok, changes} ->
          {:ok, Map.merge(changes, %{key: key, old_head: old_head, head: new_head})}

        error ->
          error
      end
    end
  end

  defp plan_objects(backend, bucket, root, changes, opts) do
    reader = &backend.get(&1, read_opts(opts))

    initial = %{
      current: Radix.new(reader, namespace(bucket, root, :current), root.current_root),
      versions: Radix.new(reader, namespace(bucket, root, :versions), root.versions_root)
    }

    Enum.reduce_while(changes.objects, {:ok, initial}, fn change, {:ok, state} ->
      with {:ok, versions} <- remove_versions(state.versions, change),
           {:ok, versions} <- insert_versions(versions, bucket, change),
           {:ok, current} <- Radix.delete(state.current, change.key),
           {:ok, current} <- insert_current(current, bucket, change) do
        {:cont, {:ok, %{versions: versions, current: current}}}
      else
        error -> {:halt, error}
      end
    end)
  end

  defp remove_versions(tree, change) do
    reduce_tree(change.old, tree, fn {id, metadata}, tree ->
      Radix.delete(
        tree,
        Codec.version(change.key, id, metadata, id == field(change.old_head, :version_id))
      )
    end)
  end

  defp insert_versions(tree, bucket, change) do
    reduce_tree(change.new, tree, fn {id, metadata}, tree ->
      latest = id == field(change.head, :version_id)

      Radix.put(
        tree,
        Codec.version(change.key, id, metadata, latest),
        summary(bucket, change.key, id, metadata) |> Map.put(:is_latest, latest)
      )
    end)
  end

  defp insert_current(tree, bucket, change) do
    id = field(change.head, :version_id)

    case Enum.find(change.new, fn {candidate, metadata} ->
           candidate == id and not Map.get(metadata, :delete_marker, false)
         end) do
      {^id, metadata} -> Radix.put(tree, change.key, summary(bucket, change.key, id, metadata))
      nil -> {:ok, tree}
    end
  end

  defp summary(bucket, key, id, metadata) do
    %{
      key: key,
      version_id: id,
      metadata_key: Keys.object_version(bucket, key, id),
      etag: Map.get(metadata, :etag),
      size: Map.get(metadata, :size, 0),
      last_modified: Map.get(metadata, :created_at, Map.get(metadata, :updated_at, "")),
      delete_marker:
        Map.get(metadata, :delete_marker, Map.get(metadata, :is_delete_marker, false))
    }
  end

  defp reduce_tree(entries, tree, fun) do
    Enum.reduce_while(entries, {:ok, tree}, fn entry, {:ok, tree} ->
      case fun.(entry, tree) do
        {:ok, tree} -> {:cont, {:ok, tree}}
        error -> {:halt, error}
      end
    end)
  end

  defp coherent_page(_backend, _bucket, _kind, _bound, _opts, 0),
    do: {:error, :listing_index_unavailable}

  defp coherent_page(backend, bucket, kind, bound, opts, attempts) do
    with {:ok, bucket_record} <- bucket_record(backend, bucket, opts),
         generation = BucketLifecycle.generation(bucket_record.value),
         key = root_key(bucket, generation),
         {:ok, initial} <- backend.get(key, read_opts(opts)),
         :ok <- ready(initial, generation) do
      reader = &backend.get(&1, read_opts(opts))

      tree_result =
        case Keyword.get(opts, :tree_fun) do
          nil -> {:ok, namespace(bucket, initial.value, kind), initial.value[root_field(kind)]}
          fun -> fun.(reader, initial.value)
        end

      result =
        with {:ok, tree_namespace, root_id} <- tree_result do
          tree = Radix.new(reader, tree_namespace, root_id)

          case Keyword.get(opts, :bound_fun) do
            nil ->
              collect(tree, bound, Keyword.get(opts, :exclusive, false), opts, [], empty_stats())

            fun ->
              with {:ok, start, exclusive} <- fun.(reader) do
                collect(tree, start, exclusive, opts, [], empty_stats())
              end
          end
        end

      with {:ok, closing} <- backend.get(key, read_opts(opts)),
           {:ok, closing_bucket} <- backend.get("bucket:#{bucket}", read_opts(opts)) do
        if (initial == closing and closing_bucket) &&
             BucketLifecycle.generation(closing_bucket.value) == generation do
          result
        else
          coherent_page(backend, bucket, kind, bound, opts, attempts - 1)
        end
      end
    end
  end

  defp collect(tree, bound, exclusive, opts, entries, stats) do
    maximum = Keyword.get(opts, :limit, 1000)

    if maximum == 0 or length(entries) > maximum do
      {:ok, %{entries: Enum.reverse(entries), stats: stats}}
    else
      case Radix.seek(tree, bound, exclusive) do
        {:ok, nil, visited} ->
          {:ok, %{entries: Enum.reverse(entries), stats: merge_stats(stats, visited)}}

        {:ok, {key, summary}, visited} ->
          filter = Keyword.get(opts, :select, fn key, summary -> {:emit, key, summary} end)
          stats = merge_stats(stats, visited)

          case filter.(key, summary) do
            :stop ->
              {:ok, %{entries: Enum.reverse(entries), stats: stats}}

            {:emit, item_key, item} ->
              collect(tree, key, true, opts, [{item_key, item} | entries], stats)

            {:prefix, prefix, nil} ->
              {:ok, %{entries: Enum.reverse([{prefix, :common_prefix} | entries]), stats: stats}}

            {:prefix, prefix, successor} ->
              collect(tree, successor, false, opts, [{prefix, :common_prefix} | entries], stats)

            {:skip, nil} ->
              {:ok, %{entries: Enum.reverse(entries), stats: stats}}

            {:skip, successor} ->
              collect(tree, successor, false, opts, entries, stats)

            {:error, _} = error ->
              error
          end

        error ->
          error
      end
    end
  end

  defp empty_stats, do: %{reads: 0, bytes: 0, visited: 0, depth: 0}

  defp merge_stats(left, right),
    do: Map.merge(left, right, fn key, a, b -> if key == :depth, do: max(a, b), else: a + b end)

  defp root_field(:current), do: :current_root
  defp root_field(:versions), do: :versions_root
  defp root_field(:uploads), do: :uploads_root
  defp value(nil), do: nil
  defp value(record), do: record.value
  defp field(nil, _key), do: nil
  defp field(value, key), do: Map.get(value, key)
  defp compare(key, nil), do: {:exists, key, :==, false}
  defp compare(key, record), do: {:mod_revision, key, :==, record.mod_revision}
  defp operation_entry({:put, key, value, _}), do: {key, value}
  defp operation_entry({:delete, {:key, key}, _}), do: {key, nil}

  defp object_identity(record_key, bucket) do
    encoded = Keys.encode_component(bucket)

    case String.split(record_key, ":") do
      ["ess", "v2", "object_head", ^encoded, key] -> decode_key(key)
      ["ess", "v2", "object_version", ^encoded, key, _id] -> decode_key(key)
      _ -> nil
    end
  end

  defp decode_key(key) do
    case Keys.decode_component(key) do
      {:ok, decoded} -> decoded
      :error -> nil
    end
  end

  defp read_opts(opts),
    do: Keyword.take(opts, [:engine, :timeout, :barrier]) ++ [consistency: :linearizable]
end
