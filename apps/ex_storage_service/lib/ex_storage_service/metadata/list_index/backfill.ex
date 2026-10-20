defmodule ExStorageService.Metadata.ListIndex.Backfill do
  @moduledoc """
  Explicit, quiesced, resumable listing-index conversion for operator RPC use.

  `begin/2` requires verified backup and actual source quiescence. After its
  transaction installs the per-bucket gate, services may resume: other buckets
  can mutate, while every guarded writer for this bucket remains unavailable.
  All writer binaries must implement that gate; mixed old binaries and direct
  Concord writes are unsupported and must remain stopped.

  Capture deliberately allocates one coherent global Concord snapshot. It is
  offline work, never an HTTP listing implementation or a bounded scan claim.
  External sorting uses 1 MiB runs and at most 16 open merge inputs. Manifest
  frames are checksummed and limited to 16 MiB (oversized legacy records fail
  explicitly). Resume and validation process at most 100 records per call,
  keeping checkpoints and radix paths independent of bucket cardinality.

  Original v1 records, prior epochs and backup artifacts are retained. Before
  cutover, rollback uses the verified source backup, rather than reinstalling
  an old descriptor over metadata already converted in this build.
  """
  alias ExStorageService.Metadata.{BucketLifecycle, Keys, ListIndex}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.ListIndex.{Codec, Manifest, Radix, Source}
  alias ExStorageService.Metadata.ListIndex.Multipart
  alias ExStorageService.Metadata.Migration
  alias ExStorageService.Operations.Backup

  @zero <<0::256>>
  @ambiguous [:timeout, :unknown, :cluster_not_ready, :no_leader]

  def build_key(bucket, generation),
    do: String.replace_suffix(ListIndex.root_key(bucket, generation), ":root", ":build")

  @doc "Gates a bucket after verifying a backup and actual process quiescence."
  def begin(bucket, opts \\ []) do
    backend = backend(opts)

    with :ok <- preflight(opts),
         {:ok, replication_factor} <- target_rf(opts),
         {:ok, bucket_record} <- existing(backend, "bucket:#{bucket}", opts),
         generation when is_binary(generation) <- BucketLifecycle.generation(bucket_record.value),
         root_key = ListIndex.root_key(bucket, generation),
         key = build_key(bucket, generation),
         {:ok, previous} <- backend.get(root_key, read_opts(opts)),
         :ok <- startable(previous, generation),
         {:ok, previous_build} <- backend.get(key, read_opts(opts)) do
      token = random()

      root =
        ListIndex.empty(generation)
        |> Map.merge(%{state: :building, legacy_complete: false, build_token: token})

      build = %{
        schema: 1,
        replication_factor: replication_factor,
        bucket_generation: generation,
        epoch: root.epoch,
        owner_token: token,
        phase: :capturing,
        prior_root: value(previous),
        source_manifest: nil,
        source_manifest_digest: nil,
        source_manifest_bytes: 0,
        offset: 0,
        last_sort: nil,
        processed: 0,
        build_digest: @zero,
        validation_offset: 0,
        validation_last_sort: nil,
        validation_digest: @zero,
        validation_counts: %{current: 0, versions: 0, uploads: 0},
        tree_kind: :current,
        tree_bound: <<>>,
        tree_started: false,
        tree_counts: %{current: 0, versions: 0, uploads: 0},
        last_error: nil
      }

      spec = %{
        compare: [
          compare("bucket:#{bucket}", bucket_record),
          compare(root_key, previous),
          compare(key, previous_build)
        ],
        success: [{:put, root_key, root, %{}}, {:put, key, build, %{}}],
        failure: []
      }

      with :ok <- commit(backend, spec, opts), do: {:ok, report(build)}
    else
      nil -> {:error, :invalid_bucket_generation}
      error -> error
    end
  end

  @doc "Exports a coherent offline snapshot and durably binds its sealed manifest."
  def capture(bucket, token, path, opts \\ []) do
    with {:ok, state} <- state(bucket, token, opts),
         true <- state.build.value.phase == :capturing,
         {:ok, snapshot} <- backend(opts).get_all(read_opts(opts)),
         true <- is_map(snapshot),
         true <- snapshot["bucket:#{bucket}"] == state.bucket.value,
         {:ok, _} <-
           Source.capture(bucket, state.generation, snapshot, path <> ".raw", path <> ".source"),
         {:ok, manifest} <- Manifest.create(path, seal(Manifest.stream(path <> ".source"), opts)),
         build = %{
           state.build.value
           | phase: :building,
             source_manifest: manifest.path,
             source_manifest_digest: manifest.digest,
             source_manifest_bytes: manifest.bytes
         },
         root = Map.put(state.root.value, :source_manifest_digest, manifest.digest),
         :ok <- checkpoint(state, root, build, [], [], opts) do
      {:ok, report(build)}
    else
      false -> {:error, :capture_state_changed}
      error -> error
    end
  catch
    {:source_error, reason} -> {:error, reason}
    {:manifest_error, reason} -> {:error, reason}
  end

  @doc "Replays a bounded batch, checkpointing each publication atomically."
  def resume(bucket, token, path, opts \\ []) do
    with :ok <- batch_size(opts),
         {:ok, state} <- state(bucket, token, opts),
         :ok <- manifest(state, path),
         true <- state.build.value.phase == :building do
      build_steps(bucket, token, path, Keyword.get(opts, :batch_size, 100), opts)
    else
      false -> {:error, :invalid_build_phase}
      error -> error
    end
  end

  @doc "Validates bounded source batches, then scans reachable trees in order."
  def validate(bucket, token, path, opts \\ []) do
    with :ok <- batch_size(opts),
         {:ok, state} <- state(bucket, token, opts),
         :ok <- manifest(state, path),
         true <- state.build.value.phase in [:validating, :tree_validation, :validated] do
      validate_steps(bucket, token, path, Keyword.get(opts, :batch_size, 100), opts)
    else
      false -> {:error, :invalid_validation_phase}
      error -> error
    end
  end

  @doc "Publishes validated roots and releases the gate in one fenced transaction."
  def cutover(bucket, token, opts \\ []) do
    with {:ok, state} <- state(bucket, token, opts),
         true <- state.build.value.phase == :validated,
         :ok <- manifest(state, state.build.value.source_manifest),
         true <- counts(state.root.value) == state.build.value.validation_counts,
         true <- state.build.value.tree_counts == state.build.value.validation_counts do
      build = %{state.build.value | phase: :ready}

      root =
        state.root.value
        |> Map.merge(%{
          state: :ready,
          legacy_complete: true,
          validation_digest: build.validation_digest
        })
        |> Map.delete(:build_token)

      with :ok <- ListIndex.ready(%{value: root}, state.generation),
           :ok <- checkpoint(state, root, build, [], [], opts),
           do: {:ok, report(build)}
    else
      false -> {:error, :index_validation_incomplete}
      error -> error
    end
  end

  @doc "Replaces the owner fence; the previous token cannot advance or cut over."
  def takeover(bucket, token, opts \\ []) do
    with {:ok, state} <- state(bucket, token, opts),
         true <- state.build.value.phase != :ready do
      next = random()
      build = %{state.build.value | owner_token: next}
      root = Map.put(state.root.value, :build_token, next)
      with :ok <- checkpoint(state, root, build, [], [], opts), do: {:ok, report(build)}
    else
      false -> {:error, :already_ready}
      error -> error
    end
  end

  def status(bucket, opts \\ []) do
    with {:ok, bucket_record} <- existing(backend(opts), "bucket:#{bucket}", opts),
         {:ok, record} <-
           backend(opts).get(
             build_key(bucket, BucketLifecycle.generation(bucket_record.value)),
             read_opts(opts)
           ) do
      if record, do: {:ok, report(record.value)}, else: {:ok, %{phase: :missing}}
    end
  end

  defp build_steps(bucket, token, _path, 0, opts), do: status_owned(bucket, token, opts)

  defp build_steps(bucket, token, path, remaining, opts) do
    with {:ok, state} <- state(bucket, token, opts),
         {:ok, page} <- Manifest.batch(path, state.build.value.offset, 1) do
      case page.entries do
        [] ->
          build = %{state.build.value | phase: :validating}
          root = %{state.root.value | state: :validating}
          with :ok <- checkpoint(state, root, build, [], [], opts), do: {:ok, report(build)}

        [{sort, action}] ->
          with :ok <- ordered(state.build.value.last_sort, sort),
               {:ok, spec} <- action_spec(state, action, opts),
               build = %{
                 state.build.value
                 | offset: page.offset,
                   last_sort: sort,
                   processed: state.build.value.processed + 1,
                   build_digest: chain(state.build.value.build_digest, {sort, action})
               },
               spec = %{
                 spec
                 | compare: fences(state) ++ spec.compare,
                   success: spec.success ++ [{:put, state.build_key, build, %{}}]
               },
               :ok <- commit(backend(opts), spec, opts) do
            build_steps(bucket, token, path, remaining - 1, opts)
          end
      end
    end
  end

  defp action_spec(state, action, opts) do
    with {:ok, source_compare} <- source_fences(action.sources, opts),
         {:ok, observed} <- target_record(action, opts),
         :ok <- expected_target(action, observed) do
      target_compare = if action.key, do: [compare(action.key, observed)], else: []
      success = if action.key, do: [{:put, action.key, action.value, %{}}], else: []
      spec = %{compare: source_compare ++ target_compare, success: success, failure: []}

      case action.type do
        :assert ->
          {:ok, %{spec | success: [{:put, state.root_key, state.root.value, %{}}]}}

        :upload_init ->
          upload =
            action.value
            |> Map.merge(%{
              parts_index_schema: 1,
              parts_root: nil,
              parts_count: 0,
              parts_index_epoch: state.root.value.epoch,
              parts_index_complete: true
            })

          root = Map.update!(state.root.value, :commit_sequence, &(&1 + 1))

          {:ok,
           %{spec | success: [{:put, action.key, upload, %{}}, {:put, state.root_key, root, %{}}]}}

        :upload_finish ->
          expected = Map.fetch!(action.value, :expected_parts_count)

          if observed && observed.value.parts_count == expected do
            spec = %{spec | success: [{:put, action.key, observed.value, %{}}]}
            ListIndex.augment(backend(opts), state.name, spec, build_opts(state, opts))
          else
            {:error, {:part_count_mismatch, action.key}}
          end

        :version ->
          with {:ok, blobs} <-
                 Migration.prepare_index_version(action.value, build_opts(state, opts)) do
            spec = %{
              spec
              | compare: spec.compare ++ blobs.compare,
                success: spec.success ++ blobs.success
            }

            ListIndex.augment(backend(opts), state.name, spec, build_opts(state, opts))
          end

        _ ->
          ListIndex.augment(backend(opts), state.name, spec, build_opts(state, opts))
      end
    end
  end

  defp validate_steps(bucket, token, _path, 0, opts), do: status_owned(bucket, token, opts)

  defp validate_steps(bucket, token, path, remaining, opts) do
    with {:ok, state} <- state(bucket, token, opts) do
      case state.build.value.phase do
        :validated ->
          {:ok, report(state.build.value)}

        :tree_validation ->
          with {:ok, build} <- validate_tree_step(state, opts),
               :ok <- checkpoint(state, state.root.value, build, [], [], opts),
               do: validate_steps(bucket, token, path, remaining - 1, opts)

        :validating ->
          with {:ok, page} <- Manifest.batch(path, state.build.value.validation_offset, 1) do
            case page.entries do
              [] ->
                build = %{state.build.value | phase: :tree_validation}

                with true <- build.validation_counts == counts(state.root.value),
                     :ok <- checkpoint(state, state.root.value, build, [], [], opts),
                     do: validate_steps(bucket, token, path, remaining - 1, opts)

              [{sort, action}] ->
                with :ok <- ordered(state.build.value.validation_last_sort, sort),
                     {:ok, delta} <- validate_action(state, action, opts),
                     build = %{
                       state.build.value
                       | validation_offset: page.offset,
                         validation_last_sort: sort,
                         validation_counts:
                           add_counts(state.build.value.validation_counts, delta),
                         validation_digest:
                           chain(state.build.value.validation_digest, {sort, action})
                     },
                     :ok <- checkpoint(state, state.root.value, build, [], [], opts),
                     do: validate_steps(bucket, token, path, remaining - 1, opts)
            end
          end
      end
    end
  end

  defp validate_action(_state, %{type: :assert} = action, opts) do
    result =
      Enum.reduce_while(action.sources, :ok, fn {key, expected}, :ok ->
        case backend(opts).get(key, read_opts(opts)) do
          {:ok, %{value: value}} when value == expected.value -> {:cont, :ok}
          {:ok, _} -> {:halt, {:error, {:source_changed, key}}}
          error -> {:halt, error}
        end
      end)

    with :ok <- result, do: {:ok, %{}}
  end

  defp validate_action(state, %{type: :version} = action, opts) do
    metadata = action.value

    with {:ok, %{value: ^metadata}} <- existing(backend(opts), action.key, opts),
         {:ok, _} <-
           Migration.prepare_index_version(
             metadata,
             Keyword.put(build_opts(state, opts), :index_validate, true)
           ),
         {:ok, head} <-
           backend(opts).get(Keys.object_head(state.name, metadata.key), read_opts(opts)),
         latest = head != nil and Map.get(head.value, :version_id) == metadata.version_id,
         summary = summary(state.name, metadata),
         :ok <-
           terminal(
             state,
             :versions,
             Codec.version(metadata.key, metadata.version_id, metadata, latest),
             Map.put(summary, :is_latest, latest),
             opts
           ) do
      {:ok, %{versions: 1}}
    else
      {:ok, _} -> {:error, {:normalized_version_changed, action.key}}
      error -> error
    end
  end

  defp validate_action(state, %{type: :head} = action, opts) do
    with {:ok, %{value: head}} <- existing(backend(opts), action.key, opts),
         true <- head == action.value do
      id = Map.get(head, :version_id)

      if id == nil or Map.get(head, :delete_marker, false) do
        with :ok <- absent_terminal(state, :current, head.key, opts), do: {:ok, %{}}
      else
        with {:ok, %{value: metadata}} <-
               existing(backend(opts), Keys.object_version(state.name, head.key, id), opts),
             :ok <- terminal(state, :current, head.key, summary(state.name, metadata), opts),
             do: {:ok, %{current: 1}}
      end
    else
      false -> {:error, {:head_changed, action.key}}
      error -> error
    end
  end

  defp validate_action(_state, %{type: :upload_init}, _opts), do: {:ok, %{}}

  defp validate_action(state, %{type: :part} = action, opts) do
    with {:ok, %{value: part}} <- existing(backend(opts), action.key, opts),
         true <- part == action.value,
         {:ok, id} <- part_upload(action.key, state.name),
         {:ok, upload} <- upload(state.name, id, opts),
         expected =
           Map.take(part, [
             :part_number,
             :etag,
             :size,
             :hash,
             :uploaded_at,
             :checksums,
             :checksum_algorithm,
             :checksum_type
           ]),
         :ok <-
           exact_terminal(
             Multipart.parts_namespace(state.name, state.root.value, id),
             upload.value.parts_root,
             Codec.part(part.part_number),
             expected,
             opts
           ) do
      {:ok, %{}}
    else
      false -> {:error, {:part_changed, action.key}}
      error -> error
    end
  end

  defp validate_action(state, %{type: :upload_finish} = action, opts) do
    expected = Map.pop!(action.value, :expected_parts_count)
    {expected_parts, expected_upload} = expected

    with {:ok, %{value: actual}} <- existing(backend(opts), action.key, opts),
         true <- Enum.all?(expected_upload, fn {key, value} -> Map.get(actual, key) == value end),
         true <-
           actual.parts_count == expected_parts and
             actual.parts_index_epoch == state.root.value.epoch and
             actual.parts_index_complete == true and actual.parts_index_schema == 1,
         :ok <- validate_parts_shape(state, actual, expected_parts, opts) do
      if actual.status in [:initiated, :completing] do
        with :ok <-
               terminal(
                 state,
                 :uploads,
                 Codec.upload(actual.key, actual.upload_id),
                 Map.take(actual, [:bucket, :key, :upload_id, :created_at]),
                 opts
               ),
             do: {:ok, %{uploads: 1}}
      else
        with :ok <-
               absent_terminal(state, :uploads, Codec.upload(actual.key, actual.upload_id), opts),
             do: {:ok, %{}}
      end
    else
      false -> {:error, {:upload_changed, action.key}}
      error -> error
    end
  end

  defp validate_parts_shape(state, upload, expected, opts) do
    namespace = Multipart.parts_namespace(state.name, state.root.value, upload.upload_id)
    tree = Radix.new(&backend(opts).get(&1, read_opts(opts)), namespace, upload.parts_root)
    count_parts(tree, <<>>, false, 0, expected)
  end

  defp count_parts(_tree, _bound, _exclusive, count, expected)
       when count > expected or count > 10_000,
       do: {:error, :parts_tree_count_mismatch}

  defp count_parts(tree, bound, exclusive, count, expected) do
    case Radix.seek(tree, bound, exclusive) do
      {:ok, nil, _} when count == expected ->
        :ok

      {:ok, nil, _} ->
        {:error, :parts_tree_count_mismatch}

      {:ok, {encoded, value}, _} ->
        with {:ok, number} <- Codec.decode_part(encoded),
             true <- Map.get(value, :part_number) == number,
             do: count_parts(tree, encoded, true, count + 1, expected)

      error ->
        error
    end
  end

  defp validate_tree_step(state, opts) do
    build = state.build.value
    kind = build.tree_kind

    tree =
      Radix.new(
        &backend(opts).get(&1, read_opts(opts)),
        ListIndex.namespace(state.name, state.root.value, kind),
        state.root.value[root_field(kind)]
      )

    case Radix.seek(tree, build.tree_bound, build.tree_started) do
      {:ok, nil, _} ->
        if build.tree_counts[kind] == build.validation_counts[kind] do
          case kind do
            :uploads ->
              {:ok, %{build | phase: :validated}}

            :current ->
              {:ok, %{build | tree_kind: :versions, tree_bound: <<>>, tree_started: false}}

            :versions ->
              {:ok, %{build | tree_kind: :uploads, tree_bound: <<>>, tree_started: false}}
          end
        else
          {:error, {:tree_count_mismatch, kind}}
        end

      {:ok, {key, value}, _} ->
        next_count = build.tree_counts[kind] + 1

        if next_count <= build.validation_counts[kind] do
          {:ok,
           %{
             build
             | tree_bound: key,
               tree_started: true,
               tree_counts: Map.put(build.tree_counts, kind, next_count),
               validation_digest: chain(build.validation_digest, {kind, key, value})
           }}
        else
          {:error, {:tree_count_mismatch, kind}}
        end

      error ->
        error
    end
  end

  defp terminal(state, kind, key, expected, opts),
    do:
      exact_terminal(
        ListIndex.namespace(state.name, state.root.value, kind),
        state.root.value[root_field(kind)],
        key,
        expected,
        opts
      )

  defp exact_terminal(namespace, root, key, expected, opts) do
    tree = Radix.new(&backend(opts).get(&1, read_opts(opts)), namespace, root)

    case Radix.seek(tree, key) do
      {:ok, {^key, ^expected}, _} -> :ok
      {:ok, _, _} -> {:error, :index_terminal_mismatch}
      error -> error
    end
  end

  defp absent_terminal(state, kind, key, opts) do
    tree =
      Radix.new(
        &backend(opts).get(&1, read_opts(opts)),
        ListIndex.namespace(state.name, state.root.value, kind),
        state.root.value[root_field(kind)]
      )

    case Radix.seek(tree, key) do
      {:ok, {^key, _}, _} -> {:error, :unexpected_index_terminal}
      {:ok, _, _} -> :ok
      error -> error
    end
  end

  defp seal(entries, opts) do
    Stream.map(entries, fn {sort, action} ->
      sources =
        Enum.map(action.sources, fn {key, expected} ->
          case backend(opts).get(key, read_opts(opts)) do
            {:ok, %{value: ^expected} = record} -> {key, record}
            _ -> throw({:source_error, {key, :source_changed_during_capture}})
          end
        end)

      target =
        if action.key && action.type != :upload_finish do
          case backend(opts).get(action.key, read_opts(opts)) do
            {:ok, record} when record == nil and action.previous == nil -> nil
            {:ok, %{value: previous} = record} when previous == action.previous -> record
            _ -> throw({:source_error, {action.key, :target_changed_during_capture}})
          end
        end

      {sort, Map.merge(action, %{sources: sources, target_record: target})}
    end)
  end

  defp source_fences(sources, opts) do
    Enum.reduce_while(sources, {:ok, []}, fn {key, expected}, {:ok, compares} ->
      case backend(opts).get(key, read_opts(opts)) do
        {:ok, ^expected} -> {:cont, {:ok, [compare(key, expected) | compares]}}
        {:ok, _} -> {:halt, {:error, {:source_changed, key}}}
        error -> {:halt, error}
      end
    end)
  end

  defp target_record(%{key: nil}, _opts), do: {:ok, nil}
  defp target_record(action, opts), do: backend(opts).get(action.key, read_opts(opts))
  defp expected_target(%{type: :upload_finish}, _record), do: :ok
  defp expected_target(%{target_record: expected}, expected), do: :ok
  defp expected_target(action, _record), do: {:error, {:target_changed, action.key}}

  defp state(bucket, token, opts) do
    with {:ok, bucket_record} <- existing(backend(opts), "bucket:#{bucket}", opts),
         generation = BucketLifecycle.generation(bucket_record.value),
         root_key = ListIndex.root_key(bucket, generation),
         key = build_key(bucket, generation),
         {:ok, root} <- existing(backend(opts), root_key, opts),
         {:ok, build} <- existing(backend(opts), key, opts),
         true <-
           build.value.schema == 1 and build.value.bucket_generation == generation and
             build.value.owner_token == token and root.value.epoch == build.value.epoch and
             (build.value.phase == :ready or Map.get(root.value, :build_token) == token) do
      {:ok,
       %{
         name: bucket,
         bucket: bucket_record,
         generation: generation,
         root_key: root_key,
         root: root,
         build_key: key,
         build: build
       }}
    else
      false -> {:error, :obsolete_build_owner}
      error -> error
    end
  end

  defp checkpoint(state, root, build, compare, success, opts) do
    spec = %{
      compare: fences(state) ++ compare,
      success:
        success ++ [{:put, state.root_key, root, %{}}, {:put, state.build_key, build, %{}}],
      failure: []
    }

    commit(backend(opts), spec, opts)
  end

  defp fences(state),
    do: [
      compare("bucket:#{state.name}", state.bucket),
      compare(state.root_key, state.root),
      compare(state.build_key, state.build)
    ]

  defp commit(backend, spec, opts) do
    # A failed compare is a completed transaction outcome. A later explicit
    # attempt needs a fresh ID even when its specification is byte-identical;
    # ambiguous resolution below retains this invocation's captured ID.
    id =
      "listing-backfill:" <>
        random() <>
        ":" <>
        Base.encode16(:crypto.hash(:sha256, :erlang.term_to_binary(spec)), case: :lower)

    case backend.transaction(spec, Keyword.put(write_opts(opts), :idempotency_key, id)) do
      {:ok, %{succeeded: true}} ->
        :ok

      {:ok, %{succeeded: false}} ->
        {:error, :build_conflict}

      {:error, reason} when reason in @ambiguous ->
        case backend.resolve_transaction(id, read_opts(opts)) do
          {:ok, %{succeeded: true}} -> :ok
          {:ok, %{succeeded: false}} -> {:error, :build_conflict}
          _ -> {:error, {:unresolved_build_transaction, id, reason}}
        end

      error ->
        error
    end
  end

  defp manifest(state, path) do
    with true <- is_binary(path) and Path.expand(path) == state.build.value.source_manifest,
         {:ok, digest, bytes} <- Manifest.digest(path),
         true <-
           digest == state.build.value.source_manifest_digest and
             bytes == state.build.value.source_manifest_bytes do
      :ok
    else
      false -> {:error, :manifest_identity_changed}
      error -> error
    end
  end

  defp preflight(opts) do
    module = Keyword.get(opts, :backup_module, Backup)
    sources = Keyword.get(opts, :sources, [])
    backup = Keyword.get(opts, :backup)

    if is_list(sources) and sources != [] and is_binary(backup) do
      with :ok <- complete_sources(module, sources),
           :ok <-
             Enum.reduce_while(sources, :ok, fn source, :ok ->
               case quiesced(module, source) do
                 :ok -> {:cont, :ok}
                 error -> {:halt, error}
               end
             end),
           {:ok, _} <- module.verify(backup, opts),
           do: :ok
    else
      {:error, :verified_backup_and_quiesced_sources_required}
    end
  end

  defp complete_sources(Backup, [first | _] = sources) do
    expected =
      if first.config.mode == :cluster,
        do: Enum.map(first.config.cluster_members, & &1.id),
        else: [first.config.node_id]

    actual = Enum.map(sources, & &1.config.node_id)

    if Enum.sort(expected) == Enum.sort(actual) and length(actual) == length(Enum.uniq(actual)),
      do: :ok,
      else: {:error, :incomplete_backup_sources}
  end

  defp complete_sources(_module, _sources), do: :ok

  defp quiesced(module, %ExStorageService.Context{config: config} = source) do
    if config.erlang_node == node() do
      module.assert_quiesced(source)
    else
      case :rpc.call(config.erlang_node, module, :assert_quiesced, [source], 30_000) do
        :ok -> :ok
        {:error, _} = error -> error
        _ -> {:error, :source_quiescence_unavailable}
      end
    end
  end

  defp quiesced(module, source), do: module.assert_quiesced(source)
  defp startable(nil, _generation), do: :ok

  defp startable(%{value: %{state: :ready}} = record, generation),
    do: ListIndex.ready(record, generation)

  defp startable(_, _generation), do: {:error, :build_already_active}

  defp batch_size(opts) do
    if Keyword.get(opts, :batch_size, 100) in 1..100, do: :ok, else: {:error, :invalid_batch_size}
  end

  defp ordered(nil, _next), do: :ok
  defp ordered(previous, next) when previous < next, do: :ok
  defp ordered(_, _), do: {:error, :manifest_order_violation}

  defp counts(root),
    do: %{
      current: root.visible_count,
      versions: root.version_count,
      uploads: root.active_upload_count
    }

  defp add_counts(counts, delta),
    do: Map.merge(counts, delta, fn _key, old, change -> old + change end)

  defp chain(previous, entry),
    do: :crypto.hash(:sha256, [previous, :erlang.term_to_binary(entry)])

  defp compare(key, nil), do: {:exists, key, :==, false}
  defp compare(key, record), do: {:mod_revision, key, :==, record.mod_revision}

  defp existing(backend, key, opts) do
    case backend.get(key, read_opts(opts)) do
      {:ok, nil} -> {:error, {:missing_metadata, key}}
      result -> result
    end
  end

  defp value(nil), do: nil
  defp value(record), do: record.value
  defp random, do: Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)
  defp backend(opts), do: Keyword.get(opts, :backend, Backend)
  defp read_opts(opts), do: Keyword.put(write_opts(opts), :consistency, :linearizable)
  defp write_opts(opts), do: Keyword.take(opts, [:engine, :timeout, :barrier])

  defp build_opts(state, opts),
    do:
      opts
      |> Keyword.put(:index_build_token, state.build.value.owner_token)
      |> Keyword.put(:replication_factor, state.build.value.replication_factor)

  defp target_rf(opts) do
    configured =
      case Keyword.get(opts, :sources, []) do
        [%ExStorageService.Context{config: config} | _] -> config.replication_factor
        _ -> 1
      end

    requested = Keyword.get(opts, :replication_factor, configured)

    if is_integer(requested) and requested >= configured,
      do: {:ok, requested},
      else: {:error, :invalid_replication_factor}
  end

  defp root_field(:current), do: :current_root
  defp root_field(:versions), do: :versions_root
  defp root_field(:uploads), do: :uploads_root

  defp report(build),
    do:
      Map.take(build, [
        :bucket_generation,
        :epoch,
        :owner_token,
        :phase,
        :source_manifest,
        :source_manifest_digest,
        :source_manifest_bytes,
        :offset,
        :processed,
        :build_digest,
        :validation_offset,
        :validation_digest,
        :validation_counts,
        :tree_counts,
        :last_error
      ])

  defp status_owned(bucket, token, opts) do
    with {:ok, state} <- state(bucket, token, opts), do: {:ok, report(state.build.value)}
  end

  defp summary(bucket, metadata),
    do: %{
      key: metadata.key,
      version_id: metadata.version_id,
      metadata_key: Keys.object_version(bucket, metadata.key, metadata.version_id),
      etag: Map.get(metadata, :etag),
      size: Map.get(metadata, :size, 0),
      last_modified: Map.get(metadata, :created_at, Map.get(metadata, :updated_at, "")),
      delete_marker:
        Map.get(metadata, :delete_marker, Map.get(metadata, :is_delete_marker, false))
    }

  defp upload(bucket, id, opts) do
    case backend(opts).get(Keys.multipart_upload(id), read_opts(opts)) do
      {:ok, nil} -> existing(backend(opts), "mpu:#{bucket}:#{id}", opts)
      result -> result
    end
  end

  defp part_upload(key, bucket) do
    prefix = "mpu_part:#{bucket}:"

    if String.starts_with?(key, prefix) do
      id =
        key
        |> String.replace_prefix(prefix, "")
        |> String.split(":")
        |> Enum.drop(-1)
        |> Enum.join(":")

      {:ok, id}
    else
      case String.split(String.replace_prefix(key, "ess:v2:multipart_part:", ""), ":", parts: 2) do
        [encoded, _] -> Keys.decode_component(encoded)
        _ -> {:error, :invalid_part_key}
      end
    end
  end
end
