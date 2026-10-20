defmodule ExStorageService.Metadata.ListIndex.BackfillTest do
  use ExUnit.Case, async: true
  alias ExStorageService.Metadata.{Keys, ListIndex, ObjectCommit}
  alias ExStorageService.Metadata.ListIndex.{Backfill, Manifest, Multipart}

  defmodule ExactBackend do
    def get(key, opts), do: Agent.get(opts[:engine], &{:ok, &1.records[key]})

    def get_all(opts) do
      Agent.get_and_update(opts[:engine], fn state ->
        {{:ok, Map.new(state.records, fn {key, record} -> {key, record.value} end)},
         %{state | exports: state.exports + 1}}
      end)
    end

    def transaction(spec, opts) do
      before_transaction =
        Agent.get_and_update(opts[:engine], fn state ->
          {state.before_transaction, %{state | before_transaction: nil}}
        end)

      if before_transaction, do: before_transaction.(spec)

      Agent.get_and_update(opts[:engine], fn state ->
        id = opts[:idempotency_key]

        if id && Map.has_key?(state.results, id) do
          {state.results[id], state}
        else
          success = not state.fail_next and Enum.all?(spec.compare, &matches?(&1, state.records))
          revision = state.revision + 1

          records =
            if success do
              Enum.reduce(spec.success, state.records, fn
                {:put, key, value, _}, records ->
                  Map.put(records, key, %{value: value, mod_revision: revision})

                {:delete, {:key, key}, _}, records ->
                  Map.delete(records, key)
              end)
            else
              state.records
            end

          result = {:ok, %{succeeded: success}}
          response = if state.lose_next and success, do: {:error, :timeout}, else: result

          {response,
           %{
             state
             | records: records,
               revision: revision,
               results: Map.put(state.results, id, result),
               fail_next: false,
               lose_next: false,
               transactions: state.transactions + 1
           }}
        end
      end)
    end

    def resolve_transaction(id, opts),
      do: Agent.get(opts[:engine], &Map.get(&1.results, id, {:error, :not_found}))

    def resolve_operation(key, opts), do: get(key, opts)

    defp matches?({:exists, key, :==, expected}, records),
      do: Map.has_key?(records, key) == expected

    defp matches?({:mod_revision, key, :==, revision}, records),
      do: Map.get(records, key, %{mod_revision: 0}).mod_revision == revision
  end

  defmodule BackupProof do
    def assert_quiesced(engine) do
      if Agent.get(engine, & &1.quiesced), do: :ok, else: {:error, :not_quiesced}
    end

    def verify("verified-backup", _opts), do: {:ok, %{backup_id: "unit-proof"}}
    def verify(_, _opts), do: {:error, :invalid_backup}
  end

  setup do
    generation = "backfill-generation"

    records = %{
      "bucket:bucket" => %{value: %{name: "bucket", generation: generation}, mod_revision: 1}
    }

    {:ok, engine} =
      Agent.start_link(fn ->
        %{
          records: records,
          revision: 1,
          results: %{},
          transactions: 0,
          exports: 0,
          fail_next: false,
          lose_next: false,
          before_transaction: nil,
          quiesced: true
        }
      end)

    directory =
      Path.join(System.tmp_dir!(), "ess-index-backfill-#{System.unique_integer([:positive])}")

    File.mkdir!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)

    opts = [
      backend: ExactBackend,
      engine: engine,
      backup_module: BackupProof,
      sources: [engine],
      backup: "verified-backup",
      local_blob_probe: fn _hash, _size ->
        {:ok, %{node_id: "fixture-node", node_generation: 1, verified_at: "2020-01-01"}}
      end
    ]

    %{engine: engine, opts: opts, path: Path.join(directory, "manifest"), generation: generation}
  end

  test "quiescence and verified backup are mandatory before a gate is installed", context do
    assert {:error, :verified_backup_and_quiesced_sources_required} =
             Backfill.begin("bucket", backend: ExactBackend, engine: context.engine)

    Agent.update(context.engine, &%{&1 | quiesced: false})
    assert {:error, :not_quiesced} = Backfill.begin("bucket", context.opts)
    Agent.update(context.engine, &%{&1 | quiesced: true})

    assert {:error, :invalid_backup} =
             Backfill.begin("bucket", Keyword.put(context.opts, :backup, "invalid"))

    assert {:ok, nil} =
             ExactBackend.get(ListIndex.root_key("bucket", context.generation), context.opts)
  end

  test "all unindexed legacy history is normalized in bounded resumable publications", context do
    for n <- 1..300 do
      id = "version-#{n}"
      put(context, "obj_ver:bucket:dir:雪:#{id}", %{etag: id, size: n, created_at: "2020-01-01"})
    end

    sources = Agent.get(context.engine, & &1.records)
    {:ok, started} = Backfill.begin("bucket", context.opts)
    token = started.owner_token

    assert {:error, :listing_index_unavailable} =
             ListIndex.page("bucket", :current, <<>>, context.opts)

    assert {:ok, _} = Backfill.capture("bucket", token, context.path, context.opts)

    assert {:ok, %{processed: 1}} =
             Backfill.resume(
               "bucket",
               token,
               context.path,
               Keyword.put(context.opts, :batch_size, 1)
             )

    complete(context, token)
    assert {:ok, %{entries: entries}} = ListIndex.page("bucket", :versions, <<>>, context.opts)
    assert length(entries) == 300
    assert Enum.count(entries, fn {_, value} -> value.is_latest end) == 1

    assert {:ok, %{entries: [{"dir:雪", _}]}} =
             ListIndex.page("bucket", :current, <<>>, context.opts)

    assert {:ok, %{phase: :ready, processed: 301}} = Backfill.status("bucket", context.opts)

    for {key, value} <- sources, String.starts_with?(key, "obj_ver:") do
      assert {:ok, ^value} = ExactBackend.get(key, context.opts)
    end

    assert Agent.get(context.engine, & &1.exports) == 1
  end

  test "v2 deletion barriers suppress retained legacy values and remain durable", context do
    put(context, "obj:bucket:deleted", %{etag: "retained", size: 1})
    put(context, "obj_ver:bucket:deleted:old", %{etag: "old", size: 1})
    barrier = %{schema: 2, bucket: "bucket", key: "deleted", version_id: nil, delete_marker: true}
    put(context, Keys.object_head("bucket", "deleted"), barrier)
    token = capture(context)
    complete(context, token)
    assert {:ok, %{entries: []}} = ListIndex.page("bucket", :current, <<>>, context.opts)
    assert {:ok, %{entries: []}} = ListIndex.page("bucket", :versions, <<>>, context.opts)

    assert {:ok, %{value: ^barrier}} =
             ExactBackend.get(Keys.object_head("bucket", "deleted"), context.opts)

    assert {:ok, %{value: %{etag: "retained"}}} =
             ExactBackend.get("obj:bucket:deleted", context.opts)
  end

  test "both multipart namespaces are merged and terminal upload records remain", context do
    upload = %{
      bucket: "bucket",
      key: "upload:key",
      upload_id: "one",
      status: :completing,
      created_at: "2020-01-01",
      attributes: %{}
    }

    put(context, Keys.multipart_upload("one"), upload)
    put(context, "mpu:bucket:one", upload)
    put(context, Keys.multipart_part("one", 1), part(1))
    put(context, "mpu_part:bucket:one:1", part(1))
    put(context, "mpu_part:bucket:one:2", part(2))
    put(context, "mpu:bucket:done", %{upload | upload_id: "done", status: :completed})
    put(context, "mpu_part:bucket:done:3", part(3))
    token = capture(context)
    complete(context, token)

    assert {:ok, %{uploads: [%{upload_id: "one"}]}} =
             Multipart.uploads_page("bucket", context.opts)

    assert {:ok, %{parts: parts}} = Multipart.parts_page("bucket", "one", context.opts)
    assert Enum.map(parts, & &1.part_number) == [1, 2]

    assert {:ok, %{parts: [%{part_number: 3}]}} =
             Multipart.parts_inventory_page("bucket", "done", context.opts)

    assert {:error, :not_found} = Multipart.parts_page("bucket", "done", context.opts)

    {:ok, previous} = ListIndex.status("bucket", context.opts)
    rebuild = %{context | path: context.path <> "-rebuild"}
    next_token = capture(rebuild)
    complete(rebuild, next_token)
    assert {:ok, %{parts: parts}} = Multipart.parts_page("bucket", "one", context.opts)
    assert Enum.map(parts, & &1.part_number) == [1, 2]
    {:ok, current} = ListIndex.status("bucket", context.opts)
    assert current.epoch != previous.epoch
  end

  test "a version and checkpoint cannot publish without verified blob evidence", context do
    put(context, "obj:bucket:key", %{etag: "one", size: 1})
    token = capture(context)
    opts = Keyword.put(context.opts, :local_blob_probe, fn _hash, _size -> :missing end)
    # The first action only asserts the source. The next action publishes its
    # normalized version together with CAS descriptor/location evidence.
    assert {:ok, %{processed: 1}} =
             Backfill.resume("bucket", token, context.path, Keyword.put(opts, :batch_size, 1))

    assert {:error, {:insufficient_verified_blob_sources, _, 1}} =
             Backfill.resume("bucket", token, context.path, opts)

    assert {:ok, nil} = ExactBackend.get(Keys.object_version("bucket", "key", "null"), opts)
    assert {:ok, %{processed: 1}} = Backfill.status("bucket", opts)
    complete(context, token)
  end

  test "real CAS probes reject missing or corrupt replicas and preserve descriptor RF", context do
    alias ExStorageService.{Context, InstanceConfig}
    alias ExStorageService.BlobStore.LocalCAS
    alias ExStorageService.Metadata.Migration
    data = "real checksum-verified migration bytes"
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)

    contexts =
      for id <- ["source-a", "source-b"] do
        {:ok, config} =
          InstanceConfig.new(
            data_root: Path.join(Path.dirname(context.path), id),
            node_id: id,
            auto_start: false,
            web_enabled: false,
            public_s3_enabled: false
          )

        Context.new(config)
      end

    [first, second] = contexts
    first_path = LocalCAS.blob_path(hash, root: first.blob_root)
    second_path = LocalCAS.blob_path(hash, root: second.blob_root)
    File.mkdir_p!(Path.dirname(first_path))
    File.write!(first_path, data)

    version = %{
      content_hash: hash,
      size: byte_size(data),
      created_at: "2020-01-01",
      delete_marker: false
    }

    opts =
      context.opts
      |> Keyword.delete(:local_blob_probe)
      |> Keyword.put(:context, first)
      |> Keyword.put(:sources, contexts)
      |> Keyword.put(:replication_factor, 2)

    assert {:error, {:insufficient_verified_blob_sources, ^hash, 2}} =
             Migration.prepare_index_version(version, opts)

    File.mkdir_p!(Path.dirname(second_path))
    File.write!(second_path, String.duplicate("!", byte_size(data)))
    assert {:error, {:local_blob_corrupt, ^hash}} = Migration.prepare_index_version(version, opts)
    File.write!(second_path, data)

    put(context, Keys.blob(hash), %{
      schema: 2,
      hash: hash,
      size: byte_size(data),
      algorithm: :sha256,
      desired_replication_factor: 1,
      retained: :metadata
    })

    assert {:ok, planned} = Migration.prepare_index_version(version, opts)

    descriptor =
      Enum.find_value(planned.success, fn
        {:put, key, value, _} when key == "ess:v2:blob:" <> hash -> value
        _ -> nil
      end)

    assert descriptor.desired_replication_factor == 2
    assert descriptor.retained == :metadata

    assert {:ok, %{succeeded: true}} =
             ExactBackend.transaction(Map.put(planned, :failure, []), opts)

    assert {:ok, _} =
             Migration.prepare_index_version(version, Keyword.put(opts, :index_validate, true))

    assert {:ok, plan} =
             Migration.prepare_index_version(version, Keyword.put(opts, :replication_factor, 1))

    assert Enum.any?(plan.success, fn
             {:put, key, %{desired_replication_factor: 2}, _} -> key == Keys.blob(hash)
             _ -> false
           end)
  end

  test "takeover fences the old owner and resumes the same manifest checkpoint", context do
    put(context, "obj:bucket:key", %{etag: "one", size: 1})
    token = capture(context)

    assert {:ok, %{processed: 1}} =
             Backfill.resume(
               "bucket",
               token,
               context.path,
               Keyword.put(context.opts, :batch_size, 1)
             )

    assert {:ok, taken} = Backfill.takeover("bucket", token, context.opts)
    assert taken.owner_token != token

    assert {:error, :obsolete_build_owner} =
             Backfill.resume("bucket", token, context.path, context.opts)

    assert {:error, :obsolete_build_owner} = Backfill.cutover("bucket", token, context.opts)
    complete(context, taken.owner_token)

    assert {:ok, %{entries: [{"key", _}]}} =
             ListIndex.page("bucket", :current, <<>>, context.opts)
  end

  test "a compare failure leaves both publication and checkpoint untouched", context do
    put(context, "obj:bucket:key", %{etag: "one", size: 1})
    token = capture(context)
    before = Agent.get(context.engine, & &1.records)
    Agent.update(context.engine, &%{&1 | fail_next: true})

    assert {:error, :build_conflict} =
             Backfill.resume(
               "bucket",
               token,
               context.path,
               Keyword.put(context.opts, :batch_size, 1)
             )

    assert Agent.get(context.engine, & &1.records) == before
    complete(context, token)
  end

  test "a permitted shared-blob write conflicts safely and resumes the same checkpoint",
       context do
    hash = Base.encode16(:crypto.hash(:sha256, "shared backfill bytes"), case: :lower)
    metadata = %{content_hash: hash, etag: "shared", size: 21, created_at: "2020-01-01"}
    put(context, "bucket:other", %{name: "other", generation: "other-generation"})

    put(
      context,
      ListIndex.root_key("other", "other-generation"),
      ListIndex.empty("other-generation")
    )

    opts = Keyword.put(context.opts, :bucket_versioning, true)
    assert {:ok, _} = ObjectCommit.put("other", "initial", metadata, opts)
    put(context, "obj:bucket:key", metadata)
    token = capture(context)
    batch = Keyword.put(context.opts, :batch_size, 1)
    assert {:ok, %{processed: 1}} = Backfill.resume("bucket", token, context.path, batch)
    assert {:ok, checkpoint} = Backfill.status("bucket", context.opts)
    assert {:ok, root} = ExactBackend.get(ListIndex.root_key("bucket", context.generation), batch)
    assert {:ok, blob_before} = ExactBackend.get(Keys.blob(hash), context.opts)

    owner = self()

    Agent.update(context.engine, fn state ->
      %{
        state
        | before_transaction: fn spec ->
            assert {:mod_revision, Keys.blob(hash), :==, blob_before.mod_revision} in spec.compare
            assert {:ok, _} = ObjectCommit.put("other", "concurrent", metadata, opts)
            send(owner, :permitted_shared_blob_committed)
          end
      }
    end)

    assert {:error, :build_conflict} = Backfill.resume("bucket", token, context.path, batch)
    assert_received :permitted_shared_blob_committed
    assert {:ok, ^checkpoint} = Backfill.status("bucket", context.opts)

    assert {:ok, ^root} =
             ExactBackend.get(ListIndex.root_key("bucket", context.generation), batch)

    assert {:ok, nil} = ExactBackend.get(Keys.object_version("bucket", "key", "null"), batch)
    assert {:ok, blob_after} = ExactBackend.get(Keys.blob(hash), context.opts)
    assert blob_after.value == blob_before.value
    assert blob_after.mod_revision > blob_before.mod_revision
    assert {:ok, %{content_hash: ^hash}} = ObjectCommit.get_head("other", "concurrent", opts)

    # An explicit attempt re-reads the same fenced manifest/checkpoint and blob.
    assert {:ok, %{processed: 2}} = Backfill.resume("bucket", token, context.path, batch)
    complete(context, token)
    assert {:ok, %{entries: [{"key", _}]}} = ListIndex.page("bucket", :current, <<>>, batch)
  end

  test "lost committed responses resolve the original atomic checkpoint", context do
    put(context, "obj:bucket:key", %{etag: "one", size: 1})
    token = capture(context)
    Agent.update(context.engine, &%{&1 | lose_next: true})

    assert {:ok, %{processed: 1}} =
             Backfill.resume(
               "bucket",
               token,
               context.path,
               Keyword.put(context.opts, :batch_size, 1)
             )

    complete(context, token)
    assert {:ok, %{version_count: 1, visible_count: 1}} = ListIndex.status("bucket", context.opts)
  end

  test "changed source revisions and changed manifests fail closed", context do
    put(context, "obj:bucket:key", %{etag: "one", size: 1})
    token = capture(context)
    put(context, "obj:bucket:key", %{etag: "one", size: 1})

    assert {:error, {:source_changed, "obj:bucket:key"}} =
             Backfill.resume("bucket", token, context.path, context.opts)

    assert {:error, :index_validation_incomplete} =
             Backfill.cutover("bucket", token, context.opts)

    assert {:error, :listing_index_unavailable} =
             ListIndex.page("bucket", :current, <<>>, context.opts)

    File.write!(context.path, "changed", [:append])

    assert {:error, :manifest_identity_changed} =
             Backfill.resume("bucket", token, context.path, context.opts)
  end

  test "conflicting same-number parts stop capture without publishing readiness", context do
    put(context, "mpu:bucket:one", %{
      bucket: "bucket",
      key: "key",
      upload_id: "one",
      created_at: "2020-01-01",
      status: :initiated
    })

    put(context, Keys.multipart_part("one", 1), part(1))
    put(context, "mpu_part:bucket:one:1", %{part(1) | etag: "different"})
    assert {:ok, started} = Backfill.begin("bucket", context.opts)

    assert {:error, {_, :conflicting_part}} =
             Backfill.capture("bucket", started.owner_token, context.path, context.opts)

    assert {:ok, %{phase: :capturing}} = Backfill.status("bucket", context.opts)

    assert {:error, :listing_index_unavailable} =
             ListIndex.page("bucket", :current, <<>>, context.opts)
  end

  test "stable corrupt tree nodes prevent validation and cutover", context do
    put(context, "obj:bucket:key", %{etag: "one", size: 1})
    token = capture(context)
    advance(context, token, :build)
    {:ok, root} = ListIndex.status("bucket", context.opts)

    key =
      ListIndex.namespace("bucket", root, :versions) <>
        Base.url_encode64(root.versions_root, padding: false)

    put(context, key, %{schema: 1, label: "bad", terminal: nil, children: []})
    assert {:error, _} = Backfill.validate("bucket", token, context.path, context.opts)

    assert {:error, :index_validation_incomplete} =
             Backfill.cutover("bucket", token, context.opts)
  end

  test "framed manifests are sorted externally and reject frame corruption", context do
    path = context.path
    entries = for n <- 1500..1//-1, do: {n, String.duplicate("x", 1024)}
    assert {:ok, %{digest: digest}} = Manifest.create(path, entries)
    assert {:ok, ^digest, _} = Manifest.digest(path)
    assert Enum.map(Manifest.stream(path), &elem(&1, 0)) == Enum.to_list(1..1500)
    assert {:ok, %{entries: first, offset: offset}} = Manifest.batch(path, 0, 1)
    assert [{1, _}] = first
    assert {:ok, %{entries: [{2, _}]}} = Manifest.batch(path, offset, 1)
    {:ok, file} = File.open(path, [:read, :write, :binary])
    :file.position(file, offset - 1)
    IO.binwrite(file, <<0>>)
    File.close(file)

    assert catch_throw(Enum.to_list(Manifest.stream(path))) ==
             {:manifest_error, :manifest_checksum_mismatch}
  end

  defp part(number),
    do: %{
      part_number: number,
      etag: "etag-#{number}",
      size: number,
      hash: "hash-#{number}",
      uploaded_at: "2020-01-01"
    }

  defp put(context, key, value) do
    value =
      if String.starts_with?(key, ["obj:", "obj_ver:"]) and is_map(value) do
        Map.put_new(value, :content_hash, Base.encode16(:crypto.hash(:sha256, key), case: :lower))
      else
        value
      end

    Agent.update(context.engine, fn state ->
      revision = state.revision + 1

      %{
        state
        | revision: revision,
          records: Map.put(state.records, key, %{value: value, mod_revision: revision})
      }
    end)
  end

  defp capture(context) do
    assert {:ok, started} = Backfill.begin("bucket", context.opts)
    assert {:ok, _} = Backfill.capture("bucket", started.owner_token, context.path, context.opts)
    started.owner_token
  end

  defp complete(context, token) do
    advance(context, token, :build)
    advance(context, token, :validate)
    assert {:ok, %{phase: :ready}} = Backfill.cutover("bucket", token, context.opts)
  end

  defp advance(context, token, phase, attempts \\ 1000)
  defp advance(_context, _token, _phase, 0), do: flunk("bounded build did not finish")

  defp advance(context, token, phase, attempts) do
    result =
      case phase do
        :build -> Backfill.resume("bucket", token, context.path, context.opts)
        :validate -> Backfill.validate("bucket", token, context.path, context.opts)
      end

    assert {:ok, status} = result

    if (phase == :build and status.phase == :building) or
         (phase == :validate and status.phase != :validated) do
      advance(context, token, phase, attempts - 1)
    end
  end
end
