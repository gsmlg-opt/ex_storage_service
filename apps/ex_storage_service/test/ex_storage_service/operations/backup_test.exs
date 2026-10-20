defmodule ExStorageService.Operations.BackupTest do
  use ExUnit.Case, async: true

  alias ExStorageService.{Context, InstanceConfig}
  alias ExStorageService.Operations.{Backup, BackupManifest}

  @key "separately-provisioned-test-master-key"

  defmodule LegacyRestoreEngine do
    def status(_opts), do: {:ok, %{command_version: 0}}
  end

  defmodule UnavailableRestoreEngine do
    def status(_opts), do: {:error, :quorum_unavailable}
  end

  defmodule UnknownRestoreEngine do
    def status(_opts), do: {:ok, %{}}
  end

  setup do
    root = Path.join(System.tmp_dir!(), "ess-backup-unit-" <> BackupManifest.random_id())
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "verifies declared deterministic ETF snapshots through the backup bundle", %{root: root} do
    {bundle, manifest} = bundle(root)
    path = Path.join(bundle, manifest["metadata"])
    %{data: snapshot} = :erlang.binary_to_term(File.read!(path))
    snapshot = %{snapshot | indexes: checksum_snapshot().indexes}
    encoded = :erlang.term_to_binary(snapshot, [:deterministic])
    refute encoded == :erlang.term_to_binary(snapshot)

    backup = %{
      metadata: %{
        checksum_scheme: :sha256_deterministic_etf_v1,
        checksum: :crypto.hash(:sha256, encoded)
      },
      data: snapshot
    }

    File.write!(path, :erlang.term_to_binary(backup, [:compressed]))

    manifest =
      Map.update!(manifest, "artifacts", fn artifacts ->
        Enum.map(artifacts, fn artifact ->
          if artifact["path"] == manifest["metadata"],
            do: file_artifact(bundle, path, "metadata"),
            else: artifact
        end)
      end)

    rewrite_manifest(bundle, manifest)
    assert {:ok, ^snapshot, _entries} = BackupManifest.read_snapshot(path)
    assert {:ok, %{referenced_blobs: 1, artifacts: 2}} = Backup.verify(bundle, master_key: @key)
  end

  test "legacy snapshots retain their original checksum validation", %{root: root} do
    snapshot = checksum_snapshot()
    path = Path.join(root, "legacy.backup")
    checksum = :crypto.hash(:sha256, :erlang.term_to_binary(snapshot))
    backup = %{metadata: %{checksum: checksum}, data: snapshot}
    File.write!(path, :erlang.term_to_binary(backup, [:compressed]))

    assert {:ok, ^snapshot, []} = BackupManifest.read_snapshot(path)
  end

  test "corrupt checksums and legacy hashes declared as deterministic are rejected", %{root: root} do
    snapshot = checksum_snapshot()
    legacy_checksum = :crypto.hash(:sha256, :erlang.term_to_binary(snapshot))

    deterministic_checksum =
      :crypto.hash(:sha256, :erlang.term_to_binary(snapshot, [:deterministic]))

    refute legacy_checksum == deterministic_checksum

    metadata = [
      %{checksum: <<0::256>>},
      %{checksum_scheme: :sha256_deterministic_etf_v1, checksum: <<0::256>>},
      %{checksum_scheme: :sha256_deterministic_etf_v1, checksum: legacy_checksum},
      %{checksum: deterministic_checksum}
    ]

    for {metadata, index} <- Enum.with_index(metadata) do
      path = Path.join(root, "invalid-#{index}.backup")

      File.write!(
        path,
        :erlang.term_to_binary(%{metadata: metadata, data: snapshot}, [:compressed])
      )

      assert {:error, :invalid_metadata_snapshot} = BackupManifest.read_snapshot(path)
    end
  end

  test "unknown checksum schemes never fall back to legacy validation", %{root: root} do
    snapshot = checksum_snapshot()
    checksum = :crypto.hash(:sha256, :erlang.term_to_binary(snapshot))

    for scheme <- [:future_checksum_scheme, nil] do
      path = Path.join(root, "unknown-#{inspect(scheme)}.backup")
      backup = %{metadata: %{checksum_scheme: scheme, checksum: checksum}, data: snapshot}
      File.write!(path, :erlang.term_to_binary(backup, [:compressed]))
      assert {:error, :invalid_metadata_snapshot} = BackupManifest.read_snapshot(path)
    end
  end

  test "verifies metadata and every referenced historical/multipart body", %{root: root} do
    {bundle, manifest} = bundle(root)
    assert {:ok, %{referenced_blobs: 1, artifacts: 2}} = Backup.verify(bundle, master_key: @key)

    assert {:ok, %{upstream_only_bodies_included: false}} =
             Backup.verify(bundle, master_key: @key)

    body = Enum.find(manifest["artifacts"], &(&1["kind"] == "blob"))
    File.rm!(Path.join(bundle, body["path"]))
    assert {:error, :enoent} = Backup.verify(bundle, master_key: @key)
  end

  test "rejects a checksum-valid inventory that silently omits a referenced blob", %{root: root} do
    {bundle, manifest} = bundle(root)

    rewrite_manifest(
      bundle,
      Map.update!(manifest, "artifacts", &Enum.filter(&1, fn a -> a["kind"] == "metadata" end))
    )

    assert {:error, {:referenced_blob_missing, _hash}} = Backup.verify(bundle, master_key: @key)
  end

  test "refuses absent and wrong master keys before trusting encrypted records", %{root: root} do
    {bundle, _} = bundle(root)
    assert {:error, :master_key_required} = Backup.verify(bundle, master_key: nil)
    assert {:error, :master_key_mismatch} = Backup.verify(bundle, master_key: "wrong")
  end

  test "rejects changed metadata, changed bodies, duplicate and traversal artifacts", %{
    root: root
  } do
    {bundle, manifest} = bundle(root)
    metadata = Path.join(bundle, manifest["metadata"])
    original = File.read!(metadata)
    File.write!(metadata, "corrupt")
    assert {:error, :artifact_checksum_mismatch} = Backup.verify(bundle, master_key: @key)
    File.write!(metadata, original)

    [first | _] = manifest["artifacts"]
    rewrite_manifest(bundle, Map.update!(manifest, "artifacts", &[first | &1]))
    assert {:error, :duplicate_artifact} = Backup.verify(bundle, master_key: @key)

    escaped = Map.put(first, "path", "../outside")
    rewrite_manifest(bundle, Map.put(manifest, "artifacts", [escaped]))
    assert {:error, :unsafe_artifact_path} = Backup.verify(bundle, master_key: @key)
  end

  test "symlink artifacts fail even when the target has expected bytes", %{root: root} do
    {bundle, manifest} = bundle(root)
    body = Enum.find(manifest["artifacts"], &(&1["kind"] == "blob"))
    path = Path.join(bundle, body["path"])
    outside = Path.join(root, "outside-body")
    File.rename!(path, outside)
    File.ln_s!(outside, path)
    assert {:error, :symlink_artifact} = Backup.verify(bundle, master_key: @key)
  end

  test "fresh recovery roots are persistently quarantined across config reconstruction", %{
    root: root
  } do
    {bundle, _} = bundle(root)
    context = context(Path.join(root, "target"))
    assert {:ok, %{status: :quarantined}} = Backup.prepare(context, bundle, master_key: @key)
    assert Backup.quarantined?(context.config)
    assert :ok = Backup.startup_guard(context.config)

    assert {:error, :recovery_quarantined} =
             Backup.startup_guard(%{context.config | auto_start: true})

    assert {:error, :recovery_quarantined} =
             Backup.startup_guard(%{context.config | public_s3_enabled: true})

    assert {:error, :recovery_quarantined} =
             Backup.startup_guard(%{context.config | web_enabled: true})

    assert {:error, :recovery_quarantined} =
             Backup.startup_guard(%{context.config | cluster_data_plane_enabled: true})

    assert {:error, :restore_destination_not_empty} =
             Backup.prepare(context, bundle, master_key: @key)
  end

  test "legacy command emission refuses recovery before changing the prepared target", %{
    root: root
  } do
    {bundle, _} = bundle(root)
    context = context(Path.join(root, "target"))
    assert {:ok, _} = Backup.prepare(context, bundle, master_key: @key)
    marker = Path.join(context.data_root, ".ess-recovery.json")
    prepared = File.read!(marker)

    for engine <- [LegacyRestoreEngine, UnknownRestoreEngine] do
      assert {:error, :restore_requires_command_version_1} =
               Backup.restore(context, bundle, engine: engine, master_key: @key)

      assert File.read!(marker) == prepared
      refute File.exists?(context.metadata_root)
      refute File.exists?(context.blob_root)
    end
  end

  test "recovery propagates command-version status failures without changing its marker", %{
    root: root
  } do
    {bundle, _} = bundle(root)
    context = context(Path.join(root, "target"))
    assert {:ok, _} = Backup.prepare(context, bundle, master_key: @key)
    marker = Path.join(context.data_root, ".ess-recovery.json")
    prepared = File.read!(marker)

    assert {:error, :quorum_unavailable} =
             Backup.restore(context, bundle, engine: UnavailableRestoreEngine, master_key: @key)

    assert File.read!(marker) == prepared
  end

  test "nonempty destination is preserved and generation reuse is rejected", %{root: root} do
    {bundle, _} = bundle(root)
    context = context(Path.join(root, "target"))
    File.mkdir_p!(context.blob_root)
    keep = Path.join(context.blob_root, "keep")
    File.write!(keep, "unrelated")

    assert {:error, :restore_destination_not_empty} =
             Backup.prepare(context, bundle, master_key: @key)

    assert File.read!(keep) == "unrelated"

    same_generation = %{context | config: %{context.config | node_generation: 1}}

    assert {:error, :restore_generation_not_advanced} =
             Backup.prepare(same_generation, bundle, master_key: @key)
  end

  test "invalid or relocated recovery markers fail closed", %{root: root} do
    {bundle, _} = bundle(root)
    context = context(Path.join(root, "target"))
    assert {:ok, _} = Backup.prepare(context, bundle, master_key: @key)

    assert {:error, :invalid_recovery_marker} =
             Backup.startup_guard(%{context.config | node_generation: 3})

    File.write!(Path.join(context.data_root, ".ess-recovery.json"), "{")
    assert {:error, {:invalid_recovery_marker, _}} = Backup.startup_guard(context.config)
    assert Backup.quarantined?(context.config)
  end

  test "validation config disables every worker explicitly", %{root: root} do
    safe = context(root).config |> Backup.validation_config()
    assert Enum.all?(safe.workers, fn {_, enabled} -> enabled == false end)
    refute safe.auto_start
    refute safe.public_s3_enabled
    refute safe.web_enabled
    refute safe.cluster_data_plane_enabled

    assert {:error, :explicit_complete_worker_policy_required} =
             Backup.activate(Context.new(safe), root)
  end

  test "packed historical body requires pack, sidecar and exact verified segment", %{root: root} do
    {bundle, manifest} = bundle(root, packed: true)
    assert {:ok, _} = Backup.verify(bundle, master_key: @key)
    sidecar = Enum.find(manifest["artifacts"], &(&1["kind"] == "pack-index"))
    index_path = Path.join(bundle, sidecar["path"])
    index = JSON.decode!(File.read!(index_path))
    bad_index = Map.put(index, "entries", [])
    File.write!(index_path, JSON.encode!(bad_index))

    updated =
      Enum.map(manifest["artifacts"], fn a ->
        if a == sidecar, do: file_artifact(bundle, index_path, "pack-index"), else: a
      end)

    rewrite_manifest(bundle, Map.put(manifest, "artifacts", updated))
    assert {:error, {:invalid_packed_blob, _}} = Backup.verify(bundle, master_key: @key)
  end

  test "bounded copy does not overwrite an existing file and produces independent bytes", %{
    root: root
  } do
    source = Path.join(root, "source")
    target = Path.join(root, "target")
    data = :binary.copy(<<0, 255, 32>>, 200_000)
    File.write!(source, data)
    assert :ok = BackupManifest.copy(source, target)
    assert {:ok, digest, size} = BackupManifest.digest(target)
    assert digest == hash(data)
    assert size == byte_size(data)
    File.write!(source, "changed")
    assert File.read!(target) == data
    assert {:error, :eexist} = BackupManifest.copy(source, target)
  end

  test "interrupted restore-owned partial copy is replaced before atomic publication", %{
    root: root
  } do
    source = Path.join(root, "source")
    destination = Path.join(root, "destination")
    bytes = :binary.copy("recover", 50_000)
    digest = hash(bytes)
    File.write!(source, bytes)
    File.write!(destination <> ".ess-restore-" <> digest, "interrupted")
    assert :ok = BackupManifest.install(source, destination, digest, byte_size(bytes))
    assert File.read!(destination) == bytes
    assert {:error, :enoent} = File.lstat(destination <> ".ess-restore-" <> digest)

    assert {:error, :restore_destination_conflict} =
             BackupManifest.install(source, destination, digest, byte_size(bytes))

    assert File.read!(destination) == bytes
  end

  test "listing backfill must be drained and cluster packing stays unsupported" do
    key = "ess:v2:list_index:bucket:generation:root"

    for state <- [:building, :validating, :failed] do
      assert {:error, :listing_index_not_quiesced} =
               BackupManifest.validate_boundary([{key, %{state: state}}], :standalone)
    end

    assert :ok = BackupManifest.validate_boundary([{key, %{state: :ready}}], :cluster)

    assert {:error, :packed_cluster_restore_unsupported} =
             BackupManifest.validate_boundary([{"blob:sha256:hash", %{state: :packed}}], :cluster)
  end

  test "job references retain pinned bodies and resolve descriptor sizes" do
    content_hash = hash("job-body")

    entries = [
      {"ess:v2:blob:" <> content_hash, %{size: 8}},
      {"ess:v2:job:repair", %{state: :pending, payload: %{hash: content_hash}}},
      {"ess:v2:job:put",
       %{state: :running, payload: %{object: %{content_hash: content_hash, size: 8}}}}
    ]

    assert {:ok, %{^content_hash => 8}} = BackupManifest.references(entries)
  end

  defp context(root) do
    {:ok, config} =
      InstanceConfig.new(
        data_root: root,
        node_id: "source",
        node_generation: 2,
        auto_start: false,
        public_s3_enabled: false,
        web_enabled: false
      )

    Context.new(config)
  end

  defp checksum_snapshot do
    %{
      version: 2,
      kv_data: [],
      indexes: Map.new(1..64, &{"checksum-field-#{&1}", &1})
    }
  end

  defp bundle(root, opts \\ []) do
    directory = Path.join(root, "bundle")
    File.mkdir_p!(directory)
    body = <<0, 255, 1, 128, 32, 13, 10>>
    content_hash = hash(body)
    size = byte_size(body)
    metadata = Path.join(directory, "metadata.backup")

    object = %{
      bucket: "bucket",
      key: "old-version",
      version_id: "old",
      content_hash: content_hash,
      size: size
    }

    entries = [
      {"ess:v2:object_version:b:k:old", object},
      {"ess:v2:multipart_part:upload:1", %{hash: content_hash, size: size}}
    ]

    {entries, files} =
      if opts[:packed] do
        pack_bytes = "prefix" <> body <> "suffix"
        pack_hash = hash(pack_bytes)

        pack =
          Path.join([directory, "nodes", "source", "blob", "packs", "pack-#{pack_hash}.pack"])

        File.mkdir_p!(Path.dirname(pack))
        File.write!(pack, pack_bytes)

        File.write!(
          pack <> ".idx",
          JSON.encode!(%{
            format: "ess-pack-v1",
            hash: pack_hash,
            entries: [[content_hash, 6, size]]
          })
        )

        blob = %{state: :packed, size: size, pack: %{hash: pack_hash, offset: 6}}

        {[
           {"blob:sha256:" <> content_hash, blob},
           {"pack:" <> pack_hash, %{entries: [%{blob_hash: content_hash, offset: 6, size: size}]}}
           | entries
         ],
         [
           file_artifact(directory, pack, "pack"),
           file_artifact(directory, pack <> ".idx", "pack-index")
         ]}
      else
        path = Path.join(directory, "body")
        File.write!(path, body)
        {entries, [file_artifact(directory, path, "blob")]}
      end

    snapshot = %{version: 2, kv_data: entries, indexes: %{}}

    backup = %{
      metadata: %{checksum: :crypto.hash(:sha256, :erlang.term_to_binary(snapshot))},
      data: snapshot
    }

    File.write!(metadata, :erlang.term_to_binary(backup, [:compressed]))

    manifest = %{
      "format" => BackupManifest.format(),
      "backup_id" => "fixture-backup",
      "mode" => "standalone",
      "nodes" => [
        %{"id" => "source", "generation" => 1, "role" => "data", "erlang_node" => "nonode@nohost"}
      ],
      "master_key_sha256" => hash(@key),
      "metadata" => "metadata.backup",
      "consistency_point" => "quiesced-test-fixture",
      "references" => %{content_hash => size},
      "artifacts" => [file_artifact(directory, metadata, "metadata") | files]
    }

    rewrite_manifest(directory, manifest)
    {directory, manifest}
  end

  defp file_artifact(root, path, kind) do
    {:ok, digest, size} = BackupManifest.digest(path)
    %{"path" => Path.relative_to(path, root), "sha256" => digest, "size" => size, "kind" => kind}
  end

  defp rewrite_manifest(directory, manifest) do
    bytes = JSON.encode!(manifest)
    File.write!(Path.join(directory, "manifest.json"), bytes)
    File.write!(Path.join(directory, "manifest.sha256"), hash(bytes) <> "\n")
  end

  defp hash(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
