defmodule ExStorageService.InstanceConfigTest do
  use ExUnit.Case, async: true

  alias ExStorageService.InstanceConfig

  test "current standalone defaults are valid" do
    assert {:ok, config} = InstanceConfig.new([])
    assert config.mode == :standalone
    assert config.replication_factor == 1
    assert config.write_quorum == 1
    assert config.instance == :default
    assert config.auto_start
    assert config.blob_root == Path.join(config.data_root, "cas")
    assert config.tmp_root == Path.join(config.blob_root, "tmp")
    assert config.ra_root == Path.join(config.data_root, "ra")
    assert config.metadata_root == Path.join(config.data_root, "concord")
    assert config.web_enabled

    assert Enum.all?(Map.take(config.workers, [:multipart_gc, :cas_gc, :packer]), fn {
                                                                                       _worker,
                                                                                       enabled
                                                                                     } ->
             enabled
           end)

    refute config.allow_degraded_writes
    refute config.cluster_data_plane_enabled
    assert config.public_s3_enabled
  end

  test "rejects invalid replication factor and write quorum" do
    assert {:error, _} = InstanceConfig.new(replication_factor: 0)
    assert {:error, _} = InstanceConfig.new(write_quorum: 0)

    assert {:error, message} =
             InstanceConfig.new(replication_factor: 2, write_quorum: 3)

    assert message =~ "1 <= W <= RF"
  end

  test "cluster mode fails fast while the cluster data plane is disabled" do
    assert {:error, message} =
             InstanceConfig.new(
               mode: :cluster,
               replication_factor: 2,
               write_quorum: 2,
               public_s3_enabled: true,
               cluster_data_plane_enabled: false
             )

    assert message =~ "cannot expose the public S3 writer"
  end

  test "metadata-only cluster scaffolding accepts strict RF=2/W=2" do
    assert {:ok, config} =
             InstanceConfig.new(
               mode: :cluster,
               replication_factor: 2,
               write_quorum: 2,
               public_s3_enabled: false,
               cluster_data_plane_enabled: false
             )

    assert config.mode == :cluster
    assert config.replication_factor == 2
    assert config.write_quorum == 2
  end

  test "split roots override independently while data_root remains the fallback" do
    assert {:ok, fallback} = InstanceConfig.new(data_root: "/srv/ess")
    assert fallback.blob_root == "/srv/ess/cas"
    assert fallback.tmp_root == "/srv/ess/cas/tmp"
    assert fallback.ra_root == "/srv/ess/ra"
    assert fallback.metadata_root == "/srv/ess/concord"

    assert {:ok, blob_override} = InstanceConfig.new(blob_root: "/blob/ess")
    assert blob_override.tmp_root == "/blob/ess/tmp"

    assert {:ok, config} =
             InstanceConfig.new(
               data_root: "/srv/ess",
               blob_root: "/blob/ess",
               tmp_root: "/staging/ess",
               ra_root: "/raft/ess",
               metadata_root: "/metadata/ess"
             )

    assert config.data_root == "/srv/ess"
    assert config.blob_root == "/blob/ess"
    assert config.tmp_root == "/staging/ess"
    assert config.ra_root == "/raft/ess"
    assert config.metadata_root == "/metadata/ess"
  end

  test "validates embedding and worker options" do
    assert {:error, _message} = InstanceConfig.new(instance: "")
    assert {:error, _message} = InstanceConfig.new(auto_start: :yes)
    assert {:error, _message} = InstanceConfig.new(web_enabled: :yes)
    assert {:error, _message} = InstanceConfig.new(workers: [packer: :yes])
    assert {:error, _message} = InstanceConfig.new(workers: [unknown: true])
    assert {:error, _message} = InstanceConfig.new(workers: :invalid)
    assert {:error, _message} = InstanceConfig.new(workers: [:invalid])

    assert {:ok, config} =
             InstanceConfig.new(
               instance: "embedded-a",
               auto_start: false,
               workers: [
                 multipart_gc: false,
                 content_gc: false,
                 cas_gc: false,
                 packer: false,
                 lifecycle: false,
                 cross_cluster_replication: false
               ]
             )

    refute config.auto_start
    refute InstanceConfig.worker_enabled?(config, :packer)
    refute InstanceConfig.worker_enabled?(config, :cross_cluster_replication)
  end
end
