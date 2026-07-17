defmodule ExStorageService.InstanceConfigTest do
  use ExUnit.Case, async: true

  alias ExStorageService.InstanceConfig

  test "current standalone defaults are valid" do
    assert {:ok, config} = InstanceConfig.new([])
    assert config.mode == :standalone
    assert config.replication_factor == 1
    assert config.write_quorum == 1
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
end
