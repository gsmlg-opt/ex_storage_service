defmodule ExStorageService.InstanceConfigTest do
  use ExUnit.Case, async: true

  alias ExStorageService.InstanceConfig

  @cluster_members [
    %{id: "node-a", endpoint: :"ess-a@127.0.0.1"},
    %{id: "node-b", endpoint: :"ess-b@127.0.0.1"},
    %{id: "node-c", endpoint: :"ess-c@127.0.0.1"}
  ]

  @cluster_opts [
    mode: :cluster,
    node_role: :data,
    node_id: "node-a",
    cluster_name: "ess-test",
    cluster_topology: :static,
    cluster_members: @cluster_members,
    cluster_seeds: [:"ess-b@127.0.0.1", :"ess-c@127.0.0.1"],
    cluster_bootstrap: true,
    erlang_node: :"ess-a@127.0.0.1",
    erlang_cookie: :ess_test_cookie,
    internal_advertised_url: "http://ess-a.internal:9100",
    public_s3_enabled: false,
    web_enabled: false,
    cluster_data_plane_enabled: false
  ]

  test "current standalone defaults are valid" do
    assert {:ok, config} = InstanceConfig.new([])
    assert config.mode == :standalone
    assert config.node_role == :data
    assert config.node_id == "default"
    assert config.cluster_topology == :none
    assert config.cluster_members == []
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
    refute config.internal_transport_enabled
    assert config.internal_bind == {127, 0, 0, 1}
    assert config.internal_port == 9100
    assert is_nil(config.internal_advertised_url)
    assert is_nil(config.internal_secret)
    assert is_nil(config.internal_tls_certfile)
    assert is_nil(config.internal_tls_keyfile)
    assert config.internal_auth_skew_seconds == 300
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
             @cluster_opts
             |> Keyword.put(:public_s3_enabled, true)
             |> Keyword.put(:cluster_data_plane_enabled, true)
             |> InstanceConfig.new()

    assert message =~ "public S3 listener"
  end

  test "metadata-only cluster scaffolding accepts strict RF=2/W=2" do
    workers = Map.new(InstanceConfig.worker_defaults(), fn {worker, _} -> {worker, false} end)

    assert {:ok, config} =
             @cluster_opts
             |> Keyword.put(:node_role, :metadata)
             |> Keyword.put(:node_id, "node-c")
             |> Keyword.put(:erlang_node, :"ess-c@127.0.0.1")
             |> Keyword.put(:workers, workers)
             |> Keyword.put(:replication_factor, 2)
             |> Keyword.put(:write_quorum, 2)
             |> InstanceConfig.new()

    assert config.mode == :cluster
    assert config.node_role == :metadata
    assert config.replication_factor == 2
    assert config.write_quorum == 2
    refute config.internal_transport_enabled
    refute Enum.any?(config.workers, fn {_worker, enabled} -> enabled end)
  end

  test "cluster data nodes enable a validated internal transport" do
    assert {:ok, config} =
             @cluster_opts
             |> Keyword.put(:internal_bind, {10, 0, 0, 10})
             |> Keyword.put(:internal_port, 9443)
             |> Keyword.put(:internal_secret, "development-secret")
             |> Keyword.put(:internal_tls_certfile, "/run/secrets/internal.crt")
             |> Keyword.put(:internal_tls_keyfile, "/run/secrets/internal.key")
             |> Keyword.put(:internal_auth_skew_seconds, 60)
             |> InstanceConfig.new()

    assert config.internal_transport_enabled
    assert config.internal_bind == {10, 0, 0, 10}
    assert config.internal_port == 9443
    assert config.internal_advertised_url == "http://ess-a.internal:9100"
    assert config.internal_secret == "development-secret"
    assert config.internal_tls_certfile == "/run/secrets/internal.crt"
    assert config.internal_tls_keyfile == "/run/secrets/internal.key"
    assert config.internal_auth_skew_seconds == 60
    refute inspect(config) =~ "development-secret"
  end

  test "internal transport settings fail fast" do
    assert {:error, message} =
             @cluster_opts
             |> Keyword.delete(:internal_advertised_url)
             |> InstanceConfig.new()

    assert message =~ "advertised URL"

    for advertised_url <- [
          "",
          "ess-a.internal:9100",
          "ftp://ess-a.internal",
          "http:///missing",
          "http://ess-a.internal:invalid"
        ] do
      assert {:error, message} =
               @cluster_opts
               |> Keyword.put(:internal_advertised_url, advertised_url)
               |> InstanceConfig.new()

      assert message =~ "advertised URL"
    end

    assert {:error, message} =
             @cluster_opts |> Keyword.put(:internal_bind, "127.0.0.1") |> InstanceConfig.new()

    assert message =~ "bind"

    assert {:error, message} =
             @cluster_opts |> Keyword.put(:internal_port, 65_536) |> InstanceConfig.new()

    assert message =~ "port"

    assert {:error, message} =
             @cluster_opts |> Keyword.put(:internal_secret, "") |> InstanceConfig.new()

    assert message =~ "secret"

    assert {:error, message} =
             @cluster_opts
             |> Keyword.put(:internal_tls_certfile, "/run/secrets/internal.crt")
             |> InstanceConfig.new()

    assert message =~ "TLS certificate and key"

    assert {:error, message} =
             @cluster_opts |> Keyword.put(:internal_auth_skew_seconds, 0) |> InstanceConfig.new()

    assert message =~ "auth skew"
  end

  test "cluster identity, membership, topology, and role fail fast" do
    assert {:error, message} = InstanceConfig.new(mode: :cluster)
    assert message =~ "public S3 listener"

    assert {:error, message} =
             InstanceConfig.new(mode: :cluster, public_s3_enabled: false)

    assert message =~ "web listener"

    assert {:error, message} =
             @cluster_opts
             |> Keyword.put(:cluster_members, tl(@cluster_members))
             |> InstanceConfig.new()

    assert message =~ "exactly three"

    duplicate = [
      Enum.at(@cluster_members, 0),
      Enum.at(@cluster_members, 0),
      Enum.at(@cluster_members, 2)
    ]

    assert {:error, message} =
             @cluster_opts |> Keyword.put(:cluster_members, duplicate) |> InstanceConfig.new()

    assert message =~ "unique"

    assert {:error, message} =
             @cluster_opts |> Keyword.put(:erlang_node, :nonode@nohost) |> InstanceConfig.new()

    assert message =~ "distributed Erlang"

    assert {:error, message} =
             @cluster_opts |> Keyword.put(:erlang_cookie, :nocookie) |> InstanceConfig.new()

    assert message =~ "cookie"

    assert {:error, message} =
             @cluster_opts |> Keyword.put(:cluster_topology, :dns) |> InstanceConfig.new()

    assert message =~ "DNS"

    assert {:error, message} =
             @cluster_opts
             |> Keyword.put(:node_role, :metadata)
             |> Keyword.put(:workers, %{cas_gc: true})
             |> InstanceConfig.new()

    assert message =~ "data-plane workers"
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
