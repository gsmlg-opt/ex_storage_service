defmodule ExStorageServiceCluster.ApplicationTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCluster.{Application, InternalAuth, ReplayCache, Router}

  @base_opts [
    enabled: true,
    secret: "phase5-test-secret-at-least-32-bytes",
    node_id: "data-a",
    blob_store_opts: [root: "/tmp/unused", tmp_dir: "/tmp/unused-stage"],
    max_blob_size: 1_024,
    bind: {127, 0, 0, 1},
    port: 19_100
  ]

  test "disabled and metadata-derived configurations start no listener" do
    assert Application.children(enabled: false) == []
  end

  test "enabled data nodes start replay protection before the private listener" do
    assert [
             {ReplayCache, replay_opts},
             {Bandit, bandit_opts}
           ] = Application.children(@base_opts)

    assert replay_opts[:table] == InternalAuth.ReplayTable
    assert bandit_opts[:plug] |> elem(0) == Router
    assert bandit_opts[:ip] == {127, 0, 0, 1}
    assert bandit_opts[:port] == 19_100
    assert bandit_opts[:scheme] == :http
  end

  test "TLS certificate and key use Bandit's native HTTPS options" do
    tls = %{certfile: "/run/ess/cert.pem", keyfile: "/run/ess/key.pem"}
    [_cache, {Bandit, options}] = Application.children(Keyword.put(@base_opts, :tls, tls))

    assert options[:scheme] == :https
    assert options[:certfile] == tls.certfile
    assert options[:keyfile] == tls.keyfile
  end
end
