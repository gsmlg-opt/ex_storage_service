defmodule ExStorageServiceCluster.ApplicationTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCluster.{Application, InternalAuth, ReplayCache, Router}

  @base_opts [
    enabled: true,
    secret: "phase5-test-secret-at-least-32-bytes",
    node_id: "data-a",
    node_generation: 1,
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
             {Gaoxy.HTTP, http_opts}
           ] = Application.children(@base_opts)

    assert replay_opts[:table] == InternalAuth.ReplayTable
    assert http_opts[:id] == Router
    assert http_opts[:plug] |> elem(0) == Router
    assert http_opts[:ip] == {127, 0, 0, 1}
    assert http_opts[:port] == 19_100
    assert http_opts[:transport] == :tcp
    assert http_opts[:protocols] == [:http1]

    assert http_opts[:http_limits] == %{
             max_body_bytes: 1_024,
             max_read_chunk: 65_536,
             request_timeout: :infinity,
             read_timeout: 60_000,
             write_timeout: 60_000
           }

    assert {:ok, spec} = Gaoxy.HTTP.service_spec(http_opts)
    assert spec.handler_options.http_limits.header_timeout == 5_000
    assert spec.limits.max_bytes == 64 * 1_024 * 1_024
  end

  test "TLS certificate and key use Gaoxy's private HTTP/1 listener" do
    tls = %{certfile: "/run/ess/cert.pem", keyfile: "/run/ess/key.pem"}
    [_cache, {Gaoxy.HTTP, options}] = Application.children(Keyword.put(@base_opts, :tls, tls))

    assert options[:transport] == :tls
    assert options[:tls] == tls
    assert options[:protocols] == [:http1]
    assert options[:ip] == {127, 0, 0, 1}
  end

  test "retains custom inactivity and object-size bounds without a whole-transfer deadline" do
    opts = Keyword.merge(@base_opts, read_timeout: 120_000, max_blob_size: 2_147_483_648)
    [_cache, {Gaoxy.HTTP, options}] = Application.children(opts)

    assert options[:http_limits].read_timeout == 120_000
    assert options[:http_limits].max_body_bytes == 2_147_483_648
    assert options[:http_limits].request_timeout == :infinity
    assert options[:limits].max_bytes == 64 * 1_024 * 1_024
    assert {_router, router_opts} = options[:plug]
    assert router_opts[:read_timeout] == 120_000
  end

  test "an omitted bind address remains private loopback" do
    [_cache, {Gaoxy.HTTP, options}] = Application.children(Keyword.delete(@base_opts, :bind))

    assert options[:ip] == {127, 0, 0, 1}
  end
end
