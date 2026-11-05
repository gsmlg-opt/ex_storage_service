defmodule ExStorageService.Metadata.GatewayIndexTest do
  use ExUnit.Case, async: true
  alias ExStorageService.CloudCache.Config
  alias ExStorageService.Metadata.{GatewayIndex, Migration}

  @moduletag :tmp_dir

  defmodule EmptyBackend do
    def get(_key, _opts), do: {:ok, nil}
  end

  test "real signed conditional GET verifies raw encoded bytes without publishing or caching",
       context do
    body = :zlib.gzip(:crypto.strong_rand_bytes(200_000))
    headers = headers(body) ++ [{"Content-Encoding", "gzip"}]
    config = server([{200, headers, ""}, {200, headers, body}])

    metadata =
      metadata(body)
      |> Map.merge(%{
        etag: "local-digest",
        upstream_etag: "remote-etag",
        upstream_version_id: "remote-version"
      })

    unrelated = Path.join(context.tmp_dir, "retained")
    File.write!(unrelated, "keep")

    assert :ok =
             GatewayIndex.verify_current("gateway", metadata, config,
               gateway_tmp_dir: context.tmp_dir
             )

    assert_received {:gateway_request, "HEAD", "/upstream/key", head}
    assert head["authorization"] =~ "AWS4-HMAC-SHA256"
    assert_received {:gateway_request, "GET", "/upstream/key", get}
    assert get["if-match"] == "\"remote-etag\""
    assert get["x-amz-checksum-mode"] == "ENABLED"
    assert File.ls!(context.tmp_dir) == ["retained"]
    assert File.read!(unrelated) == "keep"
  end

  test "cloud records plan metadata only while hosted and false-valued flags retain CAS checks" do
    cloud = metadata("bytes")

    opts = [
      replication_factor: 2,
      local_blob_probe: fn _, _ -> flunk("cloud version probed CAS") end
    ]

    assert {:ok, %{compare: [], success: []}} = Migration.prepare_index_version(cloud, opts)

    assert {:error, :invalid_gateway_blob_identity} =
             Migration.prepare_index_version(%{cloud | size: -1}, opts)

    assert {:error, :invalid_gateway_blob_identity} =
             Migration.prepare_index_version(%{cloud | content_hash: "not-sha256"}, opts)

    assert {:error, :invalid_gateway_object_identity} =
             Migration.prepare_index_version(Map.delete(cloud, :key), opts)

    marker = cloud |> Map.drop([:size, :content_hash]) |> Map.put(:delete_marker, true)
    assert {:ok, %{compare: [], success: []}} = Migration.prepare_index_version(marker, opts)

    for flag <- [false, "true", nil] do
      assert {:error, {:insufficient_verified_blob_sources, _, 2}} =
               Migration.prepare_index_version(%{cloud | cloud_backed: flag},
                 backend: EmptyBackend,
                 replication_factor: 2,
                 local_blob_probe: fn _, _ -> :missing end
               )
    end
  end

  test "selected cloud marker verifies only current upstream absence", context do
    config = server([{404, [{"Content-Length", "0"}], ""}])

    marker =
      metadata("ignored") |> Map.drop([:size, :content_hash]) |> Map.put(:delete_marker, true)

    assert :ok =
             GatewayIndex.verify_current("gateway", marker, config,
               gateway_tmp_dir: context.tmp_dir
             )

    assert_received {:gateway_request, "HEAD", "/upstream/key", _}
    refute_received {:gateway_request, "GET", _, _}

    config = server([{200, headers("still present"), ""}])

    assert {:error, {:gateway_index_verification_failed, :upstream_present}} =
             GatewayIndex.verify_current("gateway", marker, config,
               gateway_tmp_dir: context.tmp_dir
             )

    assert File.ls!(context.tmp_dir) == []
  end

  test "missing or invalid configuration and missing upstream fail closed", context do
    body = "bytes"

    for record <- [
          nil,
          %{value: %{enabled: false}},
          %{value: %{enabled: true, provider: :unknown}}
        ] do
      assert {:error, {:gateway_index_verification_failed, :gateway_configuration_unavailable}} =
               GatewayIndex.verify_current("gateway", metadata(body), record,
                 gateway_tmp_dir: context.tmp_dir
               )
    end

    config = server([{404, [{"Content-Length", "0"}], ""}])

    assert {:error, {:gateway_index_verification_failed, :upstream_missing}} =
             GatewayIndex.verify_current("gateway", metadata(body), config,
               gateway_tmp_dir: context.tmp_dir
             )

    assert File.ls!(context.tmp_dir) == []
  end

  test "body hash mismatch removes only verification bytes", context do
    body = "changed bytes"
    config = server([{200, headers(body), ""}, {200, headers(body), body}])
    metadata = %{metadata(body) | content_hash: String.duplicate("0", 64)}

    assert {:error, {:gateway_index_verification_failed, :content_mismatch}} =
             GatewayIndex.verify_current("gateway", metadata, config,
               gateway_tmp_dir: context.tmp_dir
             )

    assert File.ls!(context.tmp_dir) == []
  end

  test "HEAD and GET ETag or remote version changes cannot certify the selected body", context do
    body = "bytes"

    for changed <- [
          replace_header(headers(body), "ETag", "\"replacement\""),
          replace_header(headers(body), "x-amz-version-id", "replacement")
        ] do
      config = server([{200, headers(body), ""}, {200, changed, body}])

      assert {:error, {:gateway_index_verification_failed, :upstream_changed}} =
               GatewayIndex.verify_current("gateway", metadata(body), config,
                 gateway_tmp_dir: context.tmp_dir
               )

      assert File.ls!(context.tmp_dir) == []
    end
  end

  test "recorded upstream identities and HEAD length are mandatory when supplied", context do
    body = "bytes"

    for {metadata, expected} <- [
          {Map.put(metadata(body), :upstream_etag, "stale"), :stored_upstream_identity_changed},
          {Map.put(metadata(body), :upstream_version_id, "stale"),
           :stored_upstream_identity_changed},
          {Map.put(metadata(body), :upstream_version_id, nil), :stored_upstream_identity_changed},
          {%{metadata(body) | size: 0}, :invalid_upstream_identity}
        ] do
      config = server([{200, headers(body), ""}])

      assert {:error, {:gateway_index_verification_failed, ^expected}} =
               GatewayIndex.verify_current("gateway", metadata, config,
                 gateway_tmp_dir: context.tmp_dir
               )

      assert_received {:gateway_request, "HEAD", "/upstream/key", _}
      refute_received {:gateway_request, "GET", _, _}
    end

    assert File.ls!(context.tmp_dir) == []
  end

  defp metadata(body) do
    %{
      schema: 2,
      bucket: "gateway",
      key: "key",
      version_id: "local-version",
      cloud_backed: true,
      content_hash: Base.encode16(:crypto.hash(:sha256, body), case: :lower),
      size: byte_size(body),
      delete_marker: false
    }
  end

  defp headers(body),
    do: [
      {"Content-Length", to_string(byte_size(body))},
      {"ETag", "\"remote-etag\""},
      {"x-amz-version-id", "remote-version"}
    ]

  defp replace_header(headers, name, value),
    do:
      Enum.map(headers, fn {key, current} -> {key, if(key == name, do: value, else: current)} end)

  defp server(responses) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_, port}} = :inet.sockname(listener)
    owner = self()

    server =
      spawn(fn ->
        Enum.each(responses, fn {status, headers, body} ->
          {:ok, socket} = :gen_tcp.accept(listener, 5_000)

          try do
            [line | lines] = socket |> read_head("") |> String.split("\r\n", trim: true)
            [method, target, _] = String.split(line, " ")

            request_headers =
              Map.new(lines, fn line ->
                [name, value] = String.split(line, ":", parts: 2)
                {String.downcase(name), String.trim(value)}
              end)

            send(owner, {:gateway_request, method, target, request_headers})
            wire_headers = Enum.map(headers, fn {name, value} -> [name, ": ", value, "\r\n"] end)

            :ok =
              :gen_tcp.send(socket, [
                "HTTP/1.1 #{status} Response\r\n",
                wire_headers,
                "Connection: close\r\n\r\n",
                body
              ])
          after
            :gen_tcp.close(socket)
          end
        end)
      end)

    on_exit(fn ->
      monitor = Process.monitor(server)
      if Process.alive?(server), do: Process.exit(server, :kill)

      receive do
        {:DOWN, ^monitor, :process, ^server, _} -> :ok
      after
        1_000 -> raise "owned gateway fixture did not stop"
      end

      :gen_tcp.close(listener)
    end)

    %{
      mod_revision: 1,
      value: %{
        enabled: true,
        provider: :s3_compat,
        endpoint: "http://127.0.0.1:#{port}",
        bucket: "upstream",
        region: "us-east-1",
        access_key_id: "fixture-access",
        encrypted_secret: Config.encrypt_secret("fixture-secret")
      }
    }
  end

  defp read_head(socket, data) when byte_size(data) < 65_536 do
    case :binary.split(data, "\r\n\r\n") do
      [head, _] ->
        head

      [_] ->
        {:ok, chunk} = :gen_tcp.recv(socket, 0, 5_000)
        read_head(socket, data <> chunk)
    end
  end
end
