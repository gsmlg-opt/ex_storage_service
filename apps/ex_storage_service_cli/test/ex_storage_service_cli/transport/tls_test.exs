defmodule ExStorageServiceCli.Transport.TLSTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Config, Context, Error, S3.Client}
  @moduletag :tmp_dir
  @body "<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>"

  setup %{tmp_dir: root} do
    openssl = System.find_executable("openssl")
    assert is_binary(openssl), "TLS acceptance requires openssl"

    command = fn arguments ->
      {output, status} = System.cmd(openssl, arguments, cd: root, stderr_to_stdout: true)
      assert status == 0, output
    end

    command.([
      "req",
      "-x509",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:P-256",
      "-nodes",
      "-keyout",
      "ca.key",
      "-out",
      "ca.pem",
      "-subj",
      "/CN=ESS CLI test CA",
      "-days",
      "1"
    ])

    for {name, serial, usage} <- [{"server", "1", "serverAuth"}, {"client", "2", "clientAuth"}] do
      command.([
        "req",
        "-new",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:P-256",
        "-nodes",
        "-keyout",
        name <> ".key",
        "-out",
        name <> ".csr",
        "-subj",
        "/CN=localhost"
      ])

      File.write!(
        Path.join(root, name <> ".ext"),
        "subjectAltName=DNS:localhost,DNS:pinned.invalid,IP:127.0.0.1\nextendedKeyUsage=" <>
          usage <> "\n"
      )

      command.([
        "x509",
        "-req",
        "-in",
        name <> ".csr",
        "-CA",
        "ca.pem",
        "-CAkey",
        "ca.key",
        "-set_serial",
        serial,
        "-days",
        "1",
        "-extfile",
        name <> ".ext",
        "-out",
        name <> ".pem"
      ])

      File.chmod!(Path.join(root, name <> ".key"), 0o600)
    end

    :ok
  end

  defp endpoint(root, mutual) do
    {:ok, _} = Application.ensure_all_started(:ssl)
    path = fn name -> root |> Path.join(name) |> String.to_charlist() end

    options = [
      certfile: path.("server.pem"),
      keyfile: path.("server.key"),
      cacertfile: path.("ca.pem"),
      verify: if(mutual, do: :verify_peer, else: :verify_none),
      fail_if_no_peer_cert: mutual,
      active: false,
      mode: :binary,
      ip: {127, 0, 0, 1}
    ]

    {:ok, listener} = :ssl.listen(0, options)
    {:ok, {_, port}} = :ssl.sockname(listener)
    owner = self()

    server =
      spawn(fn ->
        with {:ok, socket} <- :ssl.transport_accept(listener, 3000),
             {:ok, socket} <- :ssl.handshake(socket, 3000) do
          send(owner, {:peer_certificate, :ssl.peercert(socket)})
          send(owner, {:tls_sni, :ssl.connection_information(socket, [:sni_hostname])})
          send(owner, {:tls_request, request(socket, "")})

          :ssl.send(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: #{byte_size(@body)}\r\nConnection: close\r\n\r\n" <>
              @body
          )

          :ssl.close(socket)
        end

        :ssl.close(listener)
      end)

    on_exit(fn ->
      :ssl.close(listener)
      if Process.alive?(server), do: Process.exit(server, :kill)
    end)

    "https://127.0.0.1:#{port}"
  end

  defp request(socket, bytes) do
    if :binary.match(bytes, "\r\n\r\n") != :nomatch do
      bytes
    else
      {:ok, chunk} = :ssl.recv(socket, 0, 3000)
      request(socket, bytes <> chunk)
    end
  end

  defp context(root, endpoint, extra) do
    Config.read(config_dir: Path.join(root, "config"))
    |> then(fn {:ok, config} ->
      Context.native(nil, Map.merge(%{endpoint: endpoint, no_sign_request: true}, extra), config)
    end)
  end

  test "custom CA validates a real TLS server without disabling verification", %{tmp_dir: root} do
    endpoint = endpoint(root, false)
    assert {:ok, context} = context(root, endpoint, %{ca_bundle: Path.join(root, "ca.pem")})
    assert context.tls[:verify] == :verify_peer
    assert {:ok, %{"Buckets" => []}} = Client.call(context, "ListBuckets", %{}, max_attempts: 1)
  end

  test "untrusted server is rejected by the default trust policy", %{tmp_dir: root} do
    endpoint = endpoint(root, false)
    assert {:ok, context} = context(root, endpoint, %{})

    assert {:error, %Error{category: :transport}} =
             Client.call(context, "ListBuckets", %{}, max_attempts: 1)
  end

  test "a native address pin preserves original TLS hostname verification and SNI", %{
    tmp_dir: root
  } do
    url = endpoint(root, false) |> String.replace("127.0.0.1", "pinned.invalid")

    assert {:ok, options} =
             ExStorageServiceCli.CLI.NativeGlobals.prepare(%{
               resolve: ["pinned.invalid=127.0.0.1"],
               ca_bundle: Path.join(root, "ca.pem")
             })

    assert {:ok, context} = context(root, url, options)
    assert context.tls[:verify] == :verify_peer
    assert {:ok, %{"Buckets" => []}} = Client.call(context, "ListBuckets", %{}, max_attempts: 1)
    assert_receive {:tls_sni, {:ok, [sni_hostname: ~c"pinned.invalid"]}}
    assert_receive {:tls_request, request}
    assert request =~ "pinned.invalid:#{URI.parse(url).port}"
  end

  test "a native pin cannot substitute its IP for an invalid original TLS identity", %{
    tmp_dir: root
  } do
    url = endpoint(root, false) |> String.replace("127.0.0.1", "wrong.invalid")

    assert {:ok, options} =
             ExStorageServiceCli.CLI.NativeGlobals.prepare(%{
               resolve: ["wrong.invalid=127.0.0.1"],
               ca_bundle: Path.join(root, "ca.pem")
             })

    assert {:ok, context} = context(root, url, options)

    assert {:error, %Error{category: :transport}} =
             Client.call(context, "ListBuckets", %{}, max_attempts: 1)

    refute_receive {:tls_request, _}, 100
  end

  test "native mTLS sends the selected client certificate", %{tmp_dir: root} do
    endpoint = endpoint(root, true)

    assert {:ok, context} =
             context(root, endpoint, %{
               ca_bundle: Path.join(root, "ca.pem"),
               client_cert: Path.join(root, "client.pem"),
               client_key: Path.join(root, "client.key")
             })

    assert {:ok, %{"Buckets" => []}} = Client.call(context, "ListBuckets", %{}, max_attempts: 1)
    assert_receive {:peer_certificate, {:ok, certificate}}
    [entry] = root |> Path.join("client.pem") |> File.read!() |> :public_key.pem_decode()
    assert certificate == elem(entry, 1)
  end

  test "mTLS server rejects a client without a certificate", %{tmp_dir: root} do
    endpoint = endpoint(root, true)
    assert {:ok, context} = context(root, endpoint, %{ca_bundle: Path.join(root, "ca.pem")})

    assert {:error, %Error{category: :transport}} =
             Client.call(context, "ListBuckets", %{}, max_attempts: 1)
  end
end
