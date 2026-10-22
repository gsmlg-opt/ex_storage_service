defmodule ExStorageServiceWeb.EndpointHTTPTest do
  use ExUnit.Case, async: true

  alias ExStorageService.HTTPClient
  alias ExStorageService.Metadata
  alias ExStorageService.ObjectService
  alias ExStorageServiceWeb.Endpoint

  test "serves login and authentication redirects through cleartext HTTP/2" do
    assert {:ok, {_address, port}} = Endpoint.server_info(:http)
    url = "http://localhost:#{port}"
    options = [http_version: :h2c, redirect: :manual, decode_body: false, timeout: 5_000]

    assert %HTTP.Response{status: 200, http_version: :http2} =
             login =
             HTTP.fetch(url <> "/login", options) |> HTTP.Promise.await()

    assert HTTP.Response.text(login) =~ "Admin Login"

    assert %HTTP.Response{status: 302, http_version: :http2, headers: headers} =
             redirect =
             HTTP.fetch(url <> "/dashboard", options) |> HTTP.Promise.await()

    assert HTTP.Headers.get(headers, "location") == "/login"
    assert is_binary(HTTP.Response.text(redirect))
  end

  test "serves authenticated LiveView requests and theme events through the Gaoxy listener" do
    assert Endpoint.config(:adapter) == Gaoxy.PhoenixAdapter
    assert {:ok, {address, port}} = Endpoint.server_info(:http)
    url = "http://localhost:#{port}"

    assert {:ok, %{status: 302, headers: headers}} = HTTPClient.request(url <> "/dashboard")
    assert header(headers, "location") == "/login"

    assert {:ok, %{status: 200, headers: headers, body: login}} =
             HTTPClient.request(url <> "/login")

    assert login =~ "Admin Login"
    csrf = attribute(LazyHTML.from_document(login), "input[name=_csrf_token]", "value")

    assert {:ok, %{status: 403}} =
             HTTPClient.request(url <> "/login",
               method: :post,
               headers: [{"content-type", "application/x-www-form-urlencoded"}],
               body: URI.encode_query(%{username: "admin", password: "admin"})
             )

    cookie = login(url, headers, csrf)

    assert {:ok, %{status: 200, body: dashboard}} =
             HTTPClient.request(url <> "/dashboard", headers: [{"cookie", cookie}])

    assert dashboard =~ "Storage Dashboard"
    document = LazyHTML.from_document(dashboard)
    csrf = attribute(document, "meta[name=csrf-token]", "content")
    socket = websocket(address, port, url, cookie, csrf)
    {topic, rendered} = join(socket, document, url <> "/dashboard", csrf)

    assert JSON.encode!(rendered) =~ "Storage Dashboard"

    push(socket, [
      "1",
      "2",
      topic,
      "event",
      %{"type" => "hook", "event" => "theme_changed", "value" => %{"theme" => "moonlight"}}
    ])

    assert %{"status" => "ok", "response" => %{"diff" => diff}} = reply(socket, topic, "2")
    assert JSON.encode!(diff) =~ "moonlight"
  end

  test "serves a real object listing larger than 256 KiB over LiveView" do
    bucket = "gaoxy-liveview-#{System.unique_integer([:positive])}"
    keys = Enum.map(1..100, &String.pad_leading(Integer.to_string(&1), 1_024, "k"))
    assert :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      Enum.each(keys, &ObjectService.delete(bucket, &1, nil))
      assert :ok = Metadata.delete_bucket(bucket)
    end)

    for key <- keys do
      assert {:ok, _} = ObjectService.put(bucket, key, "listing fixture", "text/plain", %{})
    end

    assert {:ok, {address, port}} = Endpoint.server_info(:http)
    url = "http://localhost:#{port}"

    assert {:ok, %{status: 200, headers: headers, body: login}} =
             HTTPClient.request(url <> "/login")

    csrf = attribute(LazyHTML.from_document(login), "input[name=_csrf_token]", "value")
    cookie = login(url, headers, csrf)
    page_url = url <> "/buckets/#{bucket}/files"

    assert {:ok, %{status: 200, body: listing}} =
             HTTPClient.request(page_url, headers: [{"cookie", cookie}])

    document = LazyHTML.from_document(listing)
    csrf = attribute(document, "meta[name=csrf-token]", "content")
    socket = websocket(address, port, url, cookie, csrf)
    {_topic, rendered} = join(socket, document, page_url, csrf)
    payload = JSON.encode!(rendered)
    assert byte_size(payload) > 262_144
    assert Enum.all?(keys, &String.contains?(payload, &1))
  end

  defp login(url, headers, csrf) do
    assert {:ok, %{status: 302, headers: headers}} =
             HTTPClient.request(url <> "/login",
               method: :post,
               headers: [
                 {"cookie", session_cookie(headers)},
                 {"content-type", "application/x-www-form-urlencoded"}
               ],
               body: URI.encode_query(%{username: "admin", password: "admin", _csrf_token: csrf})
             )

    assert header(headers, "location") == "/dashboard"
    session_cookie(headers)
  end

  defp join(socket, document, url, csrf) do
    topic = "lv:" <> attribute(document, "[data-phx-main]", "id")

    push(socket, [
      "1",
      "1",
      topic,
      "phx_join",
      %{
        "url" => url,
        "session" => attribute(document, "[data-phx-main]", "data-phx-session"),
        "static" => attribute(document, "[data-phx-main]", "data-phx-static"),
        "params" => %{"_csrf_token" => csrf, "_mounts" => 0}
      }
    ])

    assert %{"status" => "ok", "response" => %{"rendered" => rendered}} =
             reply(socket, topic, "1")

    {topic, rendered}
  end

  defp attribute(document, selector, name) do
    [value] = document |> LazyHTML.query(selector) |> LazyHTML.attribute(name)
    value
  end

  defp header(headers, name) do
    Enum.find_value(headers, fn {key, value} ->
      if String.downcase(key) == name, do: value
    end)
  end

  defp session_cookie(headers) do
    [cookie | _] = headers |> header("set-cookie") |> String.split(";", parts: 2)
    cookie
  end

  defp websocket(address, port, origin, cookie, csrf) do
    assert {:ok, socket} =
             :gen_tcp.connect(address, port, [:binary, active: false, packet: :http_bin], 5_000)

    on_exit(fn -> :gen_tcp.close(socket) end)
    key = Base.encode64(:crypto.strong_rand_bytes(16))
    query = URI.encode_query(%{vsn: "2.0.0", _csrf_token: csrf})

    assert :ok =
             :gen_tcp.send(socket, [
               "GET /live/websocket?",
               query,
               " HTTP/1.1\r\n",
               "Host: localhost:",
               Integer.to_string(port),
               "\r\n",
               "Origin: ",
               origin,
               "\r\n",
               "Cookie: ",
               cookie,
               "\r\n",
               "Connection: Upgrade\r\nUpgrade: websocket\r\n",
               "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ",
               key,
               "\r\n\r\n"
             ])

    assert {:ok, {:http_response, {1, 1}, 101, _reason}} = :gen_tcp.recv(socket, 0, 5_000)
    headers = websocket_headers(socket, [])
    accept = Base.encode64(:crypto.hash(:sha, key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
    assert header(headers, "sec-websocket-accept") == accept
    assert :ok = :inet.setopts(socket, packet: :raw)
    socket
  end

  defp websocket_headers(socket, headers) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, {:http_header, _index, name, _reserved, value}} ->
        websocket_headers(socket, [{to_string(name), to_string(value)} | headers])

      {:ok, :http_eoh} ->
        headers

      other ->
        flunk("unexpected WebSocket handshake: #{inspect(other)}")
    end
  end

  # Only the uncompressed text messages used by this application's LiveView flow
  # are needed here; HTTP parsing is handled by OTP's :http_bin packet mode.
  defp push(socket, message) do
    payload = JSON.encode!(message)
    size = byte_size(payload)
    assert size <= 65_535
    prefix = if size < 126, do: <<0x81, 1::1, size::7>>, else: <<0x81, 0xFE, size::16>>
    mask = :crypto.strong_rand_bytes(4)
    key = binary_part(:binary.copy(mask, div(size + 3, 4)), 0, size)
    assert :ok = :gen_tcp.send(socket, [prefix, mask, :crypto.exor(payload, key)])
  end

  defp reply(socket, topic, ref) do
    reply(socket, topic, ref, System.monotonic_time(:millisecond) + 5_000)
  end

  defp reply(socket, topic, ref, deadline) do
    <<1::1, 0::3, 1::4, 0::1, size::7>> = read(socket, 2, deadline)

    size =
      case size do
        126 -> :binary.decode_unsigned(read(socket, 2, deadline))
        127 -> :binary.decode_unsigned(read(socket, 8, deadline))
        size -> size
      end

    assert size > 0 and size <= 1_048_576

    case socket |> read(size, deadline) |> JSON.decode!() do
      ["1", ^ref, ^topic, "phx_reply", response] -> response
      ["1", nil, ^topic, "diff", _diff] -> reply(socket, topic, ref, deadline)
      other -> flunk("unexpected LiveView message: #{inspect(other)}")
    end
  end

  defp read(socket, size, deadline) do
    timeout = deadline - System.monotonic_time(:millisecond)
    assert timeout > 0, "timed out waiting for a LiveView reply"
    assert {:ok, bytes} = :gen_tcp.recv(socket, size, timeout)
    bytes
  end
end
