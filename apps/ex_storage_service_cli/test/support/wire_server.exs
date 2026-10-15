defmodule ExStorageServiceCli.Test.WireServer do
  @moduledoc false

  def start(handler) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_address, port}} = :inet.sockname(listener)
    owner = self()

    server =
      spawn(fn ->
        try do
          handler.(listener, owner)
        rescue
          error -> send(owner, {:wire_error, self(), error, __STACKTRACE__})
        after
          :gen_tcp.close(listener)
        end
      end)

    ExUnit.Callbacks.on_exit(fn ->
      :gen_tcp.close(listener)
      if Process.alive?(server), do: Process.exit(server, :kill)
    end)

    {"http://127.0.0.1:#{port}", server}
  end

  def fixture(responses) do
    start(fn listener, owner ->
      Enum.each(responses, fn {status, headers, body} ->
        {:ok, socket} = :gen_tcp.accept(listener, 15_000)
        request = request(socket)
        send(owner, {:wire_request, request})
        response(socket, status, headers, body)
        :gen_tcp.close(socket)
      end)
    end)
  end

  def request(socket) do
    {head, rest} = head(socket)
    [line | fields] = String.split(head, "\r\n")
    [method, target, _version] = String.split(line, " ")

    headers =
      Map.new(fields, fn field ->
        [key, value] = String.split(field, ":", parts: 2)
        {String.downcase(key), String.trim(value)}
      end)

    length = String.to_integer(headers["content-length"] || "0")
    %{method: method, target: target, headers: headers, body: body(socket, rest, length)}
  end

  def head(socket, data \\ "") do
    case :binary.split(data, "\r\n\r\n") do
      [head, rest] ->
        {head, rest}

      [_partial] ->
        {:ok, chunk} = :gen_tcp.recv(socket, 0, 15_000)
        head(socket, data <> chunk)
    end
  end

  def body(_socket, data, length) when byte_size(data) >= length, do: data

  def body(socket, data, length) do
    {:ok, chunk} = :gen_tcp.recv(socket, length - byte_size(data), 15_000)
    body(socket, data <> chunk, length)
  end

  def response(socket, status, headers, body) do
    length =
      if Enum.any?(headers, fn {name, _} -> String.downcase(name) == "content-length" end),
        do: [],
        else: [{"Content-Length", to_string(byte_size(body))}]

    fields = length ++ [{"Connection", "close"} | headers]
    fields = Enum.map_join(fields, "", fn {key, value} -> "#{key}: #{value}\r\n" end)
    :ok = :gen_tcp.send(socket, "HTTP/1.1 #{status} Fixture\r\n#{fields}\r\n#{body}")
  end
end
