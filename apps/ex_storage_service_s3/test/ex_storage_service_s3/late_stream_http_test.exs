defmodule ExStorageServiceS3.LateStreamHTTPTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  alias ExStorageService.BlobStore.Source
  alias ExStorageServiceS3.Handlers.Shared

  defmodule FailingStream do
    def init(opts), do: opts

    def call(conn, _opts) do
      source =
        Source.stateful_stream(
          fn conn, reducer ->
            {:cont, next} = reducer.("valid-prefix", conn)
            {:error, {:injected, "private-storage-path"}, next}
          end,
          100
        )

      Shared.send_blob_source(conn, 200, source, request_id: "late-stream-http")
    end
  end

  test "failure after headers truncates the transfer without appending an error document" do
    server =
      start_supervised!(
        {Gaoxy.HTTP, plug: {FailingStream, []}, port: 0, limits: %{max_bytes: 4_194_304}}
      )

    %{local: {:ok, {_, port}}} = Gaoxy.info(server, :http)
    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false], 2_000)
    on_exit(fn -> :gen_tcp.close(socket) end)

    capture_log(fn ->
      :ok =
        :gen_tcp.send(
          socket,
          "GET /object HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )

      wire = receive_closed(socket, [])
      assert wire =~ "HTTP/1.1 200"
      [headers, body] = String.split(wire, "\r\n\r\n", parts: 2)
      assert String.downcase(headers) =~ "content-length: 100"
      assert body == "valid-prefix"
      refute wire =~ "<Error>"
      refute wire =~ "HTTP/1.1 500"
      refute wire =~ "private-storage-path"
    end)
  end

  defp receive_closed(socket, chunks) do
    case :gen_tcp.recv(socket, 0, 2_000) do
      {:ok, chunk} -> receive_closed(socket, [chunk | chunks])
      {:error, :closed} -> chunks |> Enum.reverse() |> IO.iodata_to_binary()
      {:error, reason} -> flunk("stream failed to close: #{inspect(reason)}")
    end
  end
end
