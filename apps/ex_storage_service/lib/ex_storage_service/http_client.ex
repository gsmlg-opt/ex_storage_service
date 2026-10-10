defmodule ExStorageService.HTTPClient do
  @moduledoc """
  Outbound HTTP requests using `http_fetch`, including lazy uploads and downloads.

  Bodies remain raw entity bytes, including Content-Encoding. Redirects are
  returned to the caller and retries are owned by the calling worker, so signed
  requests are never replayed implicitly.
  """

  @spec request(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def request(url, opts \\ []) do
    case fetch(url, opts) do
      {:ok, %HTTP.Response{} = response} ->
        {:ok,
         %{
           status: response.status,
           headers: response.headers.headers,
           body: HTTP.Response.read_all(response)
         }}

      {:error, _reason} = error ->
        error
    end
  rescue
    exception in RuntimeError -> {:error, exception}
  end

  @doc "Returns the response without consuming its body. The caller owns consumption."
  @spec fetch(String.t(), keyword()) :: {:ok, HTTP.Response.t()} | {:error, term()}
  def fetch(url, opts \\ []) do
    options =
      Keyword.merge(
        [http_version: :http1, redirect: :manual, decode_body: false, timeout: 30_000],
        opts
      )

    case Keyword.get(options, :body) do
      body when is_nil(body) or is_binary(body) or is_pid(body) ->
        await(url, options)

      enumerable ->
        # HTTP/1 uploads accept at most 64 KiB per acknowledged source chunk.
        # Keep source reduction synchronous: blob request bodies cannot suspend.
        enumerable =
          fn acc, reducer ->
            Enumerable.reduce(enumerable, acc, fn item, current ->
              reduce_upload_chunks(IO.iodata_to_binary(item), current, reducer)
            end)
          end

        with {:ok, stream} <- HTTP.Stream.from_enumerable(enumerable) do
          try do
            await(url, Keyword.merge(options, body: stream, duplex: :half))
          after
            Process.unlink(stream)
            Process.exit(stream, :shutdown)
          end
        end
    end
  end

  defp reduce_upload_chunks(<<>>, acc, _reducer), do: {:cont, acc}

  defp reduce_upload_chunks(bytes, acc, reducer) do
    size = min(byte_size(bytes), 65_536)
    <<chunk::binary-size(^size), rest::binary>> = bytes

    case reducer.(chunk, acc) do
      {:cont, next} -> reduce_upload_chunks(rest, next, reducer)
      {:halt, next} -> {:halt, next}
    end
  end

  defp await(url, options) do
    case HTTP.fetch(url, options) |> HTTP.Promise.await() do
      %HTTP.Response{} = response -> {:ok, response}
      {:error, _reason} = error -> error
    end
  end
end
