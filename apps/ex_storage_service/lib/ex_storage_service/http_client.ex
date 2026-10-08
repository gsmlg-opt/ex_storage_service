defmodule ExStorageService.HTTPClient do
  @moduledoc """
  Buffered outbound HTTP requests using `http_fetch`.

  Bodies remain raw bytes. Redirects are returned to the caller and retries are
  owned by the calling worker, so signed requests are never replayed implicitly.
  """

  @spec request(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def request(url, opts \\ []) do
    options =
      Keyword.merge([http_version: :http1, redirect: :manual, timeout: 30_000], opts)

    case HTTP.fetch(url, options) |> HTTP.Promise.await() do
      %HTTP.Response{} = response ->
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
end
