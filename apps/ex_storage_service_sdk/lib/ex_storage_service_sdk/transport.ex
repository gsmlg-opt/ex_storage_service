defmodule ExStorageServiceSdk.Transport do
  @moduledoc """
  HTTP transport contract used by `ExStorageServiceSdk.S3.Client`.

  The default, `ExStorageServiceSdk.Transport.HTTPFetch`, is built on `http_fetch`.
  A replacement must return `ExStorageServiceSdk.S3.Response` structs whose `:body`
  is either a binary or a stream process consumed through `c:consume/4`, and must
  report failures as `{:error, %ExStorageServiceSdk.Error{category: :transport}}`.
  """
  alias ExStorageServiceSdk.{Error, S3.Request, S3.Response}

  @callback request(url :: String.t(), request :: Request.t(), opts :: keyword()) ::
              {:ok, Response.t()} | {:error, Error.t()}

  @callback consume(
              response :: Response.t(),
              acc :: term(),
              reducer :: (binary(), term() -> {:ok, term()} | {:error, Error.t()}),
              opts :: keyword()
            ) :: {:ok, term(), map()} | {:error, Error.t()}

  @callback close(response :: Response.t()) :: :ok
end
