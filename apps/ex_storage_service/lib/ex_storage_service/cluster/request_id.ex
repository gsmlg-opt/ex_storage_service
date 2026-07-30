defmodule ExStorageService.Cluster.RequestId do
  @moduledoc """
  Generates and validates opaque request IDs for authenticated cluster traffic.

  Every HTTP attempt gets a fresh ID so replay protection does not conflict
  with an idempotent higher-level operation retry.
  """

  @pattern ~r/\A[A-Za-z0-9._~-]{16,128}\z/

  @spec generate() :: String.t()
  def generate do
    18
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  @spec valid?(term()) :: boolean()
  def valid?(request_id) when is_binary(request_id),
    do: Regex.match?(@pattern, request_id)

  def valid?(_request_id), do: false
end
