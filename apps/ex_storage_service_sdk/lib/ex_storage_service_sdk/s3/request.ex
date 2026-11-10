defmodule ExStorageServiceSdk.S3.Request do
  @moduledoc "An operation request before endpoint selection and authentication."
  @type t :: %__MODULE__{}

  defstruct [:operation, :method, :path, :body, :payload_hash, :lease, query: [], headers: []]
end
