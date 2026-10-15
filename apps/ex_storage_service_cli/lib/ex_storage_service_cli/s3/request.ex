defmodule ExStorageServiceCli.S3.Request do
  @moduledoc "An operation request before endpoint selection and authentication."
  defstruct [:operation, :method, :path, :body, :payload_hash, :lease, query: [], headers: []]
end
