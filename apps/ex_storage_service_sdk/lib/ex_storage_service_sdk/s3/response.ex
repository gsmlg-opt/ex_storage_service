defmodule ExStorageServiceSdk.S3.Response do
  @moduledoc "Response headers with a bounded control body or an owned object stream."
  @type t :: %__MODULE__{}

  defstruct [
    :status,
    :body,
    :request_id,
    :controller,
    :cancellation,
    :lease,
    :rate_limit,
    :timeout,
    headers: [],
    trailers: [],
    owns_controller: true
  ]
end
