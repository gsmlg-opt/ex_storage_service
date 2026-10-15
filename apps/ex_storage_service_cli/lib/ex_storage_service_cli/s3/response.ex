defmodule ExStorageServiceCli.S3.Response do
  @moduledoc "Response headers with a bounded control body or an owned object stream."
  defstruct [
    :status,
    :body,
    :request_id,
    :controller,
    :cancellation,
    :lease,
    :rate_limit,
    headers: [],
    trailers: [],
    owns_controller: true
  ]
end
