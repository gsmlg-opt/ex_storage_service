defmodule ExStorageServiceCli.Error do
  @moduledoc """
  A safe, structured CLI failure. Details must never contain credentials or signed URLs.
  """
  defstruct category: :client,
            message: "Command failed",
            operation: nil,
            code: nil,
            http_status: nil,
            request_id: nil,
            retryable: false,
            attempt: nil,
            target: nil,
            details: %{}

  def new(category, message, fields \\ []) do
    struct!(__MODULE__, Keyword.merge(fields, category: category, message: message))
  end
end
