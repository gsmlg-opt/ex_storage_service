defmodule ExStorageService.Metadata.Models.Operation do
  @moduledoc """
  Durable operation outcome used to resolve an ambiguous transaction timeout.
  """

  @enforce_keys [:operation_id, :request_fingerprint, :result]
  defstruct [
    :operation_id,
    :request_fingerprint,
    :result,
    :committed_at,
    schema: 2,
    events: []
  ]

  @type t :: %__MODULE__{
          schema: 2,
          operation_id: binary(),
          request_fingerprint: binary(),
          result: map(),
          events: [map()],
          committed_at: binary() | nil
        }
end
