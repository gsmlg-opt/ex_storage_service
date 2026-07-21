defmodule ExStorageService.Metadata.Backend do
  @moduledoc """
  Storage boundary used by atomic object metadata operations.

  Reads include the Concord modification revision needed for compare-and-swap.
  Prefix scans intentionally expose values only; callers that need a revision
  must read the individual key with `get/2`.
  """

  @type key :: binary()
  @type read_result :: %{value: term(), mod_revision: non_neg_integer()}
  @type transaction_spec :: %{
          required(:compare) => [term()],
          required(:success) => [term()],
          required(:failure) => [term()]
        }

  @callback get(key(), keyword()) ::
              {:ok, read_result() | nil} | {:error, term()}
  @callback put(key(), term(), keyword()) :: :ok | {:error, term()}
  @callback delete(key(), keyword()) :: :ok | {:error, term()}
  @callback get_all(keyword()) :: {:ok, [{key(), term()}]} | {:error, term()}
  @callback scan(binary(), keyword()) :: {:ok, [{key(), term()}]} | {:error, term()}
  @callback transaction(transaction_spec(), keyword()) :: {:ok, term()} | {:error, term()}
  @callback resolve_operation(key(), keyword()) ::
              {:ok, read_result() | nil} | {:error, term()}
end
