defmodule ExStorageService.Cluster.Readiness do
  @moduledoc """
  Quorum-backed Concord readiness checks.

  `Concord.status/1` includes a linearizable storage query. Local VSR status or
  configured membership alone does not prove that a majority is available.
  """

  @default_timeout 1_000
  @default_interval 100

  @spec check(keyword()) :: {:ok, map()} | {:error, term()}
  def check(opts \\ []) do
    backend = Keyword.get(opts, :backend, Concord)
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    case backend.status(timeout: timeout) do
      {:ok, %{cluster: %{status: :normal, primary_id: primary_id}} = status}
      when not is_nil(primary_id) ->
        {:ok, status}

      {:ok, status} ->
        {:error, {:not_ready, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec ready?(keyword()) :: boolean()
  def ready?(opts \\ []), do: match?({:ok, _status}, check(opts))

  @spec await(keyword()) :: {:ok, map()} | {:error, :timeout}
  def await(opts \\ []) do
    timeout = Keyword.get(opts, :await_timeout, 5_000)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await(opts, deadline)
  end

  defp do_await(opts, deadline) do
    case check(opts) do
      {:ok, status} ->
        {:ok, status}

      {:error, _reason} ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(Keyword.get(opts, :interval, @default_interval))
          do_await(opts, deadline)
        else
          {:error, :timeout}
        end
    end
  end
end
