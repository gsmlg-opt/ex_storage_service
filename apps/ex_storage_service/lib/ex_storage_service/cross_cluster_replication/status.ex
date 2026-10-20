defmodule ExStorageService.CrossClusterReplication.Status do
  @moduledoc """
  Page-limited operational observation of unfinished external replication jobs.

  Counts cover only observed pages when `complete` is false. Metrics retain the
  last complete counts and expose scan completeness separately. Call this
  process-free boundary through release RPC to refresh the observation; it does
  not add an unbounded worker scan or metadata work to every metrics scrape.
  Concord 3.1.2 seeks ordered prefix/range indexes for each page. Expired or
  deleted candidates inside the selected range can still increase query work.
  """

  alias ExStorageService.Metadata.JobStore
  alias ExStorageService.Metadata.Models.Job

  @spec snapshot(keyword()) :: {:ok, map()} | {:error, term()}
  def snapshot(opts \\ []) do
    page_size = Keyword.get(opts, :page_size, 100)
    max_pages = Keyword.get(opts, :max_pages, 10)
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))

    result =
      if is_integer(page_size) and page_size in 1..1000 and is_integer(max_pages) and
           max_pages in 1..100 and is_integer(now_ms) and now_ms >= 0 do
        counts = %{pending: 0, running: 0, failed: 0, scanned: 0, oldest_created_ms: nil}
        scan(nil, page_size, max_pages, counts, now_ms, opts)
      else
        {:error, :invalid_scan_options}
      end

    case result do
      {:ok, status} -> emit(status, status.complete)
      {:error, _reason} -> emit(%{}, false)
    end

    result
  end

  defp scan(cursor, page_size, pages_left, counts, now_ms, opts) do
    store = Keyword.get(opts, :job_store, JobStore)

    read_opts =
      opts
      |> Keyword.take([:backend, :engine, :timeout, :barrier])
      |> Keyword.put_new(:timeout, 200)
      |> Keyword.put(:consistency, :linearizable)

    with {:ok, %{jobs: jobs, next_cursor: next_cursor}} <-
           store.list_page(cursor, page_size, read_opts) do
      counts = Enum.reduce(jobs, counts, &count/2)

      cond do
        is_nil(next_cursor) -> {:ok, report(counts, nil, true, now_ms)}
        pages_left == 1 -> {:ok, report(counts, next_cursor, false, now_ms)}
        true -> scan(next_cursor, page_size, pages_left - 1, counts, now_ms, opts)
      end
    end
  end

  defp count(%Job{kind: kind, state: state, created_at_ms: created}, counts)
       when kind in [:cross_cluster_put, :cross_cluster_delete] and
              state in [:pending, :running, :failed] do
    oldest =
      if is_nil(counts.oldest_created_ms),
        do: created,
        else: min(created, counts.oldest_created_ms)

    counts
    |> Map.update!(state, &(&1 + 1))
    |> Map.update!(:scanned, &(&1 + 1))
    |> Map.put(:oldest_created_ms, oldest)
  end

  defp count(_job, counts), do: Map.update!(counts, :scanned, &(&1 + 1))

  defp report(counts, cursor, complete, now_ms) do
    age =
      if is_nil(counts.oldest_created_ms), do: 0, else: max(now_ms - counts.oldest_created_ms, 0)

    counts
    |> Map.delete(:oldest_created_ms)
    |> Map.merge(%{complete: complete, next_cursor: cursor, oldest_age_ms: age})
  end

  defp emit(status, complete) do
    :telemetry.execute([:ex_storage_service, :replication, :backlog], status, %{
      complete: complete
    })
  end
end
