defmodule ExStorageService.Storage.MultipartGC do
  @moduledoc """
  Garbage collector for stale multipart uploads.

  Periodically scans for incomplete multipart uploads that are older than
  the configured maximum age and aborts them, cleaning up part files and
  metadata.

  Configuration:
  - `:multipart_gc_interval` — scan interval in milliseconds (default: 3_600_000 = 1 hour)
  - `:multipart_max_age` — max age in seconds for incomplete uploads (default: 86_400 = 24 hours)
  """

  use GenServer

  require Logger

  alias ExStorageService.Storage.Multipart

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    interval = Application.get_env(:ex_storage_service, :multipart_gc_interval, 3_600_000)
    max_age = Application.get_env(:ex_storage_service, :multipart_max_age, 86_400)

    schedule_sweep(interval)

    {:ok, %{interval: interval, max_age: max_age}}
  end

  @impl true
  def handle_info(:sweep, state) do
    sweep(state.max_age)
    schedule_sweep(state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Public for testing
  @doc false
  def sweep(max_age_seconds) do
    case Multipart.list_active_uploads() do
      {:ok, uploads} ->
        now = DateTime.utc_now()

        Enum.each(uploads, fn upload ->
          case DateTime.from_iso8601(upload.created_at) do
            {:ok, created_at, _} ->
              age = DateTime.diff(now, created_at, :second)

              if age > max_age_seconds do
                Logger.info(
                  "MultipartGC: aborting stale upload #{upload.upload_id} for #{upload.bucket}/#{upload.key} (age: #{age}s)"
                )

                Multipart.abort_upload(upload.bucket, upload.upload_id)
              end

            _ ->
              :ok
          end
        end)

      {:error, reason} ->
        Logger.warning("MultipartGC: failed to list uploads: #{inspect(reason)}")
    end
  end

  defp schedule_sweep(interval) do
    Process.send_after(self(), :sweep, interval)
  end
end
