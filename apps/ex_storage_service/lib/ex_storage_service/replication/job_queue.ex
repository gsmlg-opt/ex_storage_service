defmodule ExStorageService.Replication.JobQueue do
  @moduledoc """
  A custom job queue backed by Concord KV store.

  Jobs are persisted in Concord with keys of the form `"job:{queue}:{job_id}"`.
  The GenServer maintains an in-memory index and periodically processes pending jobs
  with configurable concurrency.
  """

  use GenServer
  require Logger

  @default_concurrency 4
  @default_poll_interval 1_000
  @default_max_attempts 3

  defmodule Job do
    @moduledoc "Represents a replication job."
    defstruct [
      :id,
      :queue,
      :status,
      :payload,
      :created_at,
      :updated_at,
      attempts: 0,
      max_attempts: 3,
      error: nil
    ]

    @type t :: %__MODULE__{
            id: String.t(),
            queue: :replication | :sync | :gc,
            status: :pending | :running | :completed | :failed,
            payload: map(),
            attempts: non_neg_integer(),
            max_attempts: pos_integer(),
            created_at: String.t(),
            updated_at: String.t() | nil,
            error: String.t() | nil
          }
  end

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Enqueue a new job. Returns `{:ok, job_id}`.

  Options:
    - `:queue` - one of `:replication`, `:sync`, `:gc` (default: `:replication`)
    - `:payload` - the job payload map
    - `:max_attempts` - max retry attempts (default: 3)
  """
  @spec enqueue(keyword()) :: {:ok, String.t()} | {:error, term()}
  def enqueue(opts) do
    GenServer.call(__MODULE__, {:enqueue, opts})
  end

  @doc """
  Trigger immediate processing of pending jobs.
  """
  @spec process_jobs() :: :ok
  def process_jobs do
    GenServer.cast(__MODULE__, :process_jobs)
  end

  @doc """
  Get a job by ID and queue.
  """
  @spec get_job(atom(), String.t()) :: {:ok, Job.t()} | {:error, :not_found}
  def get_job(queue, job_id) do
    GenServer.call(__MODULE__, {:get_job, queue, job_id})
  end

  @doc """
  List all jobs in the dead letter queue.
  """
  @spec list_dead_letter_jobs() :: {:ok, [Job.t()]}
  def list_dead_letter_jobs do
    GenServer.call(__MODULE__, :list_dead_letter_jobs)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    concurrency = Keyword.get(opts, :concurrency, @default_concurrency)
    poll_interval = Keyword.get(opts, :poll_interval, @default_poll_interval)
    processor = Keyword.get(opts, :processor, &default_processor/1)

    state = %{
      concurrency: concurrency,
      poll_interval: poll_interval,
      running: 0,
      processor: processor
    }

    schedule_poll(poll_interval)
    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, opts}, _from, state) do
    queue = Keyword.get(opts, :queue, :replication)
    payload = Keyword.get(opts, :payload, %{})
    max_attempts = Keyword.get(opts, :max_attempts, @default_max_attempts)

    job_id = generate_id()
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    job = %Job{
      id: job_id,
      queue: queue,
      status: :pending,
      payload: payload,
      attempts: 0,
      max_attempts: max_attempts,
      created_at: now,
      updated_at: now
    }

    case persist_job(job) do
      :ok ->
        {:reply, {:ok, job_id}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_job, queue, job_id}, _from, state) do
    key = job_key(queue, job_id)

    case Concord.get(key) do
      {:ok, nil} -> {:reply, {:error, :not_found}, state}
      {:ok, data} -> {:reply, {:ok, to_job(data)}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:list_dead_letter_jobs, _from, state) do
    jobs = load_jobs_by_prefix("job:dead_letter:")
    {:reply, {:ok, jobs}, state}
  end

  @impl true
  def handle_cast(:process_jobs, state) do
    state = do_process_jobs(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:poll, state) do
    state = do_process_jobs(state)
    schedule_poll(state.poll_interval)
    {:noreply, state}
  end

  def handle_info({:job_complete, queue, job_id, result}, state) do
    state = %{state | running: max(state.running - 1, 0)}

    case result do
      :ok ->
        mark_completed(queue, job_id)

      {:error, reason} ->
        handle_job_failure(queue, job_id, reason)
    end

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Private

  defp do_process_jobs(state) do
    available = state.concurrency - state.running

    if available <= 0 do
      state
    else
      pending = load_pending_jobs(available)

      Enum.reduce(pending, state, fn job, acc ->
        run_job(job, acc)
      end)
    end
  end

  defp run_job(job, state) do
    processor = state.processor
    parent = self()
    queue = job.queue
    job_id = job.id

    # Mark as running
    mark_running(queue, job_id)

    Task.start(fn ->
      result =
        try do
          processor.(job)
        rescue
          e -> {:error, Exception.message(e)}
        catch
          :exit, reason -> {:error, inspect(reason)}
        end

      send(parent, {:job_complete, queue, job_id, result})
    end)

    %{state | running: state.running + 1}
  end

  defp mark_running(queue, job_id) do
    key = job_key(queue, job_id)

    case Concord.get(key) do
      {:ok, data} when is_map(data) ->
        now = DateTime.utc_now() |> DateTime.to_iso8601()

        updated =
          data
          |> Map.put(:status, :running)
          |> Map.put(:updated_at, now)
          |> Map.put(:attempts, (data[:attempts] || 0) + 1)

        Concord.put(key, updated)

      _ ->
        :ok
    end
  end

  defp mark_completed(queue, job_id) do
    key = job_key(queue, job_id)

    case Concord.get(key) do
      {:ok, data} when is_map(data) ->
        now = DateTime.utc_now() |> DateTime.to_iso8601()

        updated =
          data
          |> Map.put(:status, :completed)
          |> Map.put(:updated_at, now)

        Concord.put(key, updated)

      _ ->
        :ok
    end
  end

  defp handle_job_failure(queue, job_id, reason) do
    key = job_key(queue, job_id)

    case Concord.get(key) do
      {:ok, data} when is_map(data) ->
        attempts = data[:attempts] || 1
        max_attempts = data[:max_attempts] || @default_max_attempts

        if attempts >= max_attempts do
          move_to_dead_letter(queue, job_id, data, reason)
        else
          # Schedule retry with exponential backoff
          backoff = backoff_ms(attempts)
          now = DateTime.utc_now() |> DateTime.to_iso8601()

          updated =
            data
            |> Map.put(:status, :pending)
            |> Map.put(:updated_at, now)
            |> Map.put(:error, inspect(reason))

          Concord.put(key, updated)

          Logger.warning(
            "Job #{job_id} failed (attempt #{attempts}/#{max_attempts}), retrying in #{backoff}ms"
          )

          Process.send_after(self(), :poll, backoff)
        end

      _ ->
        :ok
    end
  end

  defp move_to_dead_letter(queue, job_id, data, reason) do
    # Remove from original queue
    original_key = job_key(queue, job_id)
    Concord.delete(original_key)

    # Add to dead letter queue
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    dead_data =
      data
      |> Map.put(:status, :failed)
      |> Map.put(:updated_at, now)
      |> Map.put(:error, inspect(reason))
      |> Map.put(:original_queue, queue)

    dead_key = "job:dead_letter:#{job_id}"
    Concord.put(dead_key, dead_data)

    Logger.error("Job #{job_id} moved to dead letter queue after #{data[:max_attempts]} attempts")
  end

  @doc false
  def backoff_ms(attempt) do
    # Exponential backoff: 1s, 2s, 4s, ...
    trunc(:math.pow(2, attempt - 1) * 1_000)
  end

  defp load_pending_jobs(limit) do
    case Concord.get_all() do
      {:ok, all} ->
        all
        |> Enum.filter(fn {k, _v} ->
          String.starts_with?(k, "job:") and not String.starts_with?(k, "job:dead_letter:")
        end)
        |> Enum.map(fn {_k, v} -> to_job(v) end)
        |> Enum.filter(fn job -> job.status == :pending end)
        |> Enum.sort_by(fn job -> job.created_at end)
        |> Enum.take(limit)

      _ ->
        []
    end
  end

  defp load_jobs_by_prefix(prefix) do
    case Concord.get_all() do
      {:ok, all} ->
        all
        |> Enum.filter(fn {k, _v} -> String.starts_with?(k, prefix) end)
        |> Enum.map(fn {_k, v} -> to_job(v) end)

      _ ->
        []
    end
  end

  defp persist_job(%Job{} = job) do
    key = job_key(job.queue, job.id)

    data = %{
      id: job.id,
      queue: job.queue,
      status: job.status,
      payload: job.payload,
      attempts: job.attempts,
      max_attempts: job.max_attempts,
      created_at: job.created_at,
      updated_at: job.updated_at,
      error: job.error
    }

    case Concord.put(key, data) do
      {:ok, _} -> :ok
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp to_job(%Job{} = job), do: job

  defp to_job(data) when is_map(data) do
    %Job{
      id: data[:id] || data["id"],
      queue: to_atom(data[:queue] || data["queue"]),
      status: to_atom(data[:status] || data["status"]),
      payload: data[:payload] || data["payload"] || %{},
      attempts: data[:attempts] || data["attempts"] || 0,
      max_attempts: data[:max_attempts] || data["max_attempts"] || @default_max_attempts,
      created_at: data[:created_at] || data["created_at"],
      updated_at: data[:updated_at] || data["updated_at"],
      error: data[:error] || data["error"]
    }
  end

  defp to_atom(val) when is_atom(val), do: val
  defp to_atom(val) when is_binary(val), do: String.to_existing_atom(val)
  defp to_atom(_), do: nil

  defp job_key(queue, job_id), do: "job:#{queue}:#{job_id}"

  defp generate_id do
    :crypto.strong_rand_bytes(12) |> Base.url_encode64(padding: false)
  end

  defp schedule_poll(interval) do
    Process.send_after(self(), :poll, interval)
  end

  defp default_processor(job) do
    alias ExStorageService.Replication.Worker

    case job.payload do
      %{action: :put, bucket: bucket, key: key, replica: replica} ->
        Worker.replicate_put(bucket, key, to_replica_struct(replica))

      %{action: :delete, bucket: bucket, key: key, replica: replica} ->
        Worker.replicate_delete(bucket, key, to_replica_struct(replica))

      _ ->
        Logger.warning("Unknown job payload: #{inspect(job.payload)}")
        :ok
    end
  end

  defp to_replica_struct(map) when is_map(map) do
    alias ExStorageService.Replication.Config.Replica

    %Replica{
      endpoint: map[:endpoint] || map["endpoint"],
      access_key: map[:access_key] || map["access_key"],
      secret_key_enc: map[:secret_key_enc] || map["secret_key_enc"],
      bucket: map[:bucket] || map["bucket"]
    }
  end
end
