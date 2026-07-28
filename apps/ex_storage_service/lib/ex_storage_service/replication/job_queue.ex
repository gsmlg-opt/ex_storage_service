defmodule ExStorageService.Replication.JobQueue do
  @moduledoc """
  Compatibility facade over the fenced v2 durable outbox.

  New code should use `ExStorageService.Metadata.Outbox`,
  `ExStorageService.Metadata.JobStore`, and
  `ExStorageService.Cluster.Outbox.Dispatcher` directly.
  """

  alias ExStorageService.Cluster.Outbox.Dispatcher
  alias ExStorageService.Metadata.{JobStore, Outbox}
  alias ExStorageService.Names

  defmodule Job do
    @moduledoc "Compatibility view of a v2 durable job."
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
            id: binary() | nil,
            queue: atom() | nil,
            status: atom() | nil,
            payload: map() | nil,
            attempts: non_neg_integer(),
            max_attempts: pos_integer(),
            created_at: term(),
            updated_at: term(),
            error: term()
          }
  end

  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Agent.start_link(fn -> :compatibility_facade end, name: name)
  end

  @spec enqueue(keyword()) :: {:ok, binary()} | {:error, term()}
  def enqueue(opts), do: enqueue(__MODULE__, opts)

  @spec enqueue(GenServer.server(), keyword()) :: {:ok, binary()} | {:error, term()}
  def enqueue(_server, opts) do
    job_id = Keyword.get_lazy(opts, :job_id, &generated_id/0)
    operation_id = Keyword.get(opts, :operation_id, "legacy-job-" <> job_id)
    payload = Keyword.get(opts, :payload, %{})

    event = %{
      id: job_id,
      kind: event_kind(payload),
      state: :pending,
      payload: payload,
      max_attempts: Keyword.get(opts, :max_attempts, 3)
    }

    case Outbox.enqueue_legacy([event], operation_id: operation_id) do
      :ok -> {:ok, job_id}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec process_jobs() :: :ok
  def process_jobs, do: process_jobs(default_dispatcher())

  @spec process_jobs(GenServer.server()) :: :ok
  def process_jobs(server) do
    if process_alive?(server), do: Dispatcher.process_jobs(server)
    :ok
  end

  @spec get_job(atom(), binary()) :: {:ok, Job.t()} | {:error, :not_found | term()}
  def get_job(queue, job_id), do: get_job(__MODULE__, queue, job_id)

  @spec get_job(GenServer.server(), atom(), binary()) ::
          {:ok, Job.t()} | {:error, :not_found | term()}
  def get_job(_server, queue, job_id) do
    with {:ok, job} <- JobStore.get(job_id) do
      {:ok, compatibility_job(job, queue)}
    end
  end

  @spec list_dead_letter_jobs() :: {:ok, [Job.t()]} | {:error, term()}
  def list_dead_letter_jobs, do: list_dead_letter_jobs(__MODULE__)

  @spec list_dead_letter_jobs(GenServer.server()) :: {:ok, [Job.t()]} | {:error, term()}
  def list_dead_letter_jobs(_server) do
    with {:ok, %{jobs: jobs}} <- JobStore.list_page(nil, 1_000) do
      {:ok,
       jobs
       |> Enum.filter(&(&1.state == :failed))
       |> Enum.map(&compatibility_job(&1, :dead_letter))}
    end
  end

  @doc false
  def backoff_ms(attempt), do: Integer.pow(2, max(attempt - 1, 0)) * 1_000

  defp compatibility_job(job, queue) do
    %Job{
      id: job.job_id,
      queue: queue,
      status: job.state,
      payload: job.payload,
      attempts: job.attempts,
      max_attempts: job.max_attempts,
      created_at: job.created_at_ms,
      updated_at: job.updated_at_ms,
      error: job.last_error
    }
  end

  defp event_kind(payload) do
    case Map.get(payload, :action, Map.get(payload, "action")) do
      action when action in [:put, "put"] -> :cross_cluster_put
      action when action in [:delete, "delete"] -> :cross_cluster_delete
      action when action in [:cleanup, "cleanup"] -> :cleanup
      _other -> :cleanup
    end
  end

  defp default_dispatcher, do: Names.process(:default, :outbox_dispatcher, Dispatcher)

  defp process_alive?(pid) when is_pid(pid), do: Process.alive?(pid)

  defp process_alive?(name) when is_atom(name), do: is_pid(Process.whereis(name))

  defp process_alive?({:via, module, term}) do
    case module.whereis_name(term) do
      pid when is_pid(pid) -> Process.alive?(pid)
      _other -> false
    end
  end

  defp generated_id,
    do: Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
end
