defmodule ExStorageService.Cluster.NodeRegistrar do
  @moduledoc """
  Registers one persistent node generation after Concord becomes available.

  Failed startup registrations retry on a bounded timer. Once registration
  succeeds the process stays idle and does not emit heartbeat writes.
  """

  use GenServer

  alias ExStorageService.Cluster.Membership

  @default_retry_interval 1_000

  def start_link(opts) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @spec registered?(GenServer.server()) :: boolean()
  def registered?(server \\ __MODULE__), do: GenServer.call(server, :registered?)

  @impl true
  def init(opts) do
    state = %{
      config: Keyword.fetch!(opts, :config),
      membership: Keyword.get(opts, :membership, Membership),
      membership_opts:
        Keyword.take(opts, [
          :backend,
          :timeout,
          :engine,
          :barrier,
          :timestamp,
          :replace_control_state
        ]),
      retry_interval: Keyword.get(opts, :retry_interval, @default_retry_interval),
      registered?: false
    }

    {:ok, state, {:continue, :register}}
  end

  @impl true
  def handle_continue(:register, state), do: register(state)

  @impl true
  def handle_info(:register, %{registered?: false} = state), do: register(state)

  def handle_info(:register, state), do: {:noreply, state}

  @impl true
  def handle_call(:registered?, _from, state), do: {:reply, state.registered?, state}

  defp register(state) do
    case state.membership.register(state.config, state.membership_opts) do
      :ok ->
        {:noreply, %{state | registered?: true}}

      {:error, _reason} ->
        Process.send_after(self(), :register, state.retry_interval)
        {:noreply, state}
    end
  end
end
