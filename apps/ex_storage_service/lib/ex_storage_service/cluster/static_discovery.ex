defmodule ExStorageService.Cluster.StaticDiscovery do
  @moduledoc """
  Maintains distributed Erlang connections to an explicit static seed list.

  Discovery only establishes connectivity. Concord membership remains the
  fixed, ordered configuration validated by `InstanceConfig`.
  """

  use GenServer

  @default_interval 5_000

  def start_link(opts) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @impl true
  def init(opts) do
    state = %{
      seeds: opts |> Keyword.fetch!(:seeds) |> Enum.reject(&(&1 == node())),
      connector: Keyword.get(opts, :connector, &Node.connect/1),
      interval: Keyword.get(opts, :interval, @default_interval)
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    connect(state)
    {:noreply, schedule(state)}
  end

  @impl true
  def handle_info(:connect, state) do
    connect(state)
    {:noreply, schedule(state)}
  end

  defp connect(state), do: Enum.each(state.seeds, state.connector)

  defp schedule(state) do
    Process.send_after(self(), :connect, state.interval)
    state
  end
end
