defmodule ExStorageServiceCli.CLI.Signals do
  @moduledoc "Executable-only signal ownership; cancellation leaves cleanup to the transfer owner."
  use GenServer
  alias ExStorageServiceCli.CLI.Signals.Native
  alias ExStorageServiceCli.Transfer.Coordinator

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    pid = self()

    with {:ok, resource} <- Native.start(pid) do
      {:ok,
       %{
         resource: resource,
         coordinator: opts[:coordinator],
         owner: opts[:owner],
         reference: opts[:cancel_reference],
         monitor: Process.monitor(opts[:owner])
       }}
    else
      {:error, error} -> {:stop, error}
    end
  end

  @impl true
  def handle_info({:select, resource, :ess_sigint, :ready_input}, %{resource: resource} = state) do
    case Native.drain(resource) do
      signals when is_list(signals) -> Enum.each(signals, &cancel(state, &1))
      _ -> cancel(state, :sigint)
    end

    {:noreply, state}
  end

  def handle_info({:ess_signal, signal}, state) when signal in [:sigint, :sigterm] do
    cancel(state, signal)
    {:noreply, state}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{monitor: monitor} = state),
    do: {:stop, :normal, state}

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Native.close(state.resource)
    :ok
  end

  defp cancel(state, signal) do
    Coordinator.cancel(state.coordinator, signal)
    send(state.owner, {:ess_cancel, {state.reference, signal}})
    :ok
  end
end
