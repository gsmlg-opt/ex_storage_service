defmodule ExStorageServiceWeb.DashboardLive do
  use ExStorageServiceWeb, :live_view

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:page_title, "Dashboard")
      |> assign(:bucket_count, 0)
      |> assign(:object_count, 0)
      |> assign(:disk_usage, "0 B")

    if connected?(socket) do
      send(self(), :load_stats)
    end

    {:ok, socket}
  end

  @impl true
  def handle_info(:load_stats, socket) do
    # TODO: Wire up to actual storage backend
    socket =
      socket
      |> assign(:bucket_count, 0)
      |> assign(:object_count, 0)
      |> assign(:disk_usage, "0 B")

    {:noreply, socket}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <.header>
        Storage Dashboard
        <:subtitle>Overview of your storage service</:subtitle>
      </.header>

      <div class="mt-8 grid grid-cols-1 gap-6 sm:grid-cols-3">
        <div class="bg-white overflow-hidden shadow rounded-lg">
          <div class="p-5">
            <div class="flex items-center">
              <div class="flex-shrink-0">
                <div class="h-10 w-10 rounded-md bg-indigo-500 flex items-center justify-center">
                  <span class="text-white text-lg font-bold">B</span>
                </div>
              </div>
              <div class="ml-5 w-0 flex-1">
                <dl>
                  <dt class="text-sm font-medium text-gray-500 truncate">Total Buckets</dt>
                  <dd class="text-2xl font-semibold text-gray-900">{@bucket_count}</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div class="bg-white overflow-hidden shadow rounded-lg">
          <div class="p-5">
            <div class="flex items-center">
              <div class="flex-shrink-0">
                <div class="h-10 w-10 rounded-md bg-green-500 flex items-center justify-center">
                  <span class="text-white text-lg font-bold">O</span>
                </div>
              </div>
              <div class="ml-5 w-0 flex-1">
                <dl>
                  <dt class="text-sm font-medium text-gray-500 truncate">Total Objects</dt>
                  <dd class="text-2xl font-semibold text-gray-900">{@object_count}</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div class="bg-white overflow-hidden shadow rounded-lg">
          <div class="p-5">
            <div class="flex items-center">
              <div class="flex-shrink-0">
                <div class="h-10 w-10 rounded-md bg-yellow-500 flex items-center justify-center">
                  <span class="text-white text-lg font-bold">D</span>
                </div>
              </div>
              <div class="ml-5 w-0 flex-1">
                <dl>
                  <dt class="text-sm font-medium text-gray-500 truncate">Disk Usage</dt>
                  <dd class="text-2xl font-semibold text-gray-900">{@disk_usage}</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    """
  end
end
