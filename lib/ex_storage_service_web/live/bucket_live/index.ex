defmodule ExStorageServiceWeb.BucketLive.Index do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.Metadata

  @impl true
  def mount(_params, _session, socket) do
    {:ok, assign(socket, :buckets, [])}
  end

  @impl true
  def handle_params(_params, _url, socket) do
    buckets =
      case Metadata.list_buckets() do
        {:ok, list} -> list
        _ -> []
      end

    {:noreply, assign(socket, :buckets, buckets)}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="max-w-4xl mx-auto p-6">
      <h1 class="text-2xl font-bold mb-6">Buckets</h1>
      <div class="bg-white shadow rounded-lg">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Created</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <%= for bucket <- @buckets do %>
              <tr>
                <td class="px-6 py-4">
                  <.link navigate={~p"/buckets/#{bucket.name}"} class="text-blue-600 hover:underline">
                    {bucket.name}
                  </.link>
                </td>
                <td class="px-6 py-4 text-gray-500">{bucket.creation_date}</td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @buckets == [] do %>
          <p class="px-6 py-8 text-center text-gray-400">No buckets yet.</p>
        <% end %>
      </div>
    </div>
    """
  end
end
