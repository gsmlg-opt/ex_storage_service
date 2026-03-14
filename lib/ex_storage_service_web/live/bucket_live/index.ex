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
    <div class="max-w-4xl mx-auto">
      <.header>
        Buckets
        <:subtitle>Manage storage buckets</:subtitle>
      </.header>
      <div class="mt-6 card">
        <table class="table table-hover w-full">
          <thead>
            <tr>
              <th class="text-on-surface-variant">Name</th>
              <th class="text-on-surface-variant">Created</th>
            </tr>
          </thead>
          <tbody>
            <%= for bucket <- @buckets do %>
              <tr>
                <td>
                  <.link navigate={~p"/buckets/#{bucket.name}"} class="text-primary hover:underline">
                    {bucket.name}
                  </.link>
                </td>
                <td class="text-on-surface-variant">{bucket.creation_date}</td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @buckets == [] do %>
          <p class="px-6 py-8 text-center text-on-surface-variant">No buckets yet.</p>
        <% end %>
      </div>
    </div>
    """
  end
end
