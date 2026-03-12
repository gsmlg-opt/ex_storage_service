defmodule ExStorageServiceWeb.AuditLive.Index do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.IAM.Audit

  @page_size 50

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:page_title, "Audit Log")
      |> assign(:events, [])
      |> assign(:page, 1)
      |> assign(:has_more, false)

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _url, socket) do
    page = parse_page(params["page"])

    {:noreply, socket |> assign(:page, page) |> load_events()}
  end

  @impl true
  def handle_event("load_more", _params, socket) do
    next_page = socket.assigns.page + 1

    {:noreply,
     push_patch(socket, to: ~p"/audit?#{%{page: next_page}}")}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <.header>
        Audit Log
        <:subtitle>IAM activity log</:subtitle>
      </.header>

      <div class="mt-6 bg-white shadow rounded-lg overflow-hidden">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Timestamp</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actor</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Action</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Target</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Details</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <%= for event <- @events do %>
              <tr>
                <td class="px-6 py-4 text-sm text-gray-500 whitespace-nowrap">{event.timestamp}</td>
                <td class="px-6 py-4 text-sm text-gray-900 font-mono">{event.actor}</td>
                <td class="px-6 py-4">
                  <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                    {event.action}
                  </span>
                </td>
                <td class="px-6 py-4 text-sm text-gray-500 font-mono">{event.target}</td>
                <td class="px-6 py-4 text-sm text-gray-500">
                  <%= if event.details != %{} do %>
                    <code class="text-xs bg-gray-50 px-2 py-1 rounded">{inspect(event.details)}</code>
                  <% else %>
                    <span class="text-gray-400">-</span>
                  <% end %>
                </td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @events == [] do %>
          <p class="px-6 py-8 text-center text-gray-400">No audit events recorded.</p>
        <% end %>
      </div>

      <%= if @has_more do %>
        <div class="mt-4 text-center">
          <button
            phx-click="load_more"
            class="px-4 py-2 bg-gray-100 text-gray-700 text-sm font-medium rounded-md hover:bg-gray-200"
          >
            Load More
          </button>
        </div>
      <% end %>

      <%= if @page > 1 do %>
        <div class="mt-4 text-center">
          <.link
            patch={~p"/audit?#{%{page: @page - 1}}"}
            class="px-4 py-2 bg-gray-100 text-gray-700 text-sm font-medium rounded-md hover:bg-gray-200 inline-block"
          >
            Previous Page
          </.link>
          <span class="mx-4 text-sm text-gray-500">Page {@page}</span>
          <%= if @has_more do %>
            <.link
              patch={~p"/audit?#{%{page: @page + 1}}"}
              class="px-4 py-2 bg-gray-100 text-gray-700 text-sm font-medium rounded-md hover:bg-gray-200 inline-block"
            >
              Next Page
            </.link>
          <% end %>
        </div>
      <% end %>
    </div>
    """
  end

  defp load_events(socket) do
    page = socket.assigns.page
    limit = @page_size * page + 1

    events =
      case Audit.list_events(limit: limit) do
        {:ok, events} -> events
        _ -> []
      end

    has_more = length(events) > @page_size * page
    visible = Enum.take(events, @page_size * page)

    socket
    |> assign(:events, visible)
    |> assign(:has_more, has_more)
  end

  defp parse_page(nil), do: 1

  defp parse_page(page_str) do
    case Integer.parse(page_str) do
      {n, _} when n > 0 -> n
      _ -> 1
    end
  end
end
