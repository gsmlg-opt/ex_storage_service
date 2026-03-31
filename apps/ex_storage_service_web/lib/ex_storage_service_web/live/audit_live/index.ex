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

    {:noreply, push_patch(socket, to: ~p"/audit?#{%{page: next_page}}")}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <.header>
        Audit Log
        <:subtitle>IAM activity log</:subtitle>
      </.header>

      <div class="mt-6 card">
        <table class="table table-hover w-full">
          <thead>
            <tr>
              <th class="text-on-surface-variant">Timestamp</th>
              <th class="text-on-surface-variant">Actor</th>
              <th class="text-on-surface-variant">Action</th>
              <th class="text-on-surface-variant">Target</th>
              <th class="text-on-surface-variant">Details</th>
            </tr>
          </thead>
          <tbody>
            <%= for event <- @events do %>
              <tr>
                <td class="text-sm text-on-surface-variant whitespace-nowrap">{event.timestamp}</td>
                <td class="text-sm text-on-surface font-mono">{event.actor}</td>
                <td>
                  <span class="badge badge-info">{event.action}</span>
                </td>
                <td class="text-sm text-on-surface-variant font-mono">{event.target}</td>
                <td class="text-sm text-on-surface-variant">
                  <%= if event.details != %{} do %>
                    <code class="text-xs bg-surface-container px-2 py-1 rounded">
                      {inspect(event.details)}
                    </code>
                  <% else %>
                    <span class="opacity-40">-</span>
                  <% end %>
                </td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @events == [] do %>
          <p class="px-6 py-8 text-center text-on-surface-variant">No audit events recorded.</p>
        <% end %>
      </div>

      <%= if @has_more do %>
        <div class="mt-4 text-center">
          <button phx-click="load_more" class="btn btn-secondary btn-sm">Load More</button>
        </div>
      <% end %>

      <%= if @page > 1 do %>
        <div class="mt-4 text-center">
          <.link patch={~p"/audit?#{%{page: @page - 1}}"} class="btn btn-secondary btn-sm">
            Previous Page
          </.link>
          <span class="mx-4 text-sm text-on-surface-variant">Page {@page}</span>
          <%= if @has_more do %>
            <.link patch={~p"/audit?#{%{page: @page + 1}}"} class="btn btn-secondary btn-sm">
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
