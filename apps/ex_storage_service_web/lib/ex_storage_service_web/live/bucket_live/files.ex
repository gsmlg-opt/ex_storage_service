defmodule ExStorageServiceWeb.BucketLive.Files do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Engine

  @impl true
  def mount(%{"name" => name}, _session, socket) do
    case Metadata.get_bucket(name) do
      {:ok, _bucket} ->
        {:ok,
         socket
         |> assign(bucket_name: name, objects: [])
         |> load_objects()}

      {:error, :not_found} ->
        {:ok, socket |> put_flash(:error, "Bucket not found") |> redirect(to: ~p"/buckets")}
    end
  end

  @impl true
  def handle_params(_params, _url, socket), do: {:noreply, socket}

  @impl true
  def handle_event("delete_object", %{"key" => key}, socket) do
    bucket = socket.assigns.bucket_name

    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        Metadata.delete_object_meta(bucket, key)
        Engine.delete_content(bucket, meta.content_hash)

        {:noreply,
         socket
         |> put_flash(:info, "Deleted \"#{key}\"")
         |> load_objects()}

      {:error, :not_found} ->
        {:noreply, put_flash(socket, :error, "Object not found")}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="max-w-4xl mx-auto">
      <%!-- Breadcrumb --%>
      <div class="flex items-center gap-2 text-sm text-on-surface-variant mb-1">
        <.dm_link navigate={~p"/buckets"} class="text-primary">Buckets</.dm_link>
        <span>/</span>
        <.dm_link navigate={~p"/buckets/#{@bucket_name}"} class="text-primary">
          {@bucket_name}
        </.dm_link>
        <span>/</span>
        <span>Files</span>
      </div>

      <%!-- Header --%>
      <div class="flex items-center justify-between mt-2 mb-4">
        <h1 class="text-2xl font-bold text-on-surface">{@bucket_name}</h1>
        <span class="text-sm text-on-surface-variant">
          {length(@objects)} object{if length(@objects) != 1, do: "s", else: ""}
        </span>
      </div>

      <%!-- Sub-nav tabs --%>
      <div class="flex gap-1 border-b border-outline-variant mb-6">
        <.dm_link
          navigate={~p"/buckets/#{@bucket_name}"}
          class="px-4 py-2 text-sm font-medium text-on-surface-variant hover:text-on-surface"
        >
          Overview
        </.dm_link>
        <.dm_link
          navigate={~p"/buckets/#{@bucket_name}/files"}
          class="px-4 py-2 text-sm font-medium border-b-2 border-primary text-primary -mb-px"
        >
          Files
        </.dm_link>
        <.dm_link
          navigate={~p"/buckets/#{@bucket_name}/settings"}
          class="px-4 py-2 text-sm font-medium text-on-surface-variant hover:text-on-surface"
        >
          Settings
        </.dm_link>
      </div>

      <%!-- Objects table --%>
      <div class="card">
        <table class="table table-hover w-full">
          <thead>
            <tr>
              <th class="text-on-surface-variant">Key</th>
              <th class="text-on-surface-variant">Size</th>
              <th class="text-on-surface-variant">Last Modified</th>
              <th class="text-on-surface-variant text-right">Actions</th>
            </tr>
          </thead>
          <tbody>
            <%= for obj <- @objects do %>
              <tr>
                <td class="font-mono text-sm">{obj.key}</td>
                <td class="text-on-surface-variant text-sm">{format_size(obj[:size] || 0)}</td>
                <td class="text-on-surface-variant text-sm">
                  {obj[:updated_at] || obj[:created_at]}
                </td>
                <td class="text-right">
                  <button
                    phx-click="delete_object"
                    phx-value-key={obj.key}
                    data-confirm={"Delete \"#{obj.key}\"?"}
                    class="btn btn-ghost btn-xs text-error hover:btn-error"
                  >
                    Delete
                  </button>
                </td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @objects == [] do %>
          <div class="flex flex-col items-center gap-2 px-6 py-16 text-on-surface-variant">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              class="w-10 h-10 opacity-30"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="1.5"
            >
              <path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z" />
            </svg>
            <p class="text-sm">No objects in this bucket.</p>
          </div>
        <% end %>
      </div>
    </div>
    """
  end

  defp load_objects(socket) do
    case Metadata.list_objects(socket.assigns.bucket_name) do
      {:ok, %{keys: keys}} ->
        objects = Enum.map(keys, fn {key, meta} -> Map.put(meta, :key, key) end)
        assign(socket, :objects, objects)

      _ ->
        assign(socket, :objects, [])
    end
  end

  defp format_size(bytes) when bytes < 1024, do: "#{bytes} B"
  defp format_size(bytes) when bytes < 1_048_576, do: "#{Float.round(bytes / 1024, 1)} KB"
  defp format_size(bytes), do: "#{Float.round(bytes / 1_048_576, 1)} MB"
end
