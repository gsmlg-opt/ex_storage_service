defmodule ExStorageServiceWeb.BucketLive.Show do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.Metadata

  @impl true
  def mount(%{"name" => name}, _session, socket) do
    case Metadata.get_bucket(name) do
      {:ok, bucket} ->
        {:ok, assign(socket, bucket: bucket, bucket_name: name, objects: [])}

      {:error, :not_found} ->
        {:ok, socket |> put_flash(:error, "Bucket not found") |> redirect(to: ~p"/buckets")}
    end
  end

  @impl true
  def handle_params(_params, _url, socket) do
    case Metadata.list_objects(socket.assigns.bucket_name) do
      {:ok, result} ->
        objects =
          Enum.map(result.keys, fn {key, meta} ->
            Map.put(meta, :key, key)
          end)

        {:noreply, assign(socket, :objects, objects)}

      _ ->
        {:noreply, socket}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="max-w-4xl mx-auto p-6">
      <.link navigate={~p"/buckets"} class="text-blue-600 hover:underline text-sm">&larr; Back to Buckets</.link>
      <h1 class="text-2xl font-bold mb-6 mt-2">{@bucket_name}</h1>

      <div class="bg-white shadow rounded-lg">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Key</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Size</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Last Modified</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <%= for obj <- @objects do %>
              <tr>
                <td class="px-6 py-4 font-mono text-sm">{obj.key}</td>
                <td class="px-6 py-4 text-gray-500">{format_size(obj[:size] || 0)}</td>
                <td class="px-6 py-4 text-gray-500">{obj[:updated_at] || obj[:created_at]}</td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @objects == [] do %>
          <p class="px-6 py-8 text-center text-gray-400">No objects in this bucket.</p>
        <% end %>
      </div>
    </div>
    """
  end

  defp format_size(bytes) when bytes < 1024, do: "#{bytes} B"
  defp format_size(bytes) when bytes < 1024 * 1024, do: "#{Float.round(bytes / 1024, 1)} KB"
  defp format_size(bytes), do: "#{Float.round(bytes / (1024 * 1024), 1)} MB"
end
