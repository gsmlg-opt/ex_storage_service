defmodule ExStorageServiceWeb.BucketLive.Index do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Engine

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:buckets, [])
      |> assign(:new_bucket_name, "")

    {:ok, socket}
  end

  @impl true
  def handle_params(_params, _url, socket) do
    {:noreply, load_buckets(socket)}
  end

  @impl true
  def handle_event("create_bucket", %{"name" => name}, socket) do
    name = String.trim(name)

    if name == "" do
      {:noreply, put_flash(socket, :error, "Bucket name cannot be empty")}
    else
      case Metadata.head_bucket(name) do
        :ok ->
          {:noreply, put_flash(socket, :error, "Bucket #{name} already exists")}

        {:error, :not_found} ->
          Engine.ensure_bucket_dirs(name)

          case Metadata.create_bucket(name) do
            :ok ->
              socket =
                socket
                |> put_flash(:info, "Bucket #{name} created")
                |> assign(:new_bucket_name, "")
                |> load_buckets()

              {:noreply, socket}

            {:error, reason} ->
              {:noreply, put_flash(socket, :error, "Failed to create bucket: #{inspect(reason)}")}
          end

        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
      end
    end
  end

  def handle_event("delete_bucket", %{"name" => name}, socket) do
    case Metadata.list_objects(name, max_keys: 1) do
      {:ok, %{keys: []}} ->
        Metadata.delete_bucket(name)
        {:noreply, socket |> put_flash(:info, "Bucket #{name} deleted") |> load_buckets()}

      {:ok, _} ->
        {:noreply, put_flash(socket, :error, "Bucket #{name} is not empty")}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
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
        <div class="card-body">
          <h3 class="card-title text-sm">Create New Bucket</h3>
          <form phx-submit="create_bucket" class="flex items-end gap-3">
            <div class="flex-1 form-group">
              <label for="name" class="form-label">Name</label>
              <input
                type="text"
                name="name"
                id="name"
                value={@new_bucket_name}
                placeholder="Enter bucket name"
                class="input input-primary w-full"
              />
            </div>
            <button type="submit" class="btn btn-primary">Create Bucket</button>
          </form>
        </div>
      </div>

      <div class="mt-6 card">
        <table class="table table-hover w-full">
          <thead>
            <tr>
              <th class="text-on-surface-variant">Name</th>
              <th class="text-on-surface-variant">Created</th>
              <th class="text-on-surface-variant">Actions</th>
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
                <td>
                  <button
                    phx-click="delete_bucket"
                    phx-value-name={bucket.name}
                    data-confirm={"Delete bucket #{bucket.name}? It must be empty."}
                    class="btn btn-ghost btn-xs text-error"
                  >
                    Delete
                  </button>
                </td>
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

  defp load_buckets(socket) do
    buckets =
      case Metadata.list_buckets() do
        {:ok, list} -> list
        _ -> []
      end

    assign(socket, :buckets, buckets)
  end
end
