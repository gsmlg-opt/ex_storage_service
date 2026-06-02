defmodule ExStorageServiceWeb.BucketLive.Index do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.BucketValidator
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Engine

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:page_title, "Buckets")
      |> assign(:buckets, [])
      |> assign(:new_bucket_name, "")
      |> assign(:create_error, nil)
      |> assign(:show_create_modal, false)

    {:ok, socket}
  end

  @impl true
  def handle_params(_params, _url, socket) do
    {:noreply, load_buckets(socket)}
  end

  @impl true
  def handle_event("open_create_modal", _params, socket) do
    {:noreply, assign(socket, show_create_modal: true, new_bucket_name: "", create_error: nil)}
  end

  def handle_event("close_create_modal", _params, socket) do
    {:noreply, assign(socket, show_create_modal: false, create_error: nil)}
  end

  def handle_event("validate_bucket_name", %{"name" => name}, socket) do
    error =
      case String.trim(name) do
        "" ->
          nil

        trimmed ->
          case BucketValidator.validate(trimmed) do
            :ok -> nil
            {:error, msg} -> msg
          end
      end

    {:noreply, assign(socket, :create_error, error)}
  end

  def handle_event("create_bucket", %{"name" => name}, socket) do
    name = String.trim(name)

    with :ok <- validate_nonempty(name),
         :ok <- BucketValidator.validate(name),
         {:error, :not_found} <- Metadata.head_bucket(name) do
      Engine.ensure_bucket_dirs(name)

      case Metadata.create_bucket(name) do
        :ok ->
          socket =
            socket
            |> put_flash(:info, "Bucket \"#{name}\" created successfully")
            |> assign(:new_bucket_name, "")
            |> assign(:create_error, nil)
            |> assign(:show_create_modal, false)
            |> load_buckets()

          {:noreply, socket}

        {:error, reason} ->
          {:noreply, assign(socket, :create_error, "Failed to create bucket: #{inspect(reason)}")}
      end
    else
      {:error, msg} when is_binary(msg) ->
        {:noreply, assign(socket, :create_error, msg)}

      :ok ->
        {:noreply, assign(socket, :create_error, "Bucket \"#{name}\" already exists")}

      {:error, reason} ->
        {:noreply, assign(socket, :create_error, "Error: #{inspect(reason)}")}
    end
  end

  def handle_event("delete_bucket", %{"name" => name}, socket) do
    case Metadata.list_objects(name, max_keys: 1) do
      {:ok, %{keys: []}} ->
        Metadata.delete_bucket(name)
        {:noreply, socket |> put_flash(:info, "Bucket \"#{name}\" deleted") |> load_buckets()}

      {:ok, _} ->
        {:noreply, put_flash(socket, :error, "Bucket \"#{name}\" is not empty")}

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
        <:subtitle>Manage S3 storage buckets</:subtitle>
        <:actions>
          <button
            id="open-create-bucket-btn"
            type="button"
            class="btn btn-primary btn-sm gap-2"
            phx-click="open_create_modal"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              class="w-4 h-4"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
            </svg>
            Create Bucket
          </button>
        </:actions>
      </.header>

      <%!-- Modal overlay: pure LiveView, no Shadow DOM, phx-submit works --%>
      <div
        :if={@show_create_modal}
        id="create-bucket-overlay"
        class="fixed inset-0 z-50 flex items-center justify-center"
        phx-key="Escape"
        phx-window-keydown="close_create_modal"
      >
        <%!-- Backdrop --%>
        <div
          class="absolute inset-0 bg-black/50 backdrop-blur-sm"
          phx-click="close_create_modal"
        >
        </div>

        <%!-- Dialog card --%>
        <div class="relative w-full max-w-md mx-4 card shadow-2xl">
          <div class="card-body p-6 flex flex-col gap-4">
            <div class="flex items-center justify-between">
              <h2 id="create-bucket-title" class="text-lg font-semibold">Create New Bucket</h2>
              <button
                type="button"
                class="btn btn-ghost btn-sm btn-circle"
                phx-click="close_create_modal"
                aria-label="Close"
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  class="w-4 h-4"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  stroke-width="2"
                  stroke-linecap="round"
                  stroke-linejoin="round"
                >
                  <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>

            <form
              id="create-bucket-form"
              phx-submit="create_bucket"
              phx-change="validate_bucket_name"
              class="flex flex-col gap-4"
            >
              <div class="form-group">
                <label for="bucket-name" class="form-label font-medium">Bucket Name</label>
                <input
                  type="text"
                  name="name"
                  id="bucket-name"
                  value={@new_bucket_name}
                  placeholder="e.g. my-data-bucket"
                  class={"input w-full #{if @create_error, do: "input-error", else: "input-primary"}"}
                  autocomplete="off"
                  phx-debounce="300"
                  autofocus
                />
                <p :if={@create_error} class="mt-1 text-xs text-error">
                  {@create_error}
                </p>
                <p class="mt-1 text-xs text-on-surface-variant">
                  3–63 characters, lowercase letters, numbers, hyphens, and dots only.
                </p>
              </div>

              <div class="flex justify-end gap-2">
                <button type="button" class="btn btn-ghost btn-sm" phx-click="close_create_modal">
                  Cancel
                </button>
                <button
                  type="submit"
                  id="create-bucket-submit"
                  class="btn btn-primary btn-sm"
                  disabled={@create_error != nil}
                >
                  Create
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>

      <%!-- Bucket table --%>
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
                  <.link
                    navigate={~p"/buckets/#{bucket.name}"}
                    class="text-primary hover:underline font-medium"
                  >
                    {bucket.name}
                  </.link>
                </td>
                <td class="text-on-surface-variant text-sm">{bucket.creation_date}</td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @buckets == [] do %>
          <div class="flex flex-col items-center gap-3 px-6 py-16 text-on-surface-variant">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              class="w-12 h-12 opacity-30"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="1.5"
            >
              <ellipse cx="12" cy="5" rx="9" ry="3" />
              <path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5" />
              <path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3" />
            </svg>
            <p class="text-sm">No buckets yet.</p>
            <button
              id="empty-state-create-btn"
              type="button"
              class="btn btn-primary btn-sm"
              phx-click="open_create_modal"
            >
              Create your first bucket
            </button>
          </div>
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

  defp validate_nonempty(""), do: {:error, "Bucket name cannot be empty"}
  defp validate_nonempty(_), do: :ok
end
