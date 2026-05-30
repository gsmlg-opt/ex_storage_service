defmodule ExStorageServiceWeb.UserLive.Index do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.IAM.User
  alias ExStorageService.IAM.AccessKey
  alias ExStorageService.IAM.Audit

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:page_title, "Users")
      |> assign(:users, [])
      |> assign(:new_user_name, "")
      |> assign(:create_error, nil)
      |> assign(:show_create_modal, false)
      |> assign(
        show_confirm_modal: false,
        confirm_title: "",
        confirm_message: "",
        confirm_event: "",
        confirm_params: %{},
        confirm_label: "Confirm",
        confirm_style: "error"
      )

    {:ok, socket}
  end

  @impl true
  def handle_params(_params, _url, socket) do
    {:noreply, load_users(socket)}
  end

  @impl true
  def handle_event("open_create_modal", _params, socket) do
    {:noreply, assign(socket, show_create_modal: true, new_user_name: "", create_error: nil)}
  end

  def handle_event("close_create_modal", _params, socket) do
    {:noreply, assign(socket, show_create_modal: false, create_error: nil)}
  end

  def handle_event("create_user", %{"name" => name}, socket) do
    name = String.trim(name)

    if name == "" do
      {:noreply, assign(socket, :create_error, "User name cannot be empty")}
    else
      case User.create_user(name) do
        {:ok, user} ->
          Audit.log_event("root", :create_user, user.id, %{name: user.name})

          socket =
            socket
            |> put_flash(:info, "User #{user.name} created (ID: #{user.id})")
            |> assign(:new_user_name, "")
            |> assign(:create_error, nil)
            |> assign(:show_create_modal, false)
            |> load_users()

          {:noreply, socket}

        {:error, reason} ->
          {:noreply, assign(socket, :create_error, "Failed to create user: #{inspect(reason)}")}
      end
    end
  end

  def handle_event("open_confirm_modal", params, socket) do
    {title, message, event, label, style} =
      case params["action"] do
        "suspend" ->
          {"Suspend User", "Suspend this user?", "confirm_suspend_user", "Suspend", "warning"}

        "delete" ->
          {"Delete User", "Permanently delete this user? This cannot be undone.",
           "confirm_delete_user", "Delete", "error"}

        _ ->
          {"Confirm", "Are you sure?", "", "Confirm", "error"}
      end

    {:noreply,
     assign(socket,
       show_confirm_modal: true,
       confirm_title: title,
       confirm_message: message,
       confirm_event: event,
       confirm_params: %{"id" => params["id"]},
       confirm_label: label,
       confirm_style: style
     )}
  end

  def handle_event("close_confirm_modal", _params, socket) do
    {:noreply, assign(socket, show_confirm_modal: false)}
  end

  def handle_event("confirm_suspend_user", %{"id" => user_id}, socket) do
    case User.suspend_user(user_id) do
      {:ok, _} ->
        Audit.log_event("root", :suspend_user, user_id)

        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:info, "User suspended")
         |> load_users()}

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:error, "Failed: #{inspect(reason)}")}
    end
  end

  def handle_event("confirm_delete_user", %{"id" => user_id}, socket) do
    user_name =
      case User.get_user(user_id) do
        {:ok, user} -> user.name
        _ -> user_id
      end

    case User.delete_user(user_id) do
      :ok ->
        Audit.log_event("root", :delete_user, user_id, %{name: user_name})

        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:info, "User #{user_name} deleted")
         |> load_users()}

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:error, "Failed to delete user: #{inspect(reason)}")}
    end
  end

  def handle_event("activate_user", %{"id" => user_id}, socket) do
    case User.activate_user(user_id) do
      {:ok, _} ->
        Audit.log_event("root", :activate_user, user_id)
        {:noreply, socket |> put_flash(:info, "User activated") |> load_users()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="max-w-4xl mx-auto">
      <.header>
        Users
        <:subtitle>Manage IAM users</:subtitle>
        <:actions>
          <button
            id="open-create-user-btn"
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
            Create User
          </button>
        </:actions>
      </.header>

      <%!-- Modal overlay --%>
      <div
        :if={@show_create_modal}
        id="create-user-overlay"
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
              <h2 id="create-user-title" class="text-lg font-semibold">Create New User</h2>
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
              id="create-user-form"
              phx-submit="create_user"
              class="flex flex-col gap-4"
            >
              <div class="form-group">
                <label for="user-name" class="form-label font-medium">User Name</label>
                <input
                  type="text"
                  name="name"
                  id="user-name"
                  value={@new_user_name}
                  placeholder="Enter user name"
                  class={"input w-full #{if @create_error, do: "input-error", else: "input-primary"}"}
                  autocomplete="off"
                  autofocus
                />
                <p :if={@create_error} class="mt-1 text-xs text-error">
                  {@create_error}
                </p>
              </div>

              <div class="flex justify-end gap-2">
                <button type="button" class="btn btn-ghost btn-sm" phx-click="close_create_modal">
                  Cancel
                </button>
                <button
                  type="submit"
                  id="create-user-submit"
                  class="btn btn-primary btn-sm"
                >
                  Create
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>

      <%!-- User table --%>
      <div class="mt-6 card">
        <table class="table table-hover w-full">
          <thead>
            <tr>
              <th class="text-on-surface-variant">Name</th>
              <th class="text-on-surface-variant">ID</th>
              <th class="text-on-surface-variant">Status</th>
              <th class="text-on-surface-variant">Keys</th>
              <th class="text-on-surface-variant">Actions</th>
            </tr>
          </thead>
          <tbody>
            <%= for user <- @users do %>
              <tr>
                <td>
                  <.link
                    navigate={~p"/users/#{user.id}"}
                    class="text-primary hover:underline font-medium"
                  >
                    {user.name}
                  </.link>
                </td>
                <td class="text-sm text-on-surface-variant font-mono">{user.id}</td>
                <td>
                  <span class={[
                    "badge",
                    user.status == :active && "badge-success",
                    user.status == :suspended && "badge-error"
                  ]}>
                    {user.status}
                  </span>
                </td>
                <td class="text-sm text-on-surface-variant">{user.key_count}</td>
                <td>
                  <%= if user.status == :active do %>
                    <button
                      type="button"
                      class="btn btn-ghost btn-xs text-warning"
                      phx-click="open_confirm_modal"
                      phx-value-action="suspend"
                      phx-value-id={user.id}
                    >
                      Suspend
                    </button>
                  <% else %>
                    <button
                      type="button"
                      class="btn btn-ghost btn-xs text-success"
                      phx-click="activate_user"
                      phx-value-id={user.id}
                    >
                      Activate
                    </button>
                  <% end %>
                  <button
                    type="button"
                    class="btn btn-ghost btn-xs text-error"
                    phx-click="open_confirm_modal"
                    phx-value-action="delete"
                    phx-value-id={user.id}
                  >
                    Delete
                  </button>
                </td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @users == [] do %>
          <div class="flex flex-col items-center gap-3 px-6 py-16 text-on-surface-variant">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              class="w-12 h-12 opacity-30"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="1.5"
            >
              <path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" />
              <circle cx="9" cy="7" r="4" />
              <line x1="19" y1="8" x2="19" y2="14" />
              <line x1="22" y1="11" x2="16" y2="11" />
            </svg>
            <p class="text-sm">No users yet.</p>
            <button
              id="empty-state-create-user-btn"
              type="button"
              class="btn btn-primary btn-sm"
              phx-click="open_create_modal"
            >
              Create your first user
            </button>
          </div>
        <% end %>
      </div>

      <.confirm_modal
        show={@show_confirm_modal}
        title={@confirm_title}
        message={@confirm_message}
        confirm_event={@confirm_event}
        confirm_params={@confirm_params}
        confirm_label={@confirm_label}
        confirm_style={@confirm_style}
      />
    </div>
    """
  end

  defp load_users(socket) do
    users =
      case User.list_users() do
        {:ok, users} ->
          Enum.map(users, fn user ->
            key_count =
              case AccessKey.list_user_keys(user.id) do
                {:ok, keys} -> length(keys)
                _ -> 0
              end

            Map.put(user, :key_count, key_count)
          end)

        _ ->
          []
      end

    assign(socket, :users, users)
  end
end
