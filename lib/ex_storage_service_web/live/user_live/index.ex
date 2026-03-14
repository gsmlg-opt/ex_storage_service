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

    {:ok, socket}
  end

  @impl true
  def handle_params(_params, _url, socket) do
    {:noreply, load_users(socket)}
  end

  @impl true
  def handle_event("create_user", %{"name" => name}, socket) do
    name = String.trim(name)

    if name == "" do
      {:noreply, put_flash(socket, :error, "User name cannot be empty")}
    else
      case User.create_user(name) do
        {:ok, user} ->
          Audit.log_event("root", :create_user, user.id, %{name: user.name})

          socket =
            socket
            |> put_flash(:info, "User #{user.name} created (ID: #{user.id})")
            |> assign(:new_user_name, "")
            |> load_users()

          {:noreply, socket}

        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Failed to create user: #{inspect(reason)}")}
      end
    end
  end

  def handle_event("suspend_user", %{"id" => user_id}, socket) do
    case User.suspend_user(user_id) do
      {:ok, _} ->
        Audit.log_event("root", :suspend_user, user_id)
        {:noreply, socket |> put_flash(:info, "User suspended") |> load_users()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
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
    <div>
      <.header>
        Users
        <:subtitle>Manage IAM users</:subtitle>
      </.header>

      <div class="mt-6 card">
        <div class="card-body">
          <h3 class="card-title text-sm">Create New User</h3>
          <form phx-submit="create_user" class="flex items-end gap-3">
            <div class="flex-1 form-group">
              <label for="name" class="form-label">Name</label>
              <input
                type="text"
                name="name"
                id="name"
                value={@new_user_name}
                placeholder="Enter user name"
                class="input input-primary w-full"
              />
            </div>
            <button type="submit" class="btn btn-primary">Create User</button>
          </form>
        </div>
      </div>

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
                      phx-click="suspend_user"
                      phx-value-id={user.id}
                      data-confirm="Suspend this user?"
                      class="btn btn-ghost btn-xs text-error"
                    >
                      Suspend
                    </button>
                  <% else %>
                    <button
                      phx-click="activate_user"
                      phx-value-id={user.id}
                      class="btn btn-ghost btn-xs text-success"
                    >
                      Activate
                    </button>
                  <% end %>
                </td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @users == [] do %>
          <p class="px-6 py-8 text-center text-on-surface-variant">No users yet.</p>
        <% end %>
      </div>
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
