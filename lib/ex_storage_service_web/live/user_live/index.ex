defmodule ExStorageServiceWeb.UserLive.Index do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.IAM.User
  alias ExStorageService.IAM.AccessKey

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
        {:noreply, socket |> put_flash(:info, "User suspended") |> load_users()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  def handle_event("activate_user", %{"id" => user_id}, socket) do
    case User.activate_user(user_id) do
      {:ok, _} ->
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

      <div class="mt-6 bg-white shadow rounded-lg p-6">
        <h3 class="text-sm font-semibold text-gray-700 mb-3">Create New User</h3>
        <form phx-submit="create_user" class="flex items-end gap-3">
          <div class="flex-1">
            <label for="name" class="block text-sm text-gray-600 mb-1">Name</label>
            <input
              type="text"
              name="name"
              id="name"
              value={@new_user_name}
              placeholder="Enter user name"
              class="w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 text-sm"
            />
          </div>
          <button
            type="submit"
            class="px-4 py-2 bg-indigo-600 text-white text-sm font-medium rounded-md hover:bg-indigo-700"
          >
            Create User
          </button>
        </form>
      </div>

      <div class="mt-6 bg-white shadow rounded-lg overflow-hidden">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Keys</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <%= for user <- @users do %>
              <tr>
                <td class="px-6 py-4">
                  <.link navigate={~p"/users/#{user.id}"} class="text-blue-600 hover:underline font-medium">
                    {user.name}
                  </.link>
                </td>
                <td class="px-6 py-4 text-sm text-gray-500 font-mono">{user.id}</td>
                <td class="px-6 py-4">
                  <span class={[
                    "inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium",
                    user.status == :active && "bg-green-100 text-green-800",
                    user.status == :suspended && "bg-red-100 text-red-800"
                  ]}>
                    {user.status}
                  </span>
                </td>
                <td class="px-6 py-4 text-sm text-gray-500">{user.key_count}</td>
                <td class="px-6 py-4">
                  <%= if user.status == :active do %>
                    <button
                      phx-click="suspend_user"
                      phx-value-id={user.id}
                      data-confirm="Suspend this user?"
                      class="text-sm text-red-600 hover:text-red-800 mr-3"
                    >
                      Suspend
                    </button>
                  <% else %>
                    <button
                      phx-click="activate_user"
                      phx-value-id={user.id}
                      class="text-sm text-green-600 hover:text-green-800 mr-3"
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
          <p class="px-6 py-8 text-center text-gray-400">No users yet.</p>
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
