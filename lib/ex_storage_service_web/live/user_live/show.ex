defmodule ExStorageServiceWeb.UserLive.Show do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.IAM.User
  alias ExStorageService.IAM.AccessKey
  alias ExStorageService.IAM.Policy
  alias ExStorageService.IAM.Audit

  @impl true
  def mount(%{"id" => user_id}, _session, socket) do
    case User.get_user(user_id) do
      {:ok, user} ->
        socket =
          socket
          |> assign(:page_title, "User: #{user.name}")
          |> assign(:user, user)
          |> assign(:access_keys, [])
          |> assign(:user_policies, [])
          |> assign(:all_policies, [])
          |> assign(:new_secret, nil)
          |> assign(:new_key_id, nil)
          |> load_keys()
          |> load_policies()

        {:ok, socket}

      {:error, :not_found} ->
        {:ok,
         socket
         |> put_flash(:error, "User not found")
         |> redirect(to: ~p"/users")}
    end
  end

  @impl true
  def handle_event("create_key", _params, socket) do
    user = socket.assigns.user

    case AccessKey.create_access_key(user.id) do
      {:ok, key} ->
        Audit.log_event("root", :create_key, user.id, %{access_key_id: key.access_key_id})

        socket =
          socket
          |> assign(:new_secret, key.secret_access_key)
          |> assign(:new_key_id, key.access_key_id)
          |> put_flash(
            :info,
            "Access key created. Copy the secret now - it will not be shown again!"
          )
          |> load_keys()

        {:noreply, socket}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed to create key: #{inspect(reason)}")}
    end
  end

  def handle_event("dismiss_secret", _params, socket) do
    {:noreply, assign(socket, :new_secret, nil) |> assign(:new_key_id, nil)}
  end

  def handle_event("activate_key", %{"key-id" => key_id}, socket) do
    case AccessKey.activate_key(key_id) do
      {:ok, _} ->
        Audit.log_event("root", :activate_key, key_id)
        {:noreply, socket |> put_flash(:info, "Key activated") |> load_keys()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  def handle_event("deactivate_key", %{"key-id" => key_id}, socket) do
    case AccessKey.deactivate_key(key_id) do
      {:ok, _} ->
        Audit.log_event("root", :deactivate_key, key_id)
        {:noreply, socket |> put_flash(:info, "Key deactivated") |> load_keys()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  def handle_event("delete_key", %{"key-id" => key_id}, socket) do
    case AccessKey.delete_key(key_id) do
      :ok ->
        Audit.log_event("root", :delete_key, key_id)
        {:noreply, socket |> put_flash(:info, "Key deleted") |> load_keys()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  def handle_event("attach_policy", %{"policy_id" => policy_id}, socket) do
    user = socket.assigns.user

    case Policy.attach_policy(user.id, policy_id) do
      :ok ->
        Audit.log_event("root", :attach_policy, user.id, %{policy_id: policy_id})
        {:noreply, socket |> put_flash(:info, "Policy attached") |> load_policies()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  def handle_event("detach_policy", %{"policy-id" => policy_id}, socket) do
    user = socket.assigns.user

    case Policy.detach_policy(user.id, policy_id) do
      :ok ->
        Audit.log_event("root", :detach_policy, user.id, %{policy_id: policy_id})
        {:noreply, socket |> put_flash(:info, "Policy detached") |> load_policies()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <.link navigate={~p"/users"} class="text-blue-600 hover:underline text-sm">
        &larr; Back to Users
      </.link>

      <.header class="mt-2">
        {@user.name}
        <:subtitle>
          ID: {@user.id} &middot;
          Status:
          <span class={[
            "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium",
            @user.status == :active && "bg-green-100 text-green-800",
            @user.status == :suspended && "bg-red-100 text-red-800"
          ]}>
            {@user.status}
          </span>
          &middot; Created: {@user.created_at}
        </:subtitle>
      </.header>

      <%!-- Secret reveal modal --%>
      <%= if @new_secret do %>
        <div class="mt-4 bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <h3 class="text-sm font-semibold text-yellow-800 mb-2">
            New Access Key Created - Save the Secret Now!
          </h3>
          <p class="text-sm text-yellow-700 mb-2">This secret will not be shown again.</p>
          <div class="bg-white border rounded p-3 mb-3">
            <p class="text-xs text-gray-500">Access Key ID</p>
            <p class="font-mono text-sm select-all">{@new_key_id}</p>
            <p class="text-xs text-gray-500 mt-2">Secret Access Key</p>
            <p class="font-mono text-sm select-all">{@new_secret}</p>
          </div>
          <button
            phx-click="dismiss_secret"
            class="px-3 py-1.5 bg-yellow-600 text-white text-sm rounded hover:bg-yellow-700"
          >
            I have saved the secret
          </button>
        </div>
      <% end %>

      <%!-- Access Keys Section --%>
      <div class="mt-8">
        <div class="flex items-center justify-between mb-4">
          <h2 class="text-lg font-semibold text-gray-900">Access Keys</h2>
          <button
            phx-click="create_key"
            class="px-3 py-1.5 bg-indigo-600 text-white text-sm font-medium rounded-md hover:bg-indigo-700"
          >
            Create Access Key
          </button>
        </div>
        <div class="bg-white shadow rounded-lg overflow-hidden">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Access Key ID
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Status
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Created
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-200">
              <%= for key <- @access_keys do %>
                <tr>
                  <td class="px-6 py-4 font-mono text-sm">{key.access_key_id}</td>
                  <td class="px-6 py-4">
                    <span class={[
                      "inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium",
                      key.status == :active && "bg-green-100 text-green-800",
                      key.status == :inactive && "bg-gray-100 text-gray-800"
                    ]}>
                      {key.status}
                    </span>
                  </td>
                  <td class="px-6 py-4 text-sm text-gray-500">{key.created_at}</td>
                  <td class="px-6 py-4 space-x-2">
                    <%= if key.status == :active do %>
                      <button
                        phx-click="deactivate_key"
                        phx-value-key-id={key.access_key_id}
                        class="text-sm text-yellow-600 hover:text-yellow-800"
                      >
                        Deactivate
                      </button>
                    <% else %>
                      <button
                        phx-click="activate_key"
                        phx-value-key-id={key.access_key_id}
                        class="text-sm text-green-600 hover:text-green-800"
                      >
                        Activate
                      </button>
                    <% end %>
                    <button
                      phx-click="delete_key"
                      phx-value-key-id={key.access_key_id}
                      data-confirm="Delete this access key? This cannot be undone."
                      class="text-sm text-red-600 hover:text-red-800"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              <% end %>
            </tbody>
          </table>
          <%= if @access_keys == [] do %>
            <p class="px-6 py-8 text-center text-gray-400">No access keys.</p>
          <% end %>
        </div>
      </div>

      <%!-- Policies Section --%>
      <div class="mt-8">
        <div class="flex items-center justify-between mb-4">
          <h2 class="text-lg font-semibold text-gray-900">Attached Policies</h2>
        </div>

        <div class="bg-white shadow rounded-lg overflow-hidden">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Policy Name
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-200">
              <%= for policy <- @user_policies do %>
                <tr>
                  <td class="px-6 py-4">
                    <.link navigate={~p"/policies/#{policy.id}"} class="text-blue-600 hover:underline">
                      {policy.name}
                    </.link>
                  </td>
                  <td class="px-6 py-4 text-sm text-gray-500 font-mono">{policy.id}</td>
                  <td class="px-6 py-4">
                    <button
                      phx-click="detach_policy"
                      phx-value-policy-id={policy.id}
                      data-confirm="Detach this policy?"
                      class="text-sm text-red-600 hover:text-red-800"
                    >
                      Detach
                    </button>
                  </td>
                </tr>
              <% end %>
            </tbody>
          </table>
          <%= if @user_policies == [] do %>
            <p class="px-6 py-6 text-center text-gray-400">No policies attached.</p>
          <% end %>
        </div>

        <%!-- Attach policy form --%>
        <% available =
          Enum.reject(@all_policies, fn p -> p.id in Enum.map(@user_policies, & &1.id) end) %>
        <%= if available != [] do %>
          <div class="mt-4 bg-white shadow rounded-lg p-4">
            <h3 class="text-sm font-semibold text-gray-700 mb-3">Attach Policy</h3>
            <form phx-submit="attach_policy" class="flex items-end gap-3">
              <div class="flex-1">
                <select
                  name="policy_id"
                  class="w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 text-sm"
                >
                  <%= for policy <- available do %>
                    <option value={policy.id}>{policy.name}</option>
                  <% end %>
                </select>
              </div>
              <button
                type="submit"
                class="px-4 py-2 bg-indigo-600 text-white text-sm font-medium rounded-md hover:bg-indigo-700"
              >
                Attach
              </button>
            </form>
          </div>
        <% end %>
      </div>
    </div>
    """
  end

  defp load_keys(socket) do
    user = socket.assigns.user

    keys =
      case AccessKey.list_user_keys(user.id) do
        {:ok, keys} -> keys
        _ -> []
      end

    assign(socket, :access_keys, keys)
  end

  defp load_policies(socket) do
    user = socket.assigns.user

    user_policies =
      case Policy.get_user_policies(user.id) do
        {:ok, policies} -> policies
        _ -> []
      end

    all_policies =
      case Policy.list_policies() do
        {:ok, policies} -> policies
        _ -> []
      end

    socket
    |> assign(:user_policies, user_policies)
    |> assign(:all_policies, all_policies)
  end
end
