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
          |> assign(
            show_confirm_modal: false,
            confirm_title: "",
            confirm_message: "",
            confirm_event: "",
            confirm_params: %{}
          )
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

  def handle_event("open_confirm_modal", params, socket) do
    {title, message, event, confirm_params} =
      case params["action"] do
        "delete_key" ->
          {"Delete Access Key", "Delete this access key? This cannot be undone.",
           "confirm_delete_key", %{"key-id" => params["key-id"]}}

        "detach_policy" ->
          {"Detach Policy", "Detach this policy from the user?", "confirm_detach_policy",
           %{"policy-id" => params["policy-id"]}}

        _ ->
          {"Confirm", "Are you sure?", "", %{}}
      end

    {:noreply,
     assign(socket,
       show_confirm_modal: true,
       confirm_title: title,
       confirm_message: message,
       confirm_event: event,
       confirm_params: confirm_params
     )}
  end

  def handle_event("close_confirm_modal", _params, socket) do
    {:noreply, assign(socket, show_confirm_modal: false)}
  end

  def handle_event("confirm_delete_key", %{"key-id" => key_id}, socket) do
    case AccessKey.delete_key(key_id) do
      :ok ->
        Audit.log_event("root", :delete_key, key_id)

        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:info, "Key deleted")
         |> load_keys()}

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:error, "Failed: #{inspect(reason)}")}
    end
  end

  def handle_event("confirm_detach_policy", %{"policy-id" => policy_id}, socket) do
    user = socket.assigns.user

    case Policy.detach_policy(user.id, policy_id) do
      :ok ->
        Audit.log_event("root", :detach_policy, user.id, %{policy_id: policy_id})

        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:info, "Policy detached")
         |> load_policies()}

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:error, "Failed: #{inspect(reason)}")}
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

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <.dm_link navigate={~p"/users"} class="text-primary text-sm">
        &larr; Back to Users
      </.dm_link>

      <.header class="mt-2">
        {@user.name}
        <:subtitle>
          ID: {@user.id} &middot;
          Status:
          <span class={[
            "badge",
            @user.status == :active && "badge-success",
            @user.status == :suspended && "badge-error"
          ]}>
            {@user.status}
          </span>
          &middot; Created: {@user.created_at}
        </:subtitle>
      </.header>

      <%= if @new_secret do %>
        <div class="mt-4 alert alert-warning">
          <div>
            <h3 class="text-sm font-semibold mb-2">New Access Key Created - Save the Secret Now!</h3>
            <p class="text-sm mb-2">This secret will not be shown again.</p>
            <div class="bg-surface-container rounded-lg p-3 mb-3">
              <p class="text-xs text-on-surface-variant">Access Key ID</p>
              <p class="font-mono text-sm select-all">{@new_key_id}</p>
              <p class="text-xs text-on-surface-variant mt-2">Secret Access Key</p>
              <p class="font-mono text-sm select-all">{@new_secret}</p>
            </div>
            <button phx-click="dismiss_secret" class="btn btn-warning btn-sm">
              I have saved the secret
            </button>
          </div>
        </div>
      <% end %>

      <div class="mt-8">
        <div class="flex items-center justify-between mb-4">
          <h2 class="text-lg font-semibold text-on-surface">Access Keys</h2>
          <button phx-click="create_key" class="btn btn-primary btn-sm">Create Access Key</button>
        </div>
        <div class="card">
          <table class="table table-hover w-full">
            <thead>
              <tr>
                <th class="text-on-surface-variant">Access Key ID</th>
                <th class="text-on-surface-variant">Status</th>
                <th class="text-on-surface-variant">Created</th>
                <th class="text-on-surface-variant">Actions</th>
              </tr>
            </thead>
            <tbody>
              <%= for key <- @access_keys do %>
                <tr>
                  <td class="font-mono text-sm">{key.access_key_id}</td>
                  <td>
                    <span class={[
                      "badge",
                      key.status == :active && "badge-success",
                      key.status == :inactive && "badge-info"
                    ]}>
                      {key.status}
                    </span>
                  </td>
                  <td class="text-sm text-on-surface-variant">{key.created_at}</td>
                  <td>
                    <div class="flex items-center gap-1">
                      <%= if key.status == :active do %>
                        <span class="tooltip">
                          <button
                            phx-click="deactivate_key"
                            phx-value-key-id={key.access_key_id}
                            class="btn btn-ghost btn-circle btn-sm text-warning"
                            aria-label="Deactivate"
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
                              <circle cx="12" cy="12" r="10" />
                              <line x1="4.93" y1="4.93" x2="19.07" y2="19.07" />
                            </svg>
                          </button>
                          <span class="tooltip-content">Deactivate</span>
                        </span>
                      <% else %>
                        <span class="tooltip">
                          <button
                            phx-click="activate_key"
                            phx-value-key-id={key.access_key_id}
                            class="btn btn-ghost btn-circle btn-sm text-success"
                            aria-label="Activate"
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
                              <polygon points="5 3 19 12 5 21 5 3" />
                            </svg>
                          </button>
                          <span class="tooltip-content">Activate</span>
                        </span>
                      <% end %>
                      <span class="tooltip">
                        <button
                          type="button"
                          class="btn btn-ghost btn-error btn-circle btn-sm"
                          phx-click="open_confirm_modal"
                          phx-value-action="delete_key"
                          phx-value-key-id={key.access_key_id}
                          aria-label="Delete"
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
                            <polyline points="3 6 5 6 21 6" />
                            <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                          </svg>
                        </button>
                        <span class="tooltip-content">Delete</span>
                      </span>
                    </div>
                  </td>
                </tr>
              <% end %>
            </tbody>
          </table>
          <%= if @access_keys == [] do %>
            <p class="px-6 py-8 text-center text-on-surface-variant">No access keys.</p>
          <% end %>
        </div>
      </div>

      <div class="mt-8">
        <div class="flex items-center justify-between mb-4">
          <h2 class="text-lg font-semibold text-on-surface">Attached Policies</h2>
        </div>
        <div class="card">
          <table class="table table-hover w-full">
            <thead>
              <tr>
                <th class="text-on-surface-variant">Policy Name</th>
                <th class="text-on-surface-variant">ID</th>
                <th class="text-on-surface-variant">Actions</th>
              </tr>
            </thead>
            <tbody>
              <%= for policy <- @user_policies do %>
                <tr>
                  <td>
                    <.link navigate={~p"/policies/#{policy.id}"} class="text-primary hover:underline">
                      {policy.name}
                    </.link>
                  </td>
                  <td class="text-sm text-on-surface-variant font-mono">{policy.id}</td>
                  <td>
                    <span class="tooltip">
                      <button
                        type="button"
                        class="btn btn-ghost btn-error btn-circle btn-sm"
                        phx-click="open_confirm_modal"
                        phx-value-action="detach_policy"
                        phx-value-policy-id={policy.id}
                        aria-label="Detach"
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
                          <circle cx="12" cy="12" r="10" />
                          <line x1="15" y1="9" x2="9" y2="15" />
                          <line x1="9" y1="9" x2="15" y2="15" />
                        </svg>
                      </button>
                      <span class="tooltip-content">Detach</span>
                    </span>
                  </td>
                </tr>
              <% end %>
            </tbody>
          </table>
          <%= if @user_policies == [] do %>
            <p class="px-6 py-6 text-center text-on-surface-variant">No policies attached.</p>
          <% end %>
        </div>

        <% available =
          Enum.reject(@all_policies, fn p -> p.id in Enum.map(@user_policies, & &1.id) end) %>
        <%= if available != [] do %>
          <div class="mt-4 card">
            <div class="card-body">
              <h3 class="card-title text-sm">Attach Policy</h3>
              <form phx-submit="attach_policy" class="flex items-end gap-3">
                <div class="flex-1">
                  <select name="policy_id" class="select select-primary w-full">
                    <%= for policy <- available do %>
                      <option value={policy.id}>{policy.name}</option>
                    <% end %>
                  </select>
                </div>
                <button type="submit" class="btn btn-primary">Attach</button>
              </form>
            </div>
          </div>
        <% end %>
      </div>

      <.confirm_modal
        show={@show_confirm_modal}
        title={@confirm_title}
        message={@confirm_message}
        confirm_event={@confirm_event}
        confirm_params={@confirm_params}
        confirm_label="Confirm"
      />
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
