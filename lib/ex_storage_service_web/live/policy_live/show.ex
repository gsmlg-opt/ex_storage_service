defmodule ExStorageServiceWeb.PolicyLive.Show do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.IAM.Policy
  alias ExStorageService.IAM.User
  alias ExStorageService.IAM.Audit

  @s3_actions [
    "s3:*",
    "s3:ListAllMyBuckets",
    "s3:CreateBucket",
    "s3:DeleteBucket",
    "s3:HeadBucket",
    "s3:ListBucket",
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListMultipartUploadParts",
    "s3:AbortMultipartUpload"
  ]

  @impl true
  def mount(%{"id" => policy_id}, _session, socket) do
    case Policy.get_policy(policy_id) do
      {:ok, policy} ->
        attached_users = find_attached_users(policy_id)

        socket =
          socket
          |> assign(:page_title, "Policy: #{policy.name}")
          |> assign(:policy, policy)
          |> assign(:attached_users, attached_users)
          |> assign(:s3_actions, @s3_actions)
          |> assign(:new_effect, "allow")
          |> assign(:new_actions, [])
          |> assign(:new_resources, "")

        {:ok, socket}

      {:error, :not_found} ->
        {:ok,
         socket
         |> put_flash(:error, "Policy not found")
         |> redirect(to: ~p"/policies")}
    end
  end

  @impl true
  def handle_event("toggle_action", %{"action" => action}, socket) do
    current = socket.assigns.new_actions

    new_actions =
      if action in current,
        do: List.delete(current, action),
        else: current ++ [action]

    {:noreply, assign(socket, :new_actions, new_actions)}
  end

  def handle_event("set_effect", %{"effect" => effect}, socket) do
    {:noreply, assign(socket, :new_effect, effect)}
  end

  def handle_event("add_statement", params, socket) do
    effect = String.to_existing_atom(socket.assigns.new_effect)
    actions = socket.assigns.new_actions

    resources =
      (params["resources"] || "")
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))

    if actions == [] or resources == [] do
      {:noreply, put_flash(socket, :error, "Actions and resources are required")}
    else
      policy = socket.assigns.policy
      new_stmt = %{effect: effect, actions: actions, resources: resources}
      updated_statements = policy.statements ++ [new_stmt]

      case Policy.update_policy(policy.id, %{statements: updated_statements}) do
        {:ok, updated} ->
          Audit.log_event(:update_policy, :root, policy.id, %{added_statement: new_stmt})

          {:noreply,
           socket
           |> assign(:policy, updated)
           |> assign(:new_actions, [])
           |> assign(:new_resources, "")
           |> assign(:new_effect, "allow")
           |> put_flash(:info, "Statement added")}

        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
      end
    end
  end

  def handle_event("remove_statement", %{"index" => index_str}, socket) do
    index = String.to_integer(index_str)
    policy = socket.assigns.policy
    updated_statements = List.delete_at(policy.statements, index)

    case Policy.update_policy(policy.id, %{statements: updated_statements}) do
      {:ok, updated} ->
        Audit.log_event(:update_policy, :root, policy.id, %{removed_statement_index: index})

        {:noreply,
         socket
         |> assign(:policy, updated)
         |> put_flash(:info, "Statement removed")}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <.link navigate={~p"/policies"} class="text-blue-600 hover:underline text-sm">
        &larr; Back to Policies
      </.link>

      <.header class="mt-2">
        {@policy.name}
        <:subtitle>ID: {@policy.id} &middot; Created: {@policy.created_at}</:subtitle>
      </.header>

      <%!-- Statements --%>
      <div class="mt-8">
        <h2 class="text-lg font-semibold text-gray-900 mb-4">Statements</h2>
        <div class="space-y-4">
          <%= for {stmt, idx} <- Enum.with_index(@policy.statements) do %>
            <div class="bg-white shadow rounded-lg p-4">
              <div class="flex items-center justify-between mb-3">
                <div class="flex items-center gap-2">
                  <span class="text-sm font-medium text-gray-500">Statement {idx + 1}</span>
                  <span class={[
                    "inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium",
                    stmt.effect == :allow && "bg-green-100 text-green-800",
                    stmt.effect == :deny && "bg-red-100 text-red-800"
                  ]}>
                    {stmt.effect}
                  </span>
                </div>
                <button
                  phx-click="remove_statement"
                  phx-value-index={idx}
                  data-confirm="Remove this statement?"
                  class="text-xs text-red-600 hover:text-red-800"
                >
                  Remove
                </button>
              </div>
              <div class="grid grid-cols-2 gap-4">
                <div>
                  <p class="text-xs font-medium text-gray-500 uppercase mb-1">Actions</p>
                  <ul class="space-y-1">
                    <%= for action <- stmt.actions do %>
                      <li class="text-sm font-mono bg-gray-50 px-2 py-1 rounded">{action}</li>
                    <% end %>
                  </ul>
                </div>
                <div>
                  <p class="text-xs font-medium text-gray-500 uppercase mb-1">Resources</p>
                  <ul class="space-y-1">
                    <%= for resource <- stmt.resources do %>
                      <li class="text-sm font-mono bg-gray-50 px-2 py-1 rounded">{resource}</li>
                    <% end %>
                  </ul>
                </div>
              </div>
            </div>
          <% end %>
          <%= if @policy.statements == [] do %>
            <p class="text-gray-400">No statements in this policy.</p>
          <% end %>

          <%!-- Statement Builder --%>
          <div class="bg-gray-50 border-2 border-dashed border-gray-300 rounded-lg p-4 mt-4">
            <h3 class="text-sm font-semibold text-gray-700 mb-3">Add Statement</h3>
            <form phx-submit="add_statement" class="space-y-3">
              <div>
                <label class="block text-xs font-medium text-gray-500 mb-1">Effect</label>
                <div class="flex gap-3">
                  <label class="inline-flex items-center gap-1 text-sm">
                    <input
                      type="radio"
                      name="effect"
                      value="allow"
                      checked={@new_effect == "allow"}
                      phx-click="set_effect"
                      phx-value-effect="allow"
                      class="text-green-600"
                    /> Allow
                  </label>
                  <label class="inline-flex items-center gap-1 text-sm">
                    <input
                      type="radio"
                      name="effect"
                      value="deny"
                      checked={@new_effect == "deny"}
                      phx-click="set_effect"
                      phx-value-effect="deny"
                      class="text-red-600"
                    /> Deny
                  </label>
                </div>
              </div>
              <div>
                <label class="block text-xs font-medium text-gray-500 mb-1">Actions</label>
                <div class="grid grid-cols-2 gap-1 sm:grid-cols-3">
                  <%= for action <- @s3_actions do %>
                    <label class="inline-flex items-center gap-1 text-xs">
                      <input
                        type="checkbox"
                        checked={action in @new_actions}
                        phx-click="toggle_action"
                        phx-value-action={action}
                        class="rounded text-indigo-600"
                      />
                      <span class="font-mono">{action}</span>
                    </label>
                  <% end %>
                </div>
              </div>
              <div>
                <label class="block text-xs font-medium text-gray-500 mb-1">
                  Resources (comma-separated ARNs)
                </label>
                <input
                  type="text"
                  name="resources"
                  value={@new_resources}
                  placeholder="arn:ess:::my-bucket/*, arn:ess:::my-bucket"
                  class="w-full text-xs rounded border-gray-300 font-mono"
                />
                <p class="text-xs text-gray-400 mt-1">
                  Format: arn:ess:::BUCKET, arn:ess:::BUCKET/KEY, arn:ess:::BUCKET/*, arn:ess:::*
                </p>
              </div>
              <button
                type="submit"
                class="px-4 py-1.5 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700"
              >
                Add Statement
              </button>
            </form>
          </div>
        </div>
      </div>

      <%!-- Attached Users --%>
      <div class="mt-8">
        <h2 class="text-lg font-semibold text-gray-900 mb-4">Attached Users</h2>
        <div class="bg-white shadow rounded-lg overflow-hidden">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">ID</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Status
                </th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-200">
              <%= for user <- @attached_users do %>
                <tr>
                  <td class="px-6 py-4">
                    <.link navigate={~p"/users/#{user.id}"} class="text-blue-600 hover:underline">
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
                </tr>
              <% end %>
            </tbody>
          </table>
          <%= if @attached_users == [] do %>
            <p class="px-6 py-6 text-center text-gray-400">No users attached to this policy.</p>
          <% end %>
        </div>
      </div>
    </div>
    """
  end

  defp find_attached_users(policy_id) do
    users =
      case User.list_users() do
        {:ok, users} -> users
        _ -> []
      end

    Enum.filter(users, fn user ->
      case Policy.get_user_policies(user.id) do
        {:ok, policies} ->
          Enum.any?(policies, fn p -> p.id == policy_id end)

        _ ->
          false
      end
    end)
  end
end
