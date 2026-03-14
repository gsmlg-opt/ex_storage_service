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
      <.dm_link navigate={~p"/policies"} class="text-primary text-sm">
        &larr; Back to Policies
      </.dm_link>

      <.header class="mt-2">
        {@policy.name}
        <:subtitle>ID: {@policy.id} &middot; Created: {@policy.created_at}</:subtitle>
      </.header>

      <div class="mt-8">
        <h2 class="text-lg font-semibold text-on-surface mb-4">Statements</h2>
        <div class="space-y-4">
          <%= for {stmt, idx} <- Enum.with_index(@policy.statements) do %>
            <div class="card">
              <div class="card-body">
                <div class="flex items-center justify-between mb-3">
                  <div class="flex items-center gap-2">
                    <span class="text-sm font-medium text-on-surface-variant">
                      Statement {idx + 1}
                    </span>
                    <span class={[
                      "badge",
                      stmt.effect == :allow && "badge-success",
                      stmt.effect == :deny && "badge-error"
                    ]}>
                      {stmt.effect}
                    </span>
                  </div>
                  <button
                    phx-click="remove_statement"
                    phx-value-index={idx}
                    data-confirm="Remove this statement?"
                    class="btn btn-ghost btn-xs text-error"
                  >
                    Remove
                  </button>
                </div>
                <div class="grid grid-cols-2 gap-4">
                  <div>
                    <p class="text-xs font-medium text-on-surface-variant uppercase mb-1">Actions</p>
                    <ul class="space-y-1">
                      <%= for action <- stmt.actions do %>
                        <li class="text-sm font-mono bg-surface-container px-2 py-1 rounded">
                          {action}
                        </li>
                      <% end %>
                    </ul>
                  </div>
                  <div>
                    <p class="text-xs font-medium text-on-surface-variant uppercase mb-1">
                      Resources
                    </p>
                    <ul class="space-y-1">
                      <%= for resource <- stmt.resources do %>
                        <li class="text-sm font-mono bg-surface-container px-2 py-1 rounded">
                          {resource}
                        </li>
                      <% end %>
                    </ul>
                  </div>
                </div>
              </div>
            </div>
          <% end %>
          <%= if @policy.statements == [] do %>
            <p class="text-on-surface-variant">No statements in this policy.</p>
          <% end %>

          <div class="card border-2 border-dashed border-outline-variant mt-4">
            <div class="card-body">
              <h3 class="card-title text-sm">Add Statement</h3>
              <form phx-submit="add_statement" class="space-y-3">
                <div class="form-group">
                  <label class="form-label">Effect</label>
                  <div class="flex gap-3">
                    <label class="inline-flex items-center gap-1 text-sm">
                      <input
                        type="radio"
                        name="effect"
                        value="allow"
                        checked={@new_effect == "allow"}
                        phx-click="set_effect"
                        phx-value-effect="allow"
                        class="radio radio-success"
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
                        class="radio radio-error"
                      /> Deny
                    </label>
                  </div>
                </div>
                <div class="form-group">
                  <label class="form-label">Actions</label>
                  <div class="grid grid-cols-2 gap-1 sm:grid-cols-3">
                    <%= for action <- @s3_actions do %>
                      <label class="inline-flex items-center gap-1 text-xs">
                        <input
                          type="checkbox"
                          checked={action in @new_actions}
                          phx-click="toggle_action"
                          phx-value-action={action}
                          class="checkbox checkbox-primary"
                        />
                        <span class="font-mono">{action}</span>
                      </label>
                    <% end %>
                  </div>
                </div>
                <div class="form-group">
                  <label class="form-label">Resources (comma-separated ARNs)</label>
                  <input
                    type="text"
                    name="resources"
                    value={@new_resources}
                    placeholder="arn:ess:::my-bucket/*, arn:ess:::my-bucket"
                    class="input input-primary w-full font-mono text-xs"
                  />
                  <p class="text-xs text-on-surface-variant mt-1">
                    Format: arn:ess:::BUCKET, arn:ess:::BUCKET/KEY, arn:ess:::BUCKET/*, arn:ess:::*
                  </p>
                </div>
                <button type="submit" class="btn btn-primary btn-sm">Add Statement</button>
              </form>
            </div>
          </div>
        </div>
      </div>

      <div class="mt-8">
        <h2 class="text-lg font-semibold text-on-surface mb-4">Attached Users</h2>
        <div class="card">
          <table class="table table-hover w-full">
            <thead>
              <tr>
                <th class="text-on-surface-variant">Name</th>
                <th class="text-on-surface-variant">ID</th>
                <th class="text-on-surface-variant">Status</th>
              </tr>
            </thead>
            <tbody>
              <%= for user <- @attached_users do %>
                <tr>
                  <td>
                    <.link navigate={~p"/users/#{user.id}"} class="text-primary hover:underline">
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
                </tr>
              <% end %>
            </tbody>
          </table>
          <%= if @attached_users == [] do %>
            <p class="px-6 py-6 text-center text-on-surface-variant">
              No users attached to this policy.
            </p>
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
