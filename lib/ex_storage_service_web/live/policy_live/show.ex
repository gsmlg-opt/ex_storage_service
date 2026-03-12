defmodule ExStorageServiceWeb.PolicyLive.Show do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.IAM.Policy
  alias ExStorageService.IAM.User

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

        {:ok, socket}

      {:error, :not_found} ->
        {:ok,
         socket
         |> put_flash(:error, "Policy not found")
         |> redirect(to: ~p"/policies")}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <.link navigate={~p"/policies"} class="text-blue-600 hover:underline text-sm">&larr; Back to Policies</.link>

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
              <div class="flex items-center gap-2 mb-3">
                <span class="text-sm font-medium text-gray-500">Statement {idx + 1}</span>
                <span class={[
                  "inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium",
                  stmt.effect == :allow && "bg-green-100 text-green-800",
                  stmt.effect == :deny && "bg-red-100 text-red-800"
                ]}>
                  {stmt.effect}
                </span>
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
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
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
