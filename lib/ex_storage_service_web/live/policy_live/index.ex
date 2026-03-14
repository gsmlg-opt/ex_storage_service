defmodule ExStorageServiceWeb.PolicyLive.Index do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.IAM.Policy
  alias ExStorageService.IAM.Audit

  @templates %{
    "ReadOnly" => :read_only_statements,
    "ReadWrite" => :read_write_statements,
    "FullAccess" => :full_access_statements
  }

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:page_title, "Policies")
      |> assign(:policies, [])
      |> assign(:policy_name, "")
      |> assign(:template, "ReadOnly")
      |> assign(:bucket_name, "")

    {:ok, socket}
  end

  @impl true
  def handle_params(_params, _url, socket) do
    {:noreply, load_policies(socket)}
  end

  @impl true
  def handle_event("create_policy", %{"name" => name, "template" => template} = params, socket) do
    name = String.trim(name)

    if name == "" do
      {:noreply, put_flash(socket, :error, "Policy name cannot be empty")}
    else
      statements =
        case template do
          "BucketScoped" ->
            bucket = String.trim(params["bucket_name"] || "")

            if bucket == "" do
              nil
            else
              Policy.bucket_scoped_statements(bucket)
            end

          t when is_map_key(@templates, t) ->
            apply(Policy, @templates[t], [])

          _ ->
            nil
        end

      if is_nil(statements) do
        {:noreply, put_flash(socket, :error, "Invalid template or missing bucket name")}
      else
        case Policy.create_policy(name, statements) do
          {:ok, policy} ->
            Audit.log_event("root", :create_policy, policy.id, %{name: policy.name})

            socket =
              socket
              |> put_flash(:info, "Policy '#{policy.name}' created")
              |> assign(:policy_name, "")
              |> load_policies()

            {:noreply, socket}

          {:error, reason} ->
            {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
        end
      end
    end
  end

  def handle_event("delete_policy", %{"id" => policy_id}, socket) do
    case Policy.delete_policy(policy_id) do
      :ok ->
        Audit.log_event("root", :delete_policy, policy_id)
        {:noreply, socket |> put_flash(:info, "Policy deleted") |> load_policies()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <.header>
        Policies
        <:subtitle>Manage IAM policies</:subtitle>
      </.header>

      <div class="mt-6 card">
        <div class="card-body">
          <h3 class="card-title text-sm">Create Policy from Template</h3>
          <form phx-submit="create_policy" class="space-y-3">
            <div class="flex items-end gap-3">
              <div class="flex-1 form-group">
                <label for="name" class="form-label">Policy Name</label>
                <input
                  type="text"
                  name="name"
                  id="name"
                  value={@policy_name}
                  placeholder="e.g. my-readonly-policy"
                  class="input input-primary w-full"
                />
              </div>
              <div class="w-48 form-group">
                <label for="template" class="form-label">Template</label>
                <select name="template" id="template" class="select select-primary w-full">
                  <option value="ReadOnly">ReadOnly</option>
                  <option value="ReadWrite">ReadWrite</option>
                  <option value="FullAccess">FullAccess</option>
                  <option value="BucketScoped">BucketScoped</option>
                </select>
              </div>
              <div class="w-48 form-group">
                <label for="bucket_name" class="form-label">Bucket (for BucketScoped)</label>
                <input
                  type="text"
                  name="bucket_name"
                  id="bucket_name"
                  value={@bucket_name}
                  placeholder="bucket-name"
                  class="input input-primary w-full"
                />
              </div>
              <button type="submit" class="btn btn-primary">Create</button>
            </div>
          </form>
        </div>
      </div>

      <div class="mt-6 card">
        <table class="table table-hover w-full">
          <thead>
            <tr>
              <th class="text-on-surface-variant">Name</th>
              <th class="text-on-surface-variant">ID</th>
              <th class="text-on-surface-variant">Statements</th>
              <th class="text-on-surface-variant">Created</th>
              <th class="text-on-surface-variant">Actions</th>
            </tr>
          </thead>
          <tbody>
            <%= for policy <- @policies do %>
              <tr>
                <td>
                  <.link
                    navigate={~p"/policies/#{policy.id}"}
                    class="text-primary hover:underline font-medium"
                  >
                    {policy.name}
                  </.link>
                </td>
                <td class="text-sm text-on-surface-variant font-mono">{policy.id}</td>
                <td class="text-sm text-on-surface-variant">{length(policy.statements)}</td>
                <td class="text-sm text-on-surface-variant">{policy.created_at}</td>
                <td>
                  <button
                    phx-click="delete_policy"
                    phx-value-id={policy.id}
                    data-confirm="Delete this policy? This cannot be undone."
                    class="btn btn-ghost btn-xs text-error"
                  >
                    Delete
                  </button>
                </td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @policies == [] do %>
          <p class="px-6 py-8 text-center text-on-surface-variant">No policies yet.</p>
        <% end %>
      </div>
    </div>
    """
  end

  defp load_policies(socket) do
    policies =
      case Policy.list_policies() do
        {:ok, list} -> list
        _ -> []
      end

    assign(socket, :policies, policies)
  end
end
