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
      |> assign(:create_error, nil)
      |> assign(:show_create_modal, false)
      |> assign(
        show_confirm_modal: false,
        confirm_title: "",
        confirm_message: "",
        confirm_event: "",
        confirm_params: %{}
      )

    {:ok, socket}
  end

  @impl true
  def handle_params(_params, _url, socket) do
    {:noreply, load_policies(socket)}
  end

  @impl true
  def handle_event("open_create_modal", _params, socket) do
    {:noreply,
     assign(socket,
       show_create_modal: true,
       policy_name: "",
       template: "ReadOnly",
       bucket_name: "",
       create_error: nil
     )}
  end

  def handle_event("close_create_modal", _params, socket) do
    {:noreply, assign(socket, show_create_modal: false, create_error: nil)}
  end

  def handle_event("create_policy", %{"name" => name, "template" => template} = params, socket) do
    name = String.trim(name)

    if name == "" do
      {:noreply, assign(socket, :create_error, "Policy name cannot be empty")}
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
        {:noreply, assign(socket, :create_error, "Invalid template or missing bucket name")}
      else
        case Policy.create_policy(name, statements) do
          {:ok, policy} ->
            Audit.log_event("root", :create_policy, policy.id, %{name: policy.name})

            socket =
              socket
              |> put_flash(:info, "Policy '#{policy.name}' created")
              |> assign(:policy_name, "")
              |> assign(:create_error, nil)
              |> assign(:show_create_modal, false)
              |> load_policies()

            {:noreply, socket}

          {:error, reason} ->
            {:noreply, assign(socket, :create_error, "Failed: #{inspect(reason)}")}
        end
      end
    end
  end

  def handle_event("open_confirm_delete", %{"id" => policy_id}, socket) do
    {:noreply,
     assign(socket,
       show_confirm_modal: true,
       confirm_title: "Delete Policy",
       confirm_message: "Delete this policy? This cannot be undone.",
       confirm_event: "confirm_delete_policy",
       confirm_params: %{"id" => policy_id}
     )}
  end

  def handle_event("close_confirm_modal", _params, socket) do
    {:noreply, assign(socket, show_confirm_modal: false)}
  end

  def handle_event("confirm_delete_policy", %{"id" => policy_id}, socket) do
    case Policy.delete_policy(policy_id) do
      :ok ->
        Audit.log_event("root", :delete_policy, policy_id)

        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:info, "Policy deleted")
         |> load_policies()}

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:error, "Failed: #{inspect(reason)}")}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="max-w-4xl mx-auto">
      <.header>
        Policies
        <:subtitle>Manage IAM policies</:subtitle>
        <:actions>
          <button
            id="open-create-policy-btn"
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
            Create Policy
          </button>
        </:actions>
      </.header>

      <%!-- Modal overlay --%>
      <div
        :if={@show_create_modal}
        id="create-policy-overlay"
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
              <h2 id="create-policy-title" class="text-lg font-semibold">Create New Policy</h2>
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
              id="create-policy-form"
              phx-submit="create_policy"
              class="flex flex-col gap-4"
            >
              <div class="form-group">
                <label for="policy-name" class="form-label font-medium">Policy Name</label>
                <input
                  type="text"
                  name="name"
                  id="policy-name"
                  value={@policy_name}
                  placeholder="e.g. my-readonly-policy"
                  class={"input w-full #{if @create_error, do: "input-error", else: "input-primary"}"}
                  autocomplete="off"
                  autofocus
                />
              </div>

              <div class="form-group">
                <label for="policy-template" class="form-label font-medium">Template</label>
                <select name="template" id="policy-template" class="select select-primary w-full">
                  <option value="ReadOnly">ReadOnly</option>
                  <option value="ReadWrite">ReadWrite</option>
                  <option value="FullAccess">FullAccess</option>
                  <option value="BucketScoped">BucketScoped</option>
                </select>
              </div>

              <div class="form-group">
                <label for="policy-bucket-name" class="form-label font-medium">
                  Bucket Name
                  <span class="font-normal text-on-surface-variant">(for BucketScoped)</span>
                </label>
                <input
                  type="text"
                  name="bucket_name"
                  id="policy-bucket-name"
                  value={@bucket_name}
                  placeholder="bucket-name"
                  class="input input-primary w-full"
                  autocomplete="off"
                />
                <p class="mt-1 text-xs text-on-surface-variant">
                  Only required when using the BucketScoped template.
                </p>
              </div>

              <p :if={@create_error} class="text-xs text-error">
                {@create_error}
              </p>

              <div class="flex justify-end gap-2">
                <button type="button" class="btn btn-ghost btn-sm" phx-click="close_create_modal">
                  Cancel
                </button>
                <button
                  type="submit"
                  id="create-policy-submit"
                  class="btn btn-primary btn-sm"
                >
                  Create
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>

      <%!-- Policy table --%>
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
                  <span class="tooltip">
                    <button
                      type="button"
                      class="btn btn-ghost btn-error btn-circle btn-sm"
                      phx-click="open_confirm_delete"
                      phx-value-id={policy.id}
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
                </td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @policies == [] do %>
          <div class="flex flex-col items-center gap-3 px-6 py-16 text-on-surface-variant">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              class="w-12 h-12 opacity-30"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="1.5"
            >
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
              <polyline points="14 2 14 8 20 8" />
              <line x1="12" y1="18" x2="12" y2="12" />
              <line x1="9" y1="15" x2="15" y2="15" />
            </svg>
            <p class="text-sm">No policies yet.</p>
            <button
              id="empty-state-create-policy-btn"
              type="button"
              class="btn btn-primary btn-sm"
              phx-click="open_create_modal"
            >
              Create your first policy
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
        confirm_label="Delete"
      />
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
