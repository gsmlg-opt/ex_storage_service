defmodule ExStorageServiceWeb.BucketLive.Show do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Versioning
  alias ExStorageService.Storage.Lifecycle
  alias ExStorageService.Notifications
  alias ExStorageService.Replication.Config, as: ReplicationConfig

  @impl true
  def mount(%{"name" => name}, _session, socket) do
    case Metadata.get_bucket(name) do
      {:ok, bucket} ->
        {:ok,
         socket
         |> assign(bucket: bucket, bucket_name: name, objects: [])
         |> assign(versioning: :disabled, lifecycle_rules: [], notifications: [], replicas: [])
         |> load_config()}

      {:error, :not_found} ->
        {:ok, socket |> put_flash(:error, "Bucket not found") |> redirect(to: ~p"/buckets")}
    end
  end

  @impl true
  def handle_params(_params, _url, socket) do
    case Metadata.list_objects(socket.assigns.bucket_name) do
      {:ok, result} ->
        objects =
          Enum.map(result.keys, fn {key, meta} ->
            Map.put(meta, :key, key)
          end)

        {:noreply, assign(socket, :objects, objects)}

      _ ->
        {:noreply, socket}
    end
  end

  @impl true
  def handle_event("set_versioning", %{"state" => state}, socket) do
    bucket = socket.assigns.bucket_name
    atom_state = String.to_existing_atom(state)

    case Versioning.set_versioning(bucket, atom_state) do
      :ok ->
        {:noreply, socket |> put_flash(:info, "Versioning set to #{state}") |> load_config()}

      {:ok, _} ->
        {:noreply, socket |> put_flash(:info, "Versioning set to #{state}") |> load_config()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  def handle_event("add_lifecycle_rule", params, socket) do
    bucket = socket.assigns.bucket_name
    prefix = String.trim(params["prefix"] || "")
    days = String.to_integer(params["expiration_days"] || "0")

    rule = %{prefix: prefix, status: "Enabled", expiration_days: days}
    existing = socket.assigns.lifecycle_rules
    new_rules = existing ++ [rule]

    case Lifecycle.put_rules(bucket, new_rules) do
      :ok ->
        {:noreply, socket |> put_flash(:info, "Lifecycle rule added") |> load_config()}

      {:ok, _} ->
        {:noreply, socket |> put_flash(:info, "Lifecycle rule added") |> load_config()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
    end
  end

  def handle_event("delete_lifecycle_rules", _params, socket) do
    Lifecycle.delete_rules(socket.assigns.bucket_name)
    {:noreply, socket |> put_flash(:info, "Lifecycle rules removed") |> load_config()}
  end

  def handle_event("add_notification", params, socket) do
    bucket = socket.assigns.bucket_name
    endpoint = String.trim(params["endpoint"] || "")
    events = String.split(params["events"] || "", ",") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))

    if endpoint == "" or events == [] do
      {:noreply, put_flash(socket, :error, "Endpoint and events are required")}
    else
      config = %{events: events, endpoint: endpoint, enabled: true}
      existing = socket.assigns.notifications
      new_configs = existing ++ [config]

      case Notifications.put_config(bucket, new_configs) do
        :ok ->
          {:noreply, socket |> put_flash(:info, "Notification added") |> load_config()}

        {:ok, _} ->
          {:noreply, socket |> put_flash(:info, "Notification added") |> load_config()}

        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
      end
    end
  end

  def handle_event("delete_notifications", _params, socket) do
    Notifications.delete_config(socket.assigns.bucket_name)
    {:noreply, socket |> put_flash(:info, "Notifications removed") |> load_config()}
  end

  def handle_event("add_replica", params, socket) do
    bucket = socket.assigns.bucket_name
    endpoint = String.trim(params["endpoint"] || "")
    access_key = String.trim(params["access_key"] || "")
    remote_bucket = String.trim(params["remote_bucket"] || bucket)

    if endpoint == "" do
      {:noreply, put_flash(socket, :error, "Replica endpoint is required")}
    else
      replica = %{endpoint: endpoint, access_key: access_key, secret_key_enc: "", bucket: remote_bucket}
      existing = socket.assigns.replicas
      new_replicas = existing ++ [replica]

      case ReplicationConfig.set_bucket_replicas(bucket, new_replicas) do
        :ok ->
          {:noreply, socket |> put_flash(:info, "Replica added") |> load_config()}

        {:error, reason} ->
          {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
      end
    end
  end

  def handle_event("remove_replicas", _params, socket) do
    ReplicationConfig.remove_bucket_replicas(socket.assigns.bucket_name)
    {:noreply, socket |> put_flash(:info, "Replicas removed") |> load_config()}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="max-w-4xl mx-auto p-6">
      <.link navigate={~p"/buckets"} class="text-blue-600 hover:underline text-sm">&larr; Back to Buckets</.link>
      <h1 class="text-2xl font-bold mb-6 mt-2">{@bucket_name}</h1>

      <%!-- Bucket Settings --%>
      <div class="grid grid-cols-1 gap-6 mb-8 lg:grid-cols-2">
        <%!-- Versioning --%>
        <div class="bg-white shadow rounded-lg p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Versioning</h3>
          <p class="text-sm text-gray-500 mb-3">
            Current: <span class="font-medium text-gray-900">{@versioning}</span>
          </p>
          <div class="flex gap-2">
            <button phx-click="set_versioning" phx-value-state="enabled"
              class="px-3 py-1 text-xs bg-green-100 text-green-800 rounded hover:bg-green-200">
              Enable
            </button>
            <button phx-click="set_versioning" phx-value-state="suspended"
              class="px-3 py-1 text-xs bg-yellow-100 text-yellow-800 rounded hover:bg-yellow-200">
              Suspend
            </button>
          </div>
        </div>

        <%!-- Replication --%>
        <div class="bg-white shadow rounded-lg p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Replication ({length(@replicas)} replicas)</h3>
          <%= for replica <- @replicas do %>
            <div class="text-xs text-gray-600 mb-1 font-mono truncate">{replica.endpoint} -> {replica.bucket || @bucket_name}</div>
          <% end %>
          <form phx-submit="add_replica" class="mt-3 space-y-2">
            <input type="text" name="endpoint" placeholder="https://peer:9000" class="w-full text-xs rounded border-gray-300" />
            <div class="flex gap-2">
              <input type="text" name="access_key" placeholder="Access Key" class="flex-1 text-xs rounded border-gray-300" />
              <input type="text" name="remote_bucket" placeholder="Remote bucket" class="flex-1 text-xs rounded border-gray-300" />
            </div>
            <div class="flex gap-2">
              <button type="submit" class="px-3 py-1 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700">Add Replica</button>
              <%= if @replicas != [] do %>
                <button type="button" phx-click="remove_replicas" data-confirm="Remove all replicas?"
                  class="px-3 py-1 text-xs text-red-600 border border-red-300 rounded hover:bg-red-50">Remove All</button>
              <% end %>
            </div>
          </form>
        </div>

        <%!-- Lifecycle --%>
        <div class="bg-white shadow rounded-lg p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Lifecycle Rules ({length(@lifecycle_rules)})</h3>
          <%= for rule <- @lifecycle_rules do %>
            <div class="text-xs text-gray-600 mb-1">
              Prefix: "<span class="font-mono">{rule[:prefix] || rule.prefix}</span>"
              Expire after <span class="font-medium">{rule[:expiration_days] || rule.expiration_days}</span> days
              (<span class={if (rule[:status] || rule.status) == "Enabled", do: "text-green-600", else: "text-gray-400"}>{rule[:status] || rule.status}</span>)
            </div>
          <% end %>
          <form phx-submit="add_lifecycle_rule" class="mt-3 flex gap-2 items-end">
            <input type="text" name="prefix" placeholder="Prefix (e.g. logs/)" class="flex-1 text-xs rounded border-gray-300" />
            <input type="number" name="expiration_days" placeholder="Days" min="1" class="w-20 text-xs rounded border-gray-300" />
            <button type="submit" class="px-3 py-1 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700">Add</button>
          </form>
          <%= if @lifecycle_rules != [] do %>
            <button phx-click="delete_lifecycle_rules" data-confirm="Remove all rules?"
              class="mt-2 text-xs text-red-600 hover:text-red-800">Remove All Rules</button>
          <% end %>
        </div>

        <%!-- Notifications --%>
        <div class="bg-white shadow rounded-lg p-5">
          <h3 class="text-sm font-semibold text-gray-700 mb-3">Notifications ({length(@notifications)})</h3>
          <%= for notif <- @notifications do %>
            <div class="text-xs text-gray-600 mb-1">
              <span class="font-mono truncate">{notif[:endpoint] || notif.endpoint}</span>
              <span class="text-gray-400 ml-1">{Enum.join(notif[:events] || notif.events, ", ")}</span>
            </div>
          <% end %>
          <form phx-submit="add_notification" class="mt-3 space-y-2">
            <input type="text" name="endpoint" placeholder="https://example.com/webhook" class="w-full text-xs rounded border-gray-300" />
            <input type="text" name="events" placeholder="s3:ObjectCreated:*,s3:ObjectRemoved:*" class="w-full text-xs rounded border-gray-300" />
            <div class="flex gap-2">
              <button type="submit" class="px-3 py-1 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700">Add</button>
              <%= if @notifications != [] do %>
                <button type="button" phx-click="delete_notifications" data-confirm="Remove all notifications?"
                  class="px-3 py-1 text-xs text-red-600 border border-red-300 rounded hover:bg-red-50">Remove All</button>
              <% end %>
            </div>
          </form>
        </div>
      </div>

      <%!-- Objects Table --%>
      <h2 class="text-lg font-semibold text-gray-900 mb-4">Objects</h2>
      <div class="bg-white shadow rounded-lg">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Key</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Size</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Last Modified</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <%= for obj <- @objects do %>
              <tr>
                <td class="px-6 py-4 font-mono text-sm">{obj.key}</td>
                <td class="px-6 py-4 text-gray-500">{format_size(obj[:size] || 0)}</td>
                <td class="px-6 py-4 text-gray-500">{obj[:updated_at] || obj[:created_at]}</td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @objects == [] do %>
          <p class="px-6 py-8 text-center text-gray-400">No objects in this bucket.</p>
        <% end %>
      </div>
    </div>
    """
  end

  defp load_config(socket) do
    bucket = socket.assigns.bucket_name

    versioning = Versioning.get_versioning(bucket)

    lifecycle_rules =
      case Lifecycle.get_rules(bucket) do
        {:ok, rules} -> rules
        _ -> []
      end

    notifications =
      case Notifications.get_config(bucket) do
        {:ok, configs} -> configs
        _ -> []
      end

    replicas =
      case ReplicationConfig.get_bucket_replicas(bucket) do
        {:ok, reps} -> reps
        _ -> []
      end

    socket
    |> assign(:versioning, versioning)
    |> assign(:lifecycle_rules, lifecycle_rules)
    |> assign(:notifications, notifications)
    |> assign(:replicas, replicas)
  end

  defp format_size(bytes) when bytes < 1024, do: "#{bytes} B"
  defp format_size(bytes) when bytes < 1024 * 1024, do: "#{Float.round(bytes / 1024, 1)} KB"
  defp format_size(bytes), do: "#{Float.round(bytes / (1024 * 1024), 1)} MB"
end
