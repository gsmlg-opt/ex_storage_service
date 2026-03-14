defmodule ExStorageServiceWeb.BucketLive.Show do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Versioning
  alias ExStorageService.Storage.Lifecycle
  alias ExStorageService.Notifications
  alias ExStorageService.Replication.Config, as: ReplicationConfig
  alias ExStorageService.IAM.AccessKey
  alias ExStorageService.S3.Presigned
  alias ExStorageService.Storage.Engine

  @impl true
  def mount(%{"name" => name}, _session, socket) do
    case Metadata.get_bucket(name) do
      {:ok, bucket} ->
        {:ok,
         socket
         |> assign(bucket: bucket, bucket_name: name, objects: [])
         |> assign(versioning: :disabled, lifecycle_rules: [], notifications: [], replicas: [])
         |> assign(access_keys: [], presigned_url: nil)
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

    events =
      String.split(params["events"] || "", ",")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))

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
      replica = %{
        endpoint: endpoint,
        access_key: access_key,
        secret_key_enc: "",
        bucket: remote_bucket
      }

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

  def handle_event("delete_object", %{"key" => key}, socket) do
    bucket = socket.assigns.bucket_name

    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        Metadata.delete_object_meta(bucket, key)
        Engine.delete_content(bucket, meta.content_hash)

        {:noreply,
         socket
         |> put_flash(:info, "Deleted #{key}")
         |> push_patch(to: ~p"/buckets/#{bucket}")}

      {:error, :not_found} ->
        {:noreply, put_flash(socket, :error, "Object not found")}
    end
  end

  def handle_event("generate_presigned", params, socket) do
    bucket = socket.assigns.bucket_name
    object_key = String.trim(params["object_key"] || "")
    access_key_id = String.trim(params["access_key_id"] || "")
    method = params["method"] || "GET"
    expires = String.to_integer(params["expires"] || "3600")

    cond do
      object_key == "" ->
        {:noreply, put_flash(socket, :error, "Object key is required")}

      access_key_id == "" ->
        {:noreply, put_flash(socket, :error, "Access key is required")}

      true ->
        case AccessKey.get_access_key(access_key_id) do
          {:ok, key} ->
            s3_port = Application.get_env(:ex_storage_service, :s3_port, 9000)
            host = "localhost:#{s3_port}"
            scheme = "http"

            url =
              Presigned.generate_url(bucket, object_key,
                access_key_id: key.access_key_id,
                secret_access_key: key.secret_access_key,
                method: method,
                expires: expires,
                host: host,
                scheme: scheme
              )

            {:noreply, assign(socket, :presigned_url, url)}

          {:error, _} ->
            {:noreply, put_flash(socket, :error, "Access key not found")}
        end
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="max-w-4xl mx-auto">
      <.dm_link navigate={~p"/buckets"} class="text-primary text-sm">
        &larr; Back to Buckets
      </.dm_link>
      <h1 class="text-2xl font-bold mb-6 mt-2 text-on-surface">{@bucket_name}</h1>

      <div class="grid grid-cols-1 gap-6 mb-8 lg:grid-cols-2">
        <%!-- Versioning --%>
        <div class="card">
          <div class="card-body">
            <h3 class="card-title text-sm">Versioning</h3>
            <p class="text-sm text-on-surface-variant mb-3">
              Current: <span class="font-medium text-on-surface">{@versioning}</span>
            </p>
            <div class="flex gap-2">
              <button
                phx-click="set_versioning"
                phx-value-state="enabled"
                class="btn btn-success btn-xs"
              >
                Enable
              </button>
              <button
                phx-click="set_versioning"
                phx-value-state="suspended"
                class="btn btn-warning btn-xs"
              >
                Suspend
              </button>
            </div>
          </div>
        </div>

        <%!-- Replication --%>
        <div class="card">
          <div class="card-body">
            <h3 class="card-title text-sm">Replication ({length(@replicas)} replicas)</h3>
            <%= for replica <- @replicas do %>
              <div class="text-xs text-on-surface-variant mb-1 font-mono truncate">
                {replica.endpoint} -> {replica.bucket || @bucket_name}
              </div>
            <% end %>
            <form phx-submit="add_replica" class="mt-3 space-y-2">
              <input
                type="text"
                name="endpoint"
                placeholder="https://peer:9000"
                class="input input-primary w-full text-xs"
              />
              <div class="flex gap-2">
                <input
                  type="text"
                  name="access_key"
                  placeholder="Access Key"
                  class="input input-primary flex-1 text-xs"
                />
                <input
                  type="text"
                  name="remote_bucket"
                  placeholder="Remote bucket"
                  class="input input-primary flex-1 text-xs"
                />
              </div>
              <div class="flex gap-2">
                <button type="submit" class="btn btn-primary btn-xs">Add Replica</button>
                <%= if @replicas != [] do %>
                  <button
                    type="button"
                    phx-click="remove_replicas"
                    data-confirm="Remove all replicas?"
                    class="btn btn-outline btn-error btn-xs"
                  >
                    Remove All
                  </button>
                <% end %>
              </div>
            </form>
          </div>
        </div>

        <%!-- Lifecycle --%>
        <div class="card">
          <div class="card-body">
            <h3 class="card-title text-sm">Lifecycle Rules ({length(@lifecycle_rules)})</h3>
            <%= for rule <- @lifecycle_rules do %>
              <div class="text-xs text-on-surface-variant mb-1">
                Prefix: "<span class="font-mono">{rule[:prefix] || rule.prefix}</span>"
                Expire after
                <span class="font-medium">{rule[:expiration_days] || rule.expiration_days}</span>
                days
                (<span class={
                  if (rule[:status] || rule.status) == "Enabled",
                    do: "text-success",
                    else: "text-on-surface-variant"
                }>{rule[:status] || rule.status}</span>)
              </div>
            <% end %>
            <form phx-submit="add_lifecycle_rule" class="mt-3 flex gap-2 items-end">
              <input
                type="text"
                name="prefix"
                placeholder="Prefix (e.g. logs/)"
                class="input input-primary flex-1 text-xs"
              />
              <input
                type="number"
                name="expiration_days"
                placeholder="Days"
                min="1"
                class="input input-primary w-20 text-xs"
              />
              <button type="submit" class="btn btn-primary btn-xs">Add</button>
            </form>
            <%= if @lifecycle_rules != [] do %>
              <button
                phx-click="delete_lifecycle_rules"
                data-confirm="Remove all rules?"
                class="mt-2 text-xs text-error hover:underline"
              >
                Remove All Rules
              </button>
            <% end %>
          </div>
        </div>

        <%!-- Notifications --%>
        <div class="card">
          <div class="card-body">
            <h3 class="card-title text-sm">Notifications ({length(@notifications)})</h3>
            <%= for notif <- @notifications do %>
              <div class="text-xs text-on-surface-variant mb-1">
                <span class="font-mono truncate">{notif[:endpoint] || notif.endpoint}</span>
                <span class="opacity-60 ml-1">{Enum.join(notif[:events] || notif.events, ", ")}</span>
              </div>
            <% end %>
            <form phx-submit="add_notification" class="mt-3 space-y-2">
              <input
                type="text"
                name="endpoint"
                placeholder="https://example.com/webhook"
                class="input input-primary w-full text-xs"
              />
              <input
                type="text"
                name="events"
                placeholder="s3:ObjectCreated:*,s3:ObjectRemoved:*"
                class="input input-primary w-full text-xs"
              />
              <div class="flex gap-2">
                <button type="submit" class="btn btn-primary btn-xs">Add</button>
                <%= if @notifications != [] do %>
                  <button
                    type="button"
                    phx-click="delete_notifications"
                    data-confirm="Remove all notifications?"
                    class="btn btn-outline btn-error btn-xs"
                  >
                    Remove All
                  </button>
                <% end %>
              </div>
            </form>
          </div>
        </div>
      </div>

      <%!-- Presigned URL Generator --%>
      <div class="card mb-8">
        <div class="card-body">
          <h3 class="card-title text-sm">Presigned URL Generator</h3>
          <form phx-submit="generate_presigned" class="space-y-3">
            <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <input
                type="text"
                name="object_key"
                placeholder="Object key (e.g. photos/image.jpg)"
                class="input input-primary w-full text-xs"
              />
              <select name="access_key_id" class="select select-primary w-full text-xs">
                <option value="">Select access key...</option>
                <%= for key <- @access_keys do %>
                  <option value={key.access_key_id}>{key.access_key_id} ({key.user_id})</option>
                <% end %>
              </select>
            </div>
            <div class="flex gap-3 items-end">
              <div class="form-group">
                <label class="form-label text-xs">Method</label>
                <select name="method" class="select select-primary text-xs">
                  <option value="GET">GET</option>
                  <option value="PUT">PUT</option>
                </select>
              </div>
              <div class="form-group">
                <label class="form-label text-xs">Expires (seconds)</label>
                <input
                  type="number"
                  name="expires"
                  value="3600"
                  min="1"
                  max="604800"
                  class="input input-primary w-28 text-xs"
                />
              </div>
              <button type="submit" class="btn btn-primary btn-sm">Generate URL</button>
            </div>
          </form>
          <%= if @presigned_url do %>
            <div class="mt-3 p-3 bg-surface-container rounded-lg">
              <label class="form-label text-xs">Generated URL</label>
              <div class="font-mono text-xs break-all text-on-surface select-all">
                {@presigned_url}
              </div>
            </div>
          <% end %>
        </div>
      </div>

      <%!-- Objects Table --%>
      <h2 class="text-lg font-semibold text-on-surface mb-4">Objects</h2>
      <div class="card">
        <table class="table table-hover w-full">
          <thead>
            <tr>
              <th class="text-on-surface-variant">Key</th>
              <th class="text-on-surface-variant">Size</th>
              <th class="text-on-surface-variant">Last Modified</th>
              <th class="text-on-surface-variant text-right">Actions</th>
            </tr>
          </thead>
          <tbody>
            <%= for obj <- @objects do %>
              <tr>
                <td class="font-mono text-sm">{obj.key}</td>
                <td class="text-on-surface-variant">{format_size(obj[:size] || 0)}</td>
                <td class="text-on-surface-variant">{obj[:updated_at] || obj[:created_at]}</td>
                <td class="text-right">
                  <button
                    phx-click="delete_object"
                    phx-value-key={obj.key}
                    data-confirm={"Delete #{obj.key}?"}
                    class="btn btn-ghost btn-xs text-error"
                  >
                    Delete
                  </button>
                </td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @objects == [] do %>
          <p class="px-6 py-8 text-center text-on-surface-variant">No objects in this bucket.</p>
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

    access_keys = load_all_active_keys()

    socket
    |> assign(:versioning, versioning)
    |> assign(:lifecycle_rules, lifecycle_rules)
    |> assign(:notifications, notifications)
    |> assign(:replicas, replicas)
    |> assign(:access_keys, access_keys)
  end

  defp load_all_active_keys do
    case Concord.get_all() do
      {:ok, all} ->
        all
        |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "access_key:") end)
        |> Enum.map(fn {_k, v} -> v end)
        |> Enum.filter(fn key -> key.status == :active end)
        |> Enum.map(fn key ->
          %{access_key_id: key.access_key_id, user_id: key.user_id}
        end)

      _ ->
        []
    end
  end

  defp format_size(bytes) when bytes < 1024, do: "#{bytes} B"
  defp format_size(bytes) when bytes < 1024 * 1024, do: "#{Float.round(bytes / 1024, 1)} KB"
  defp format_size(bytes), do: "#{Float.round(bytes / (1024 * 1024), 1)} MB"
end
