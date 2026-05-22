defmodule ExStorageServiceWeb.BucketLive.Settings do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Versioning
  alias ExStorageService.Storage.Lifecycle
  alias ExStorageService.Notifications
  alias ExStorageService.Replication.Config, as: ReplicationConfig
  alias ExStorageService.CloudCache.Config, as: CloudConfig
  alias ExStorageService.CloudCache.Client, as: CloudClient
  alias ExStorageService.CloudCache.LocalStore
  alias ExStorageService.IAM.AccessKey
  alias ExStorageService.IAM.Policy
  alias ExStorageServiceS3.Plugs.Authorize
  alias ExStorageServiceS3.Presigned

  @impl true
  def mount(%{"name" => name}, _session, socket) do
    case Metadata.get_bucket(name) do
      {:ok, bucket} ->
        {:ok,
         socket
         |> assign(bucket: bucket, bucket_name: name)
         |> assign(versioning: :disabled, lifecycle_rules: [], notifications: [], replicas: [])
         |> assign(access_keys: [], presigned_url: nil)
         |> assign(cloud_cache: nil, cloud_cache_stats: nil, cloud_cache_test_result: nil)
         |> assign(show_confirm_modal: false, confirm_title: "", confirm_message: "",
                  confirm_event: "", confirm_params: %{})
         |> load_config()}

      {:error, :not_found} ->
        {:ok, socket |> put_flash(:error, "Bucket not found") |> redirect(to: ~p"/buckets")}
    end
  end

  @impl true
  def handle_params(_params, _url, socket), do: {:noreply, socket}

  # ── Event Handlers ──────────────────────────────────────────────────────────

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
    new_rules = socket.assigns.lifecycle_rules ++ [rule]

    case Lifecycle.put_rules(bucket, new_rules) do
      :ok -> {:noreply, socket |> put_flash(:info, "Lifecycle rule added") |> load_config()}
      {:ok, _} -> {:noreply, socket |> put_flash(:info, "Lifecycle rule added") |> load_config()}
      {:error, reason} -> {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
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
      new_configs = socket.assigns.notifications ++ [config]

      case Notifications.put_config(bucket, new_configs) do
        :ok -> {:noreply, socket |> put_flash(:info, "Notification added") |> load_config()}
        {:ok, _} -> {:noreply, socket |> put_flash(:info, "Notification added") |> load_config()}
        {:error, reason} -> {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
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

      new_replicas = socket.assigns.replicas ++ [replica]

      case ReplicationConfig.set_bucket_replicas(bucket, new_replicas) do
        :ok -> {:noreply, socket |> put_flash(:info, "Replica added") |> load_config()}
        {:error, reason} -> {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
      end
    end
  end

  def handle_event("remove_replicas", _params, socket) do
    ReplicationConfig.remove_bucket_replicas(socket.assigns.bucket_name)
    {:noreply, socket |> put_flash(:info, "Replicas removed") |> load_config()}
  end

  ## Cloud Cache events

  def handle_event("save_cloud_cache", params, socket) do
    bucket = socket.assigns.bucket_name
    provider = params["provider"] || "aws"
    endpoint = String.trim(params["endpoint"] || "")
    region = String.trim(params["region"] || "us-east-1")
    remote_bucket = String.trim(params["bucket"] || "")
    access_key_id = String.trim(params["access_key_id"] || "")
    secret_key = String.trim(params["secret_access_key"] || "")
    cache_max_gb = String.to_float(params["cache_max_gb"] || "10")
    cache_enabled = params["cache_enabled"] == "true"

    if remote_bucket == "" or access_key_id == "" do
      {:noreply, put_flash(socket, :error, "Remote bucket and access key ID are required")}
    else
      existing_enc =
        case socket.assigns.cloud_cache do
          %CloudConfig{encrypted_secret: enc} when enc != "" -> enc
          _ -> ""
        end

      config_params = %{
        enabled: true,
        provider: provider,
        endpoint: if(endpoint == "", do: nil, else: endpoint),
        region: region,
        bucket: remote_bucket,
        access_key_id: access_key_id,
        cache_max_bytes: round(cache_max_gb * 1024 * 1024 * 1024),
        cache_enabled: cache_enabled
      }

      # Only update secret if a new one was provided
      config_params =
        if secret_key != "" do
          Map.put(config_params, :secret_access_key, secret_key)
        else
          Map.put(config_params, :encrypted_secret, existing_enc)
        end

      case CloudConfig.set_config(bucket, config_params) do
        :ok -> {:noreply, socket |> put_flash(:info, "Cloud cache saved") |> load_config()}
        {:error, reason} -> {:noreply, put_flash(socket, :error, "Failed: #{inspect(reason)}")}
      end
    end
  end

  def handle_event("delete_cloud_cache", _params, socket) do
    bucket = socket.assigns.bucket_name
    CloudConfig.delete_config(bucket)
    {:noreply, socket |> put_flash(:info, "Cloud cache configuration removed") |> load_config()}
  end

  def handle_event("clear_cloud_cache", _params, socket) do
    bucket = socket.assigns.bucket_name
    LocalStore.clear(bucket)
    {:noreply, socket |> put_flash(:info, "Local cache cleared") |> load_config()}
  end

  def handle_event("test_cloud_connection", _params, socket) do
    case socket.assigns.cloud_cache do
      nil ->
        {:noreply, put_flash(socket, :error, "No cloud cache configured")}

      %CloudConfig{} = config ->
        result =
          case CloudClient.test_connection(config) do
            :ok -> {:ok, "Connection successful!"}
            {:error, :forbidden} -> {:error, "Connected but credentials may be invalid (403)"}
            {:error, :bucket_not_found} -> {:error, "Bucket not found on remote (404)"}
            {:error, reason} -> {:error, "Connection failed: #{inspect(reason)}"}
          end

        {:noreply, assign(socket, :cloud_cache_test_result, result)}
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
            action = Authorize.map_action(method, [bucket, object_key])
            resource = Authorize.build_resource_arn([bucket, object_key])

            case Policy.evaluate(key.user_id, action, resource) do
              :allow ->
                s3_port = Application.get_env(:ex_storage_service, :s3_port, 9000)

                url =
                  Presigned.generate_url(bucket, object_key,
                    access_key_id: key.access_key_id,
                    secret_access_key: key.secret_access_key,
                    method: method,
                    expires: expires,
                    host: "localhost:#{s3_port}",
                    scheme: "http"
                  )

                {:noreply, assign(socket, :presigned_url, url)}

              :deny ->
                {:noreply,
                 put_flash(socket, :error, "Access key owner lacks permission for #{action}")}
            end

          {:error, _} ->
            {:noreply, put_flash(socket, :error, "Access key not found")}
        end
    end
  end

  def handle_event("open_confirm_modal", %{"action" => action}, socket) do
    {title, message, event, _label} =
      case action do
        "delete_bucket" ->
          {"Delete Bucket",
           "Delete bucket \"#{socket.assigns.bucket_name}\"? This cannot be undone. The bucket must be empty.",
           "confirm_delete_bucket", "Delete"}

        "remove_replicas" ->
          {"Remove Replicas", "Remove all replicas?", "confirm_remove_replicas", "Remove All"}

        "delete_lifecycle" ->
          {"Remove Lifecycle Rules", "Remove all lifecycle rules?",
           "confirm_delete_lifecycle", "Remove All"}

        "delete_notifications" ->
          {"Remove Notifications", "Remove all notifications?",
           "confirm_delete_notifications", "Remove All"}

        "delete_cloud_cache" ->
          {"Remove Cloud Cache Config",
           "Remove cloud cache configuration for \"#{socket.assigns.bucket_name}\"? The local cache will also be cleared.",
           "confirm_delete_cloud_cache", "Remove"}

        _ ->
          {"Confirm", "Are you sure?", "", "Confirm"}
      end

    {:noreply,
     assign(socket,
       show_confirm_modal: true,
       confirm_title: title,
       confirm_message: message,
       confirm_event: event,
       confirm_params: %{}
     )}
  end

  def handle_event("close_confirm_modal", _params, socket) do
    {:noreply, assign(socket, show_confirm_modal: false)}
  end

  def handle_event("confirm_delete_bucket", _params, socket) do
    name = socket.assigns.bucket_name

    case Metadata.list_objects(name, max_keys: 1) do
      {:ok, %{keys: []}} ->
        Metadata.delete_bucket(name)

        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:info, "Bucket \"#{name}\" deleted")
         |> push_navigate(to: ~p"/buckets")}

      {:ok, _} ->
        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:error, "Cannot delete: bucket \"#{name}\" is not empty")}

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:error, "Failed: #{inspect(reason)}")}
    end
  end

  def handle_event("confirm_remove_replicas", _params, socket) do
    ReplicationConfig.remove_bucket_replicas(socket.assigns.bucket_name)
    {:noreply, socket |> assign(show_confirm_modal: false) |> put_flash(:info, "Replicas removed") |> load_config()}
  end

  def handle_event("confirm_delete_lifecycle", _params, socket) do
    Lifecycle.delete_rules(socket.assigns.bucket_name)
    {:noreply, socket |> assign(show_confirm_modal: false) |> put_flash(:info, "Lifecycle rules removed") |> load_config()}
  end

  def handle_event("confirm_delete_notifications", _params, socket) do
    Notifications.delete_config(socket.assigns.bucket_name)
    {:noreply, socket |> assign(show_confirm_modal: false) |> put_flash(:info, "Notifications removed") |> load_config()}
  end

  def handle_event("confirm_delete_cloud_cache", _params, socket) do
    bucket = socket.assigns.bucket_name
    CloudConfig.delete_config(bucket)
    LocalStore.clear(bucket)
    {:noreply, socket |> assign(show_confirm_modal: false) |> put_flash(:info, "Cloud cache removed") |> load_config()}
  end

  # ── Render ───────────────────────────────────────────────────────────────────

  @impl true
  def render(assigns) do
    ~H"""
    <div class="max-w-4xl mx-auto">
      <%!-- Breadcrumb nav --%>
      <div class="flex items-center gap-2 text-sm text-on-surface-variant mb-1">
        <.dm_link navigate={~p"/buckets"} class="text-primary">Buckets</.dm_link>
        <span>/</span>
        <.dm_link navigate={~p"/buckets/#{@bucket_name}"} class="text-primary">
          {@bucket_name}
        </.dm_link>
        <span>/</span>
        <span>Settings</span>
      </div>

      <%!-- Page header --%>
      <div class="flex items-center justify-between mt-2 mb-4">
        <h1 class="text-2xl font-bold text-on-surface">{@bucket_name}</h1>
        <button
          id="delete-bucket-btn"
          type="button"
          class="btn btn-error btn-sm"
          phx-click="open_confirm_modal"
          phx-value-action="delete_bucket"
        >
          Delete Bucket
        </button>
      </div>

      <%!-- Sub-nav tabs --%>
      <div class="flex gap-1 border-b border-outline-variant mb-6">
        <.dm_link
          navigate={~p"/buckets/#{@bucket_name}"}
          class="px-4 py-2 text-sm font-medium text-on-surface-variant hover:text-on-surface"
        >
          Overview
        </.dm_link>
        <.dm_link
          navigate={~p"/buckets/#{@bucket_name}/files"}
          class="px-4 py-2 text-sm font-medium text-on-surface-variant hover:text-on-surface"
        >
          Files
        </.dm_link>
        <.dm_link
          navigate={~p"/buckets/#{@bucket_name}/settings"}
          class="px-4 py-2 text-sm font-medium border-b-2 border-primary text-primary -mb-px"
        >
          Settings
        </.dm_link>
      </div>

      <%!-- Config grid --%>
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
                    class="btn btn-outline btn-error btn-xs"
                    phx-click="open_confirm_modal"
                    phx-value-action="remove_replicas"
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
                — expire after
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
                type="button"
                class="mt-2 text-xs text-error hover:underline"
                phx-click="open_confirm_modal"
                phx-value-action="delete_lifecycle"
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
                <span class="opacity-60 ml-1">
                  {Enum.join(notif[:events] || notif.events, ", ")}
                </span>
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
                    class="btn btn-outline btn-error btn-xs"
                    phx-click="open_confirm_modal"
                    phx-value-action="delete_notifications"
                  >
                    Remove All
                  </button>
                <% end %>
              </div>
            </form>
          </div>
        </div>
      </div>

      <%!-- Cloud Cache --%>
      <div class="card mb-8">
        <div class="card-body">
          <div class="flex items-center justify-between mb-3">
            <h3 class="card-title text-sm">Cloud Cache</h3>
            <%= if @cloud_cache do %>
              <span class={"badge badge-xs #{if @cloud_cache.enabled, do: "badge-success", else: "badge-ghost"}"}>
                {if @cloud_cache.enabled, do: "Enabled", else: "Disabled"}
              </span>
            <% end %>
          </div>

          <p class="text-xs text-on-surface-variant mb-4">
            Route object storage to AWS S3 or Cloudflare R2. Reads are served from a
            local LRU disk cache; writes go directly to the remote.
          </p>

          <%!-- Test result banner --%>
          <%= if @cloud_cache_test_result do %>
            <div class={"mb-3 p-2 rounded text-xs #{case @cloud_cache_test_result do; {:ok, _} -> "bg-success/10 text-success"; {:error, _} -> "bg-error/10 text-error"; end}"}>
              {case @cloud_cache_test_result do
                {:ok, msg} -> msg
                {:error, msg} -> msg
              end}
            </div>
          <% end %>

          <form id="cloud-cache-form" phx-submit="save_cloud_cache" class="space-y-3">
            <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <%!-- Provider --%>
              <div class="form-group">
                <label class="form-label text-xs">Provider</label>
                <select
                  id="cloud-cache-provider"
                  name="provider"
                  class="select select-primary w-full text-xs"
                  phx-change=""
                >
                  <option value="aws" selected={not @cloud_cache or @cloud_cache.provider == :aws}>AWS S3</option>
                  <option value="r2" selected={@cloud_cache && @cloud_cache.provider == :r2}>Cloudflare R2</option>
                </select>
              </div>

              <%!-- Region (AWS) --%>
              <div class="form-group">
                <label class="form-label text-xs">Region</label>
                <input
                  id="cloud-cache-region"
                  type="text"
                  name="region"
                  value={(@cloud_cache && @cloud_cache.region) || "us-east-1"}
                  placeholder="us-east-1"
                  class="input input-primary w-full text-xs"
                />
              </div>

              <%!-- Remote bucket --%>
              <div class="form-group">
                <label class="form-label text-xs">Remote Bucket</label>
                <input
                  id="cloud-cache-bucket"
                  type="text"
                  name="bucket"
                  value={(@cloud_cache && @cloud_cache.bucket) || ""}
                  placeholder="my-s3-bucket"
                  class="input input-primary w-full text-xs"
                />
              </div>

              <%!-- Custom endpoint (R2 / custom) --%>
              <div class="form-group">
                <label class="form-label text-xs">Custom Endpoint (R2 or custom)</label>
                <input
                  id="cloud-cache-endpoint"
                  type="text"
                  name="endpoint"
                  value={(@cloud_cache && @cloud_cache.endpoint) || ""}
                  placeholder="https://<account>.r2.cloudflarestorage.com"
                  class="input input-primary w-full text-xs"
                />
              </div>

              <%!-- Access Key ID --%>
              <div class="form-group">
                <label class="form-label text-xs">Access Key ID</label>
                <input
                  id="cloud-cache-access-key"
                  type="text"
                  name="access_key_id"
                  value={(@cloud_cache && @cloud_cache.access_key_id) || ""}
                  placeholder="AKIAIOSFODNN7EXAMPLE"
                  class="input input-primary w-full text-xs font-mono"
                />
              </div>

              <%!-- Secret Access Key --%>
              <div class="form-group">
                <label class="form-label text-xs">
                  Secret Access Key
                  <%= if @cloud_cache && @cloud_cache.encrypted_secret != "" do %>
                    <span class="opacity-50">(leave blank to keep existing)</span>
                  <% end %>
                </label>
                <input
                  id="cloud-cache-secret"
                  type="password"
                  name="secret_access_key"
                  placeholder={if @cloud_cache && @cloud_cache.encrypted_secret != "", do: "(unchanged)", else: "Secret key"}
                  class="input input-primary w-full text-xs font-mono"
                />
              </div>
            </div>

            <%!-- Cache settings --%>
            <div class="border-t border-outline-variant pt-3 mt-2">
              <div class="flex flex-wrap gap-4 items-end">
                <div class="form-group flex-1 min-w-40">
                  <label class="form-label text-xs">Local Cache Max Size (GB)</label>
                  <input
                    id="cloud-cache-max-gb"
                    type="number"
                    name="cache_max_gb"
                    value={Float.round((@cloud_cache && @cloud_cache.cache_max_bytes * 1.0 / (1024 * 1024 * 1024)) || 10.0, 1)}
                    min="0.1"
                    step="0.5"
                    class="input input-primary w-full text-xs"
                  />
                </div>

                <div class="form-group">
                  <label class="form-label text-xs">Local Cache</label>
                  <select id="cloud-cache-enabled" name="cache_enabled" class="select select-primary text-xs">
                    <option value="true" selected={not @cloud_cache or @cloud_cache.cache_enabled}>Enabled</option>
                    <option value="false" selected={@cloud_cache && not @cloud_cache.cache_enabled}>Disabled</option>
                  </select>
                </div>
              </div>

              <%!-- Cache stats --%>
              <%= if @cloud_cache_stats do %>
                <div class="mt-3 flex gap-4 text-xs text-on-surface-variant">
                  <span>Cached objects: <span class="font-medium text-on-surface">{@cloud_cache_stats.count}</span></span>
                  <span>Used: <span class="font-medium text-on-surface">{format_bytes(@cloud_cache_stats.total_bytes)}</span></span>
                  <span>Limit: <span class="font-medium text-on-surface">{format_bytes(@cloud_cache_stats.max_bytes)}</span></span>
                </div>
              <% end %>
            </div>

            <div class="flex flex-wrap gap-2 pt-1">
              <button id="save-cloud-cache-btn" type="submit" class="btn btn-primary btn-sm">Save</button>
              <%= if @cloud_cache do %>
                <button
                  id="test-cloud-connection-btn"
                  type="button"
                  class="btn btn-outline btn-sm"
                  phx-click="test_cloud_connection"
                >
                  Test Connection
                </button>
                <button
                  id="clear-cloud-cache-btn"
                  type="button"
                  class="btn btn-outline btn-warning btn-sm"
                  phx-click="clear_cloud_cache"
                >
                  Clear Local Cache
                </button>
                <button
                  id="delete-cloud-cache-btn"
                  type="button"
                  class="btn btn-outline btn-error btn-sm"
                  phx-click="open_confirm_modal"
                  phx-value-action="delete_cloud_cache"
                >
                  Remove Config
                </button>
              <% end %>
            </div>
          </form>
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

  # ── Private helpers ──────────────────────────────────────────────────────────

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

    cloud_cache =
      case CloudConfig.get_config(bucket) do
        {:ok, config} -> config
        _ -> nil
      end

    cloud_cache_stats =
      if cloud_cache do
        LocalStore.stats(bucket, cloud_cache.cache_max_bytes)
      else
        nil
      end

    access_keys = load_all_active_keys()

    socket
    |> assign(:versioning, versioning)
    |> assign(:lifecycle_rules, lifecycle_rules)
    |> assign(:notifications, notifications)
    |> assign(:replicas, replicas)
    |> assign(:cloud_cache, cloud_cache)
    |> assign(:cloud_cache_stats, cloud_cache_stats)
    |> assign(:access_keys, access_keys)
  end

  defp format_bytes(bytes) when bytes < 1024, do: "#{bytes} B"
  defp format_bytes(bytes) when bytes < 1_048_576, do: "#{Float.round(bytes / 1024, 1)} KB"
  defp format_bytes(bytes) when bytes < 1_073_741_824, do: "#{Float.round(bytes / 1_048_576, 1)} MB"
  defp format_bytes(bytes), do: "#{Float.round(bytes / 1_073_741_824, 2)} GB"

  defp load_all_active_keys do
    case Concord.get_all() do
      {:ok, all} ->
        all
        |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "access_key:") end)
        |> Enum.map(fn {_k, v} -> v end)
        |> Enum.filter(fn key -> key.status == :active end)
        |> Enum.map(fn key -> %{access_key_id: key.access_key_id, user_id: key.user_id} end)

      _ ->
        []
    end
  end
end
