defmodule ExStorageServiceWeb.BucketLive.Files do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Engine
  alias ExStorageService.CloudCache.Config, as: CloudConfig
  alias ExStorageService.CloudCache.Client, as: CloudClient

  @impl true
  def mount(%{"name" => name}, _session, socket) do
    case Metadata.get_bucket(name) do
      {:ok, _bucket} ->
        # Subscribe to real-time bucket change notifications
        if connected?(socket) do
          Phoenix.PubSub.subscribe(ExStorageService.PubSub, "bucket:#{name}")
        end

        cloud_cache =
          case CloudConfig.get_active_config(name) do
            {:ok, config} -> config
            :disabled -> nil
          end

        {:ok,
         socket
         |> assign(bucket_name: name, prefix: "", objects: [], folders: [], total_count: 0)
         |> assign(cloud_cache: cloud_cache, cloud_loading: false, cloud_error: nil)
         |> assign(show_confirm_modal: false, confirm_title: "", confirm_message: "",
                  confirm_event: "", confirm_params: %{})}

      {:error, :not_found} ->
        {:ok, socket |> put_flash(:error, "Bucket not found") |> redirect(to: ~p"/buckets")}
    end
  end

  @impl true
  def handle_params(params, _url, socket) do
    prefix = Map.get(params, "prefix", "")
    {:noreply, socket |> assign(:prefix, prefix) |> load_objects()}
  end

  @impl true
  def handle_event("navigate_folder", %{"prefix" => prefix}, socket) do
    {:noreply,
     push_patch(socket, to: ~p"/buckets/#{socket.assigns.bucket_name}/files?#{%{prefix: prefix}}")}
  end

  def handle_event("navigate_up", _params, socket) do
    parent = parent_prefix(socket.assigns.prefix)

    path =
      if parent == "" do
        ~p"/buckets/#{socket.assigns.bucket_name}/files"
      else
        ~p"/buckets/#{socket.assigns.bucket_name}/files?#{%{prefix: parent}}"
      end

    {:noreply, push_patch(socket, to: path)}
  end

  def handle_event("open_confirm_modal", %{"key" => key, "name" => name}, socket) do
    {:noreply,
     assign(socket,
       show_confirm_modal: true,
       confirm_title: "Delete Object",
       confirm_message: "Delete \"#{name}\"?",
       confirm_event: "confirm_delete_object",
       confirm_params: %{"key" => key}
     )}
  end

  def handle_event("close_confirm_modal", _params, socket) do
    {:noreply, assign(socket, show_confirm_modal: false)}
  end

  def handle_event("confirm_delete_object", %{"key" => key}, socket) do
    bucket = socket.assigns.bucket_name

    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        Metadata.delete_object_meta(bucket, key)
        Engine.delete_content(bucket, meta.content_hash)

        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:info, "Deleted \"#{key}\"")
         |> load_objects()}

      {:error, :not_found} ->
        {:noreply,
         socket
         |> assign(show_confirm_modal: false)
         |> put_flash(:error, "Object not found")}
    end
  end

  @impl true
  def handle_info({:bucket_changed, _event}, socket) do
    {:noreply, load_objects(socket)}
  end

  def handle_info(_msg, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <div class="max-w-4xl mx-auto">
      <%!-- Breadcrumb --%>
      <div class="flex items-center gap-2 text-sm text-on-surface-variant mb-1">
        <.dm_link navigate={~p"/buckets"} class="text-primary">Buckets</.dm_link>
        <span>/</span>
        <.dm_link navigate={~p"/buckets/#{@bucket_name}"} class="text-primary">
          {@bucket_name}
        </.dm_link>
        <span>/</span>
        <%= if @prefix == "" do %>
          <span class="text-on-surface font-medium">Files</span>
        <% else %>
          <.dm_link
            patch={~p"/buckets/#{@bucket_name}/files"}
            class="text-primary"
          >
            Files
          </.dm_link>
          <%= for {segment, seg_prefix} <- path_segments(@prefix) do %>
            <span>/</span>
            <%= if seg_prefix == @prefix do %>
              <span class="text-on-surface font-medium">{segment}</span>
            <% else %>
              <.dm_link
                patch={~p"/buckets/#{@bucket_name}/files?#{%{prefix: seg_prefix}}"}
                class="text-primary"
              >
                {segment}
              </.dm_link>
            <% end %>
          <% end %>
        <% end %>
      </div>

      <%!-- Header --%>
      <div class="flex items-center justify-between mt-2 mb-4">
        <div class="flex items-center gap-3">
          <h1 class="text-2xl font-bold text-on-surface">{@bucket_name}</h1>
          <%= if @cloud_cache do %>
            <span class="badge badge-xs badge-primary gap-1">
              <svg xmlns="http://www.w3.org/2000/svg" class="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M18 10h-1.26A8 8 0 109 20h9a5 5 0 000-10z"/>
              </svg>
              Cloud Cache
            </span>
          <% end %>
        </div>
        <div class="flex items-center gap-3">
          <%= if @cloud_error do %>
            <span class="text-xs text-error">{@cloud_error}</span>
          <% end %>
          <span class="text-sm text-on-surface-variant">
            {length(@folders)} folder{if length(@folders) != 1, do: "s", else: ""},
            {length(@objects)} file{if length(@objects) != 1, do: "s", else: ""}
          </span>
        </div>
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
          class="px-4 py-2 text-sm font-medium border-b-2 border-primary text-primary -mb-px"
        >
          Files
        </.dm_link>
        <.dm_link
          navigate={~p"/buckets/#{@bucket_name}/settings"}
          class="px-4 py-2 text-sm font-medium text-on-surface-variant hover:text-on-surface"
        >
          Settings
        </.dm_link>
      </div>

      <%!-- File browser --%>
      <div class="card">
        <%!-- Path bar --%>
        <div class="flex items-center gap-1 px-4 py-3 border-b border-outline-variant bg-surface-variant/30 flex-wrap">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            class="w-4 h-4 text-on-surface-variant shrink-0"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
          >
            <path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z" />
          </svg>
          <%!-- Root slash — always a link unless already at root --%>
          <%= if @prefix == "" do %>
            <span class="text-sm font-mono text-on-surface font-semibold">/</span>
          <% else %>
            <.dm_link
              patch={~p"/buckets/#{@bucket_name}/files"}
              class="text-sm font-mono text-primary hover:underline"
            >/
            </.dm_link>
          <% end %>
          <%!-- Each path segment --%>
          <%= for {segment, seg_prefix} <- path_segments(@prefix) do %>
            <%= if seg_prefix == @prefix do %>
              <span class="text-sm font-mono text-on-surface font-semibold">{segment}/</span>
            <% else %>
              <.dm_link
                patch={~p"/buckets/#{@bucket_name}/files?#{%{prefix: seg_prefix}}"}
                class="text-sm font-mono text-primary hover:underline"
              >
                {segment}/
              </.dm_link>
            <% end %>
          <% end %>
        </div>

        <table class="table table-hover w-full">
          <thead>
            <tr>
              <th class="text-on-surface-variant w-8"></th>
              <th class="text-on-surface-variant">Name</th>
              <th class="text-on-surface-variant">Size</th>
              <th class="text-on-surface-variant">Last Modified</th>
              <th class="text-on-surface-variant text-right">Actions</th>
            </tr>
          </thead>
          <tbody>
            <%!-- Go up row --%>
            <%= if @prefix != "" do %>
              <tr
                class="cursor-pointer"
                phx-click="navigate_up"
              >
                <td class="w-8">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    class="w-5 h-5 text-on-surface-variant"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="2"
                    stroke-linecap="round"
                    stroke-linejoin="round"
                  >
                    <polyline points="15 18 9 12 15 6" />
                  </svg>
                </td>
                <td colspan="4" class="text-sm text-on-surface-variant">..</td>
              </tr>
            <% end %>
            <%!-- Folder rows --%>
            <%= for folder <- @folders do %>
              <tr
                class="cursor-pointer"
                phx-click="navigate_folder"
                phx-value-prefix={folder}
              >
                <td class="w-8">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    class="w-5 h-5 text-warning"
                    viewBox="0 0 24 24"
                    fill="currentColor"
                    stroke="currentColor"
                    stroke-width="0"
                  >
                    <path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z" />
                  </svg>
                </td>
                <td class="font-medium text-sm">{display_folder_name(folder, @prefix)}</td>
                <td class="text-on-surface-variant text-sm">—</td>
                <td class="text-on-surface-variant text-sm">—</td>
                <td></td>
              </tr>
            <% end %>
            <%!-- File rows --%>
            <%= for obj <- @objects do %>
              <tr>
                <td class="w-8">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    class="w-5 h-5 text-on-surface-variant"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="2"
                    stroke-linecap="round"
                    stroke-linejoin="round"
                  >
                    <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
                    <polyline points="14 2 14 8 20 8" />
                  </svg>
                </td>
                <td class="font-mono text-sm">{display_file_name(obj.key, @prefix)}</td>
                <td class="text-on-surface-variant text-sm">{format_size(obj[:size] || 0)}</td>
                <td class="text-on-surface-variant text-sm">
                  {obj[:updated_at] || obj[:last_modified] || obj[:created_at]}
                </td>
                <td class="text-right">
                  <%= if is_nil(@cloud_cache) do %>
                    <button
                      type="button"
                      class="btn btn-ghost btn-xs text-error hover:btn-error"
                      phx-click="open_confirm_modal"
                      phx-value-key={obj.key}
                      phx-value-name={display_file_name(obj.key, @prefix)}
                    >
                      Delete
                    </button>
                  <% end %>
                </td>
              </tr>
            <% end %>
          </tbody>
        </table>
        <%= if @folders == [] and @objects == [] do %>
          <div class="flex flex-col items-center gap-2 px-6 py-16 text-on-surface-variant">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              class="w-10 h-10 opacity-30"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="1.5"
            >
              <path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z" />
            </svg>
            <p class="text-sm">
              <%= if @prefix == "" do %>
                No objects in this bucket.
              <% else %>
                This folder is empty.
              <% end %>
            </p>
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

  defp load_objects(socket) do
    bucket = socket.assigns.bucket_name
    prefix = socket.assigns.prefix

    case socket.assigns.cloud_cache do
      nil ->
        # Local storage: read from Concord metadata
        case Metadata.list_objects(bucket, prefix: prefix, delimiter: "/") do
          {:ok, %{keys: keys, common_prefixes: common_prefixes}} ->
            objects = Enum.map(keys, fn {key, meta} -> Map.put(meta, :key, key) end)

            socket
            |> assign(:objects, objects)
            |> assign(:folders, common_prefixes)
            |> assign(:cloud_error, nil)

          _ ->
            socket
            |> assign(:objects, [])
            |> assign(:folders, [])
            |> assign(:cloud_error, nil)
        end

      cloud_config ->
        # Cloud cache: list from the remote S3/R2/MinIO bucket
        case CloudClient.list_objects(cloud_config, prefix: prefix, delimiter: "/") do
          {:ok, %{keys: keys, common_prefixes: common_prefixes}} ->
            objects = Enum.map(keys, fn {key, meta} -> Map.put(meta, :key, key) end)

            socket
            |> assign(:objects, objects)
            |> assign(:folders, common_prefixes)
            |> assign(:cloud_error, nil)

          {:error, reason} ->
            socket
            |> assign(:objects, [])
            |> assign(:folders, [])
            |> assign(:cloud_error, "Failed to list remote objects: #{inspect(reason)}")
        end
    end
  end

  # Compute the parent prefix: "a/b/c/" -> "a/b/", "a/" -> ""
  defp parent_prefix(prefix) do
    prefix
    |> String.trim_trailing("/")
    |> String.split("/")
    |> Enum.drop(-1)
    |> case do
      [] -> ""
      parts -> Enum.join(parts, "/") <> "/"
    end
  end

  # Build breadcrumb segments: "a/b/c/" -> [{"a", "a/"}, {"b", "a/b/"}, {"c", "a/b/c/"}]
  defp path_segments(prefix) do
    prefix
    |> String.trim_trailing("/")
    |> String.split("/")
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce([], fn segment, acc ->
      parent =
        case acc do
          [] -> ""
          [{_, prev} | _] -> prev
        end

      [{segment, parent <> segment <> "/"} | acc]
    end)
    |> Enum.reverse()
  end

  defp display_folder_name(folder, prefix) do
    folder
    |> String.trim()
    |> String.replace_prefix(prefix, "")
    |> String.trim_trailing("/")
  end

  # Display just the file name: "a/b/file.txt" with prefix "a/b/" -> "file.txt"
  defp display_file_name(key, prefix) do
    String.replace_prefix(key, prefix, "")
  end

  defp format_size(bytes) when bytes < 1024, do: "#{bytes} B"
  defp format_size(bytes) when bytes < 1_048_576, do: "#{Float.round(bytes / 1024, 1)} KB"
  defp format_size(bytes) when bytes < 1_073_741_824, do: "#{Float.round(bytes / 1_048_576, 1)} MB"
  defp format_size(bytes), do: "#{Float.round(bytes / 1_073_741_824, 1)} GB"
end
