defmodule ExStorageServiceWeb.BucketLive.Show do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Versioning
  alias ExStorageService.CloudCache.Config, as: CloudConfig
  alias ExStorageService.CloudCache.Client, as: CloudClient

  @impl true
  def mount(%{"name" => name}, _session, socket) do
    cloud_cache =
      case CloudConfig.get_active_config(name) do
        {:ok, config} -> config
        :disabled -> nil
      end

    case Metadata.get_bucket(name) do
      {:ok, bucket} ->
        {:ok,
         socket
         |> assign(bucket: bucket, bucket_name: name, cloud_cache: cloud_cache)
         |> assign(object_count: 0, total_size: 0, versioning: :disabled)
         |> load_summary()}

      {:error, :not_found} ->
        {:ok, socket |> put_flash(:error, "Bucket not found") |> redirect(to: ~p"/buckets")}
    end
  end

  @impl true
  def handle_params(_params, _url, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <div class="max-w-4xl mx-auto">
      <%!-- Breadcrumb --%>
      <div class="flex items-center gap-2 text-sm text-on-surface-variant mb-1">
        <.dm_link navigate={~p"/buckets"} class="text-primary">Buckets</.dm_link>
        <span>/</span>
        <span class="text-on-surface font-medium">{@bucket_name}</span>
      </div>

      <%!-- Header --%>
      <div class="flex items-center justify-between mt-2 mb-4">
        <h1 class="text-2xl font-bold text-on-surface">{@bucket_name}</h1>
        <.dm_link navigate={~p"/buckets/#{@bucket_name}/settings"} class="btn btn-ghost btn-sm gap-2">
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
            <circle cx="12" cy="12" r="3" />
            <path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83 0 2 2 0 010-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 010-2.83 2 2 0 012.83 0l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 0 2 2 0 010 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z" />
          </svg>
          Settings
        </.dm_link>
      </div>

      <%!-- Sub-nav tabs --%>
      <div class="flex gap-1 border-b border-outline-variant mb-6">
        <.dm_link
          navigate={~p"/buckets/#{@bucket_name}"}
          class="px-4 py-2 text-sm font-medium border-b-2 border-primary text-primary -mb-px"
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
          class="px-4 py-2 text-sm font-medium text-on-surface-variant hover:text-on-surface"
        >
          Settings
        </.dm_link>
      </div>

      <%!-- Stat cards --%>
      <div class="grid grid-cols-1 gap-4 sm:grid-cols-3 mb-8">
        <div class="card">
          <div class="card-body py-4 px-5">
            <p class="text-xs text-on-surface-variant uppercase tracking-wide mb-1">Objects</p>
            <p class="text-3xl font-bold text-on-surface">{@object_count}</p>
          </div>
        </div>
        <div class="card">
          <div class="card-body py-4 px-5">
            <p class="text-xs text-on-surface-variant uppercase tracking-wide mb-1">Total Size</p>
            <p class="text-3xl font-bold text-on-surface">{format_size(@total_size)}</p>
          </div>
        </div>
        <div class="card">
          <div class="card-body py-4 px-5">
            <p class="text-xs text-on-surface-variant uppercase tracking-wide mb-1">Versioning</p>
            <p class="text-xl font-semibold text-on-surface capitalize">{@versioning}</p>
          </div>
        </div>
      </div>

      <%!-- Quick links --%>
      <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <.dm_link
          navigate={~p"/buckets/#{@bucket_name}/files"}
          class="card hover:shadow-md transition-shadow cursor-pointer"
        >
          <div class="card-body py-5 px-6 flex items-center gap-4">
            <div class="rounded-full bg-primary/10 p-3">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                class="w-6 h-6 text-primary"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
              >
                <path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z" />
              </svg>
            </div>
            <div>
              <p class="font-semibold text-on-surface">Browse Files</p>
              <p class="text-sm text-on-surface-variant">View and manage objects</p>
            </div>
          </div>
        </.dm_link>

        <.dm_link
          navigate={~p"/buckets/#{@bucket_name}/settings"}
          class="card hover:shadow-md transition-shadow cursor-pointer"
        >
          <div class="card-body py-5 px-6 flex items-center gap-4">
            <div class="rounded-full bg-primary/10 p-3">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                class="w-6 h-6 text-primary"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
              >
                <circle cx="12" cy="12" r="3" />
                <path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83 0 2 2 0 010-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 010-2.83 2 2 0 012.83 0l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 0 2 2 0 010 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z" />
              </svg>
            </div>
            <div>
              <p class="font-semibold text-on-surface">Settings</p>
              <p class="text-sm text-on-surface-variant">Versioning, lifecycle, replication</p>
            </div>
          </div>
        </.dm_link>
      </div>

      <%!-- Metadata --%>
      <div class="card mt-6">
        <div class="card-body py-4 px-6">
          <h3 class="card-title text-sm mb-3">Bucket Info</h3>
          <dl class="grid grid-cols-2 gap-x-6 gap-y-2 text-sm sm:grid-cols-3">
            <div>
              <dt class="text-on-surface-variant text-xs uppercase tracking-wide">Name</dt>
              <dd class="font-mono font-medium">{@bucket_name}</dd>
            </div>
            <div>
              <dt class="text-on-surface-variant text-xs uppercase tracking-wide">Created</dt>
              <dd>{@bucket.creation_date}</dd>
            </div>
            <div>
              <dt class="text-on-surface-variant text-xs uppercase tracking-wide">Versioning</dt>
              <dd class="capitalize">{@versioning}</dd>
            </div>
          </dl>
        </div>
      </div>
    </div>
    """
  end

  defp load_summary(socket) do
    bucket = socket.assigns.bucket_name

    {count, size} =
      case socket.assigns.cloud_cache do
        nil ->
          # Local storage: paginate through all Concord metadata
          collect_local_summary(bucket, nil, 0, 0)

        cloud_config ->
          # Cloud-cached bucket: paginate through all remote objects
          collect_cloud_summary(cloud_config, nil, 0, 0)
      end

    versioning = Versioning.get_versioning(bucket)

    socket
    |> assign(:object_count, count)
    |> assign(:total_size, size)
    |> assign(:versioning, versioning)
  end

  defp collect_local_summary(bucket, continuation_token, count_acc, size_acc) do
    opts =
      [max_keys: 1000] ++
        if(continuation_token, do: [continuation_token: continuation_token], else: [])

    case Metadata.list_objects(bucket, opts) do
      {:ok, %{keys: keys, is_truncated: is_truncated, next_continuation_token: next_token}} ->
        page_size = keys |> Enum.map(fn {_k, meta} -> Map.get(meta, :size, 0) end) |> Enum.sum()
        new_count = count_acc + length(keys)
        new_size = size_acc + page_size

        if is_truncated and next_token do
          collect_local_summary(bucket, next_token, new_count, new_size)
        else
          {new_count, new_size}
        end

      _ ->
        {count_acc, size_acc}
    end
  end

  defp collect_cloud_summary(cloud_config, continuation_token, count_acc, size_acc) do
    opts =
      [max_keys: 1000, delimiter: ""] ++
        if(continuation_token, do: [continuation_token: continuation_token], else: [])

    case CloudClient.list_objects(cloud_config, opts) do
      {:ok, %{keys: keys, truncated: truncated, next_continuation_token: next_token}} ->
        page_size = keys |> Enum.map(fn {_k, meta} -> Map.get(meta, :size, 0) end) |> Enum.sum()
        new_count = count_acc + length(keys)
        new_size = size_acc + page_size

        if truncated and next_token do
          collect_cloud_summary(cloud_config, next_token, new_count, new_size)
        else
          {new_count, new_size}
        end

      {:error, _reason} ->
        {count_acc, size_acc}
    end
  end

  defp format_size(bytes) when bytes < 1024, do: "#{bytes} B"
  defp format_size(bytes) when bytes < 1_048_576, do: "#{Float.round(bytes / 1024, 1)} KB"
  defp format_size(bytes), do: "#{Float.round(bytes / 1_048_576, 1)} MB"
end
