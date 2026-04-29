defmodule ExStorageServiceWeb.DashboardLive do
  use ExStorageServiceWeb, :live_view

  alias ExStorageService.Metadata

  @refresh_interval 5_000

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      send(self(), :load_stats)
      :timer.send_interval(@refresh_interval, self(), :load_stats)
    end

    socket =
      socket
      |> assign(:page_title, "Dashboard")
      |> assign(:bucket_count, 0)
      |> assign(:object_count, 0)
      |> assign(:disk_usage, "0 B")
      |> assign(:elixir_version, System.version())
      |> assign(:otp_version, :erlang.system_info(:otp_release) |> List.to_string())
      |> assign(:node_name, node() |> Atom.to_string())
      |> assign(:uptime, format_uptime())
      |> assign(:replication_stats, %{pending: 0, running: 0, completed: 0, dead_letter: 0})

    {:ok, socket}
  end

  @impl true
  def handle_info(:load_stats, socket) do
    bucket_count =
      case Metadata.list_buckets() do
        {:ok, buckets} -> length(buckets)
        _ -> 0
      end

    object_count =
      case Concord.get_all() do
        {:ok, all} ->
          all
          |> Enum.count(fn {k, _v} -> String.starts_with?(k, "obj:") end)

        _ ->
          0
      end

    data_root =
      Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")

    disk_usage = calculate_disk_usage(data_root)

    replication_stats = load_replication_stats()

    socket =
      socket
      |> assign(:bucket_count, bucket_count)
      |> assign(:object_count, object_count)
      |> assign(:disk_usage, disk_usage)
      |> assign(:uptime, format_uptime())
      |> assign(:replication_stats, replication_stats)

    {:noreply, socket}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="space-y-6">
      <section class="ess-dashboard-hero bg-primary-container text-on-primary-container rounded-xl p-6 shadow-sm">
        <div class="max-w-4xl">
          <p class="text-sm font-semibold uppercase opacity-80">Storage Control Plane</p>
          <h1 class="mt-2 text-4xl font-bold leading-tight">Storage Dashboard</h1>
          <p class="mt-3 max-w-2xl text-base">
            Live operational overview for buckets, objects, disk pressure, and replication flow.
          </p>
        </div>
      </section>

      <section class="grid grid-cols-1 gap-6 md:grid-cols-3">
        <article class="card ess-card-hover">
          <div class="card-body p-5">
            <div class="flex items-center gap-4">
              <div class="flex h-12 w-12 items-center justify-center rounded-lg bg-primary text-primary-content">
                <.dm_mdi name="bucket-outline" class="h-6 w-6" />
              </div>
              <div>
                <p class="text-sm text-on-surface-variant">Total Buckets</p>
                <p class="text-3xl font-semibold text-on-surface">{@bucket_count}</p>
              </div>
            </div>
          </div>
        </article>

        <article class="card ess-card-hover">
          <div class="card-body p-5">
            <div class="flex items-center gap-4">
              <div class="flex h-12 w-12 items-center justify-center rounded-lg bg-secondary text-secondary-content">
                <.dm_mdi name="cube-outline" class="h-6 w-6" />
              </div>
              <div>
                <p class="text-sm text-on-surface-variant">Total Objects</p>
                <p class="text-3xl font-semibold text-on-surface">{@object_count}</p>
              </div>
            </div>
          </div>
        </article>

        <article class="card ess-card-hover">
          <div class="card-body p-5">
            <div class="flex items-center gap-4">
              <div class="flex h-12 w-12 items-center justify-center rounded-lg bg-tertiary text-tertiary-content">
                <.dm_mdi name="harddisk" class="h-6 w-6" />
              </div>
              <div>
                <p class="text-sm text-on-surface-variant">Disk Usage</p>
                <p class="text-3xl font-semibold text-on-surface">{@disk_usage}</p>
              </div>
            </div>
          </div>
        </article>
      </section>

      <section class="bg-surface-container-low text-on-surface rounded-xl p-6">
        <div class="flex items-center justify-between gap-4">
          <div>
            <h2 class="text-2xl font-semibold">Replication Queue</h2>
            <p class="mt-1 text-sm text-on-surface-variant">Current job lifecycle state counts.</p>
          </div>
        </div>

        <div class="mt-6 grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <article class="card">
            <div class="card-body p-4">
              <p class="text-sm text-on-surface-variant">Pending</p>
              <p class="mt-2 text-2xl font-semibold text-warning">{@replication_stats.pending}</p>
            </div>
          </article>
          <article class="card">
            <div class="card-body p-4">
              <p class="text-sm text-on-surface-variant">Running</p>
              <p class="mt-2 text-2xl font-semibold text-info">{@replication_stats.running}</p>
            </div>
          </article>
          <article class="card">
            <div class="card-body p-4">
              <p class="text-sm text-on-surface-variant">Completed</p>
              <p class="mt-2 text-2xl font-semibold text-success">{@replication_stats.completed}</p>
            </div>
          </article>
          <article class="card">
            <div class="card-body p-4">
              <p class="text-sm text-on-surface-variant">Dead Letter</p>
              <p class="mt-2 text-2xl font-semibold text-error">{@replication_stats.dead_letter}</p>
            </div>
          </article>
        </div>
      </section>

      <section class="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <article class="card card-elevated lg:col-span-2">
          <div class="card-header px-5 py-4">
            <h2 class="text-lg font-semibold">System Information</h2>
          </div>
          <.dm_table
            data={[
              %{label: "Elixir Version", value: @elixir_version},
              %{label: "OTP Version", value: @otp_version},
              %{label: "Node Name", value: @node_name, mono: true},
              %{label: "Uptime", value: @uptime}
            ]}
            hover
            compact
            class="w-full"
          >
            <:col :let={row} label="Metric" class="w-48 text-sm font-medium text-on-surface-variant">
              {row.label}
            </:col>
            <:col :let={row} label="Value" class="text-sm text-on-surface">
              <span class={row[:mono] && "font-mono"}>{row.value}</span>
            </:col>
          </.dm_table>
        </article>

        <article class="card card-elevated bg-surface-container-high text-on-surface">
          <div class="card-body p-5">
            <h2 class="text-lg font-semibold">Health Snapshot</h2>
            <div class="space-y-4">
              <div class="flex items-center justify-between gap-4">
                <span class="text-sm text-on-surface-variant">Metadata</span>
                <span class="font-medium text-success">Online</span>
              </div>
              <div class="flex items-center justify-between gap-4">
                <span class="text-sm text-on-surface-variant">Replication Lag</span>
                <span class="font-medium text-on-surface">{@replication_stats.pending}</span>
              </div>
              <div class="flex items-center justify-between gap-4">
                <span class="text-sm text-on-surface-variant">Refresh</span>
                <span class="font-medium text-on-surface">5s</span>
              </div>
            </div>
          </div>
        </article>
      </section>
    </div>
    """
  end

  defp load_replication_stats do
    case Concord.get_all() do
      {:ok, all} ->
        {pending, running, completed, dead_letter} =
          Enum.reduce(all, {0, 0, 0, 0}, fn {k, v}, {p, r, c, d} ->
            cond do
              String.starts_with?(k, "job:dead_letter:") ->
                {p, r, c, d + 1}

              String.starts_with?(k, "job:") ->
                status = v[:status] || v["status"]

                case status do
                  :pending -> {p + 1, r, c, d}
                  :running -> {p, r + 1, c, d}
                  :completed -> {p, r, c + 1, d}
                  _ -> {p, r, c, d}
                end

              true ->
                {p, r, c, d}
            end
          end)

        %{pending: pending, running: running, completed: completed, dead_letter: dead_letter}

      _ ->
        %{pending: 0, running: 0, completed: 0, dead_letter: 0}
    end
  end

  defp calculate_disk_usage(data_root) do
    case System.cmd("du", ["-sb", data_root], stderr_to_stdout: true) do
      {output, 0} ->
        case String.split(output, "\t") do
          [bytes_str | _] ->
            case Integer.parse(String.trim(bytes_str)) do
              {bytes, _} -> format_bytes(bytes)
              :error -> "N/A"
            end

          _ ->
            "N/A"
        end

      _ ->
        if File.dir?(data_root) do
          bytes = dir_size(data_root)
          format_bytes(bytes)
        else
          "0 B"
        end
    end
  end

  defp dir_size(path) do
    case File.ls(path) do
      {:ok, entries} ->
        Enum.reduce(entries, 0, fn entry, acc ->
          full = Path.join(path, entry)

          case File.stat(full) do
            {:ok, %{type: :regular, size: size}} -> acc + size
            {:ok, %{type: :directory}} -> acc + dir_size(full)
            _ -> acc
          end
        end)

      _ ->
        0
    end
  end

  defp format_bytes(bytes) when bytes < 1024, do: "#{bytes} B"
  defp format_bytes(bytes) when bytes < 1024 * 1024, do: "#{Float.round(bytes / 1024, 1)} KB"

  defp format_bytes(bytes) when bytes < 1024 * 1024 * 1024,
    do: "#{Float.round(bytes / (1024 * 1024), 1)} MB"

  defp format_bytes(bytes), do: "#{Float.round(bytes / (1024 * 1024 * 1024), 2)} GB"

  defp format_uptime do
    {uptime_ms, _} = :erlang.statistics(:wall_clock)
    total_seconds = div(uptime_ms, 1000)
    days = div(total_seconds, 86400)
    hours = div(rem(total_seconds, 86400), 3600)
    minutes = div(rem(total_seconds, 3600), 60)
    seconds = rem(total_seconds, 60)

    parts =
      [{days, "d"}, {hours, "h"}, {minutes, "m"}, {seconds, "s"}]
      |> Enum.reject(fn {v, _} -> v == 0 end)
      |> Enum.map(fn {v, u} -> "#{v}#{u}" end)

    case parts do
      [] -> "0s"
      _ -> Enum.join(parts, " ")
    end
  end
end
