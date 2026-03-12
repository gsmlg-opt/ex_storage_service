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

    data_root = Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")
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
    <div>
      <div class="flex items-center justify-between">
        <.header>
          Storage Dashboard
          <:subtitle>Overview of your storage service</:subtitle>
        </.header>
        <form action="/logout" method="post">
          <input type="hidden" name="_csrf_token" value={Plug.CSRFProtection.get_csrf_token()} />
          <input type="hidden" name="_method" value="delete" />
          <button type="submit" class="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-900 border border-gray-300 rounded-md hover:bg-gray-50">
            Logout
          </button>
        </form>
      </div>

      <div class="mt-8 grid grid-cols-1 gap-6 sm:grid-cols-3">
        <div class="bg-white overflow-hidden shadow rounded-lg">
          <div class="p-5">
            <div class="flex items-center">
              <div class="flex-shrink-0">
                <div class="h-10 w-10 rounded-md bg-indigo-500 flex items-center justify-center">
                  <span class="text-white text-lg font-bold">B</span>
                </div>
              </div>
              <div class="ml-5 w-0 flex-1">
                <dl>
                  <dt class="text-sm font-medium text-gray-500 truncate">Total Buckets</dt>
                  <dd class="text-2xl font-semibold text-gray-900">{@bucket_count}</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div class="bg-white overflow-hidden shadow rounded-lg">
          <div class="p-5">
            <div class="flex items-center">
              <div class="flex-shrink-0">
                <div class="h-10 w-10 rounded-md bg-green-500 flex items-center justify-center">
                  <span class="text-white text-lg font-bold">O</span>
                </div>
              </div>
              <div class="ml-5 w-0 flex-1">
                <dl>
                  <dt class="text-sm font-medium text-gray-500 truncate">Total Objects</dt>
                  <dd class="text-2xl font-semibold text-gray-900">{@object_count}</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div class="bg-white overflow-hidden shadow rounded-lg">
          <div class="p-5">
            <div class="flex items-center">
              <div class="flex-shrink-0">
                <div class="h-10 w-10 rounded-md bg-yellow-500 flex items-center justify-center">
                  <span class="text-white text-lg font-bold">D</span>
                </div>
              </div>
              <div class="ml-5 w-0 flex-1">
                <dl>
                  <dt class="text-sm font-medium text-gray-500 truncate">Disk Usage</dt>
                  <dd class="text-2xl font-semibold text-gray-900">{@disk_usage}</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>
      </div>

      <%!-- Replication Job Queue Status --%>
      <div class="mt-8">
        <h2 class="text-lg font-semibold text-gray-900 mb-4">Replication Queue</h2>
        <div class="grid grid-cols-1 gap-4 sm:grid-cols-4">
          <div class="bg-white shadow rounded-lg p-4">
            <dt class="text-sm font-medium text-gray-500">Pending</dt>
            <dd class="mt-1 text-xl font-semibold text-yellow-600">{@replication_stats.pending}</dd>
          </div>
          <div class="bg-white shadow rounded-lg p-4">
            <dt class="text-sm font-medium text-gray-500">Running</dt>
            <dd class="mt-1 text-xl font-semibold text-blue-600">{@replication_stats.running}</dd>
          </div>
          <div class="bg-white shadow rounded-lg p-4">
            <dt class="text-sm font-medium text-gray-500">Completed</dt>
            <dd class="mt-1 text-xl font-semibold text-green-600">{@replication_stats.completed}</dd>
          </div>
          <div class="bg-white shadow rounded-lg p-4">
            <dt class="text-sm font-medium text-gray-500">Dead Letter</dt>
            <dd class="mt-1 text-xl font-semibold text-red-600">{@replication_stats.dead_letter}</dd>
          </div>
        </div>
      </div>

      <div class="mt-8">
        <h2 class="text-lg font-semibold text-gray-900 mb-4">System Information</h2>
        <div class="bg-white shadow rounded-lg overflow-hidden">
          <table class="min-w-full divide-y divide-gray-200">
            <tbody class="divide-y divide-gray-200">
              <tr>
                <td class="px-6 py-3 text-sm font-medium text-gray-500 w-48">Elixir Version</td>
                <td class="px-6 py-3 text-sm text-gray-900">{@elixir_version}</td>
              </tr>
              <tr>
                <td class="px-6 py-3 text-sm font-medium text-gray-500">OTP Version</td>
                <td class="px-6 py-3 text-sm text-gray-900">{@otp_version}</td>
              </tr>
              <tr>
                <td class="px-6 py-3 text-sm font-medium text-gray-500">Node Name</td>
                <td class="px-6 py-3 text-sm text-gray-900 font-mono">{@node_name}</td>
              </tr>
              <tr>
                <td class="px-6 py-3 text-sm font-medium text-gray-500">Uptime</td>
                <td class="px-6 py-3 text-sm text-gray-900">{@uptime}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
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
