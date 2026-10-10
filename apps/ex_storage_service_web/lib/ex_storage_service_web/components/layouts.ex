defmodule ExStorageServiceWeb.Layouts do
  @moduledoc """
  This module holds different layouts used by your application.

  The layouts are defined in the templates directory:
    lib/ex_storage_service_web/components/layouts/root.html.heex
    lib/ex_storage_service_web/components/layouts/app.html.heex
  """
  use ExStorageServiceWeb, :html

  @build_info Application.compile_env(:ex_storage_service_web, :build_info)
  @build_time @build_info[:build_time] ||
                DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

  embed_templates "layouts/*"

  def version_info do
    %{
      version: Application.spec(:ex_storage_service_web, :vsn) |> to_string(),
      environment: @build_info[:environment],
      git_ref: @build_info[:git_ref],
      git_sha: @build_info[:git_sha],
      build_time: @build_time
    }
  end

  attr :info, :map, required: true

  def version_badge(assigns) do
    info = assigns.info
    suffix = if info.environment == :dev, do: "-dev", else: ""

    details =
      Enum.join(
        [
          "Version: #{info.version}",
          "Environment: #{info.environment}",
          "Git ref: #{info.git_ref || "Unknown"}",
          "Commit: #{info.git_sha || "Unknown"}",
          "Build time: #{info.build_time}"
        ],
        "\n"
      )

    assigns = assign(assigns, label: "v#{info.version}#{suffix}", details: details)

    ~H"""
    <.dm_tooltip
      :let={trigger_attrs}
      id="app-version"
      content={@details}
      position="bottom"
      color="secondary"
      class="whitespace-pre-line text-left font-mono text-xs"
    >
      <button
        id="app-version-trigger"
        type="button"
        class="inline-flex rounded-full cursor-help focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2"
        aria-label={"Version #{@label} and build details"}
        {trigger_attrs}
      >
        <.dm_badge variant="secondary" size="lg" pill class="whitespace-nowrap">
          {@label}
        </.dm_badge>
      </button>
    </.dm_tooltip>
    """
  end

  def active_nav_id(page_title) when is_binary(page_title) do
    cond do
      page_title == "Dashboard" -> "dashboard"
      String.starts_with?(page_title, "Bucket") -> "buckets"
      String.starts_with?(page_title, "User") -> "users"
      String.starts_with?(page_title, "Polic") -> "policies"
      String.starts_with?(page_title, "Audit") -> "audit"
      true -> ""
    end
  end

  def active_nav_id(_page_title), do: ""
end
