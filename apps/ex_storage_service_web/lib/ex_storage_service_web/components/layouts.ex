defmodule ExStorageServiceWeb.Layouts do
  @moduledoc """
  This module holds different layouts used by your application.

  The layouts are defined in the templates directory:
    lib/ex_storage_service_web/components/layouts/root.html.heex
    lib/ex_storage_service_web/components/layouts/app.html.heex
  """
  use ExStorageServiceWeb, :html

  embed_templates "layouts/*"

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
