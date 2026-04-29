defmodule ExStorageServiceWeb.Live.AdminAuth do
  @moduledoc """
  LiveView on_mount hook that verifies admin authentication via session.

  Used to protect LiveView routes that require admin access.
  """

  import Phoenix.LiveView
  import Phoenix.Component

  @default_theme "sunshine"
  @supported_themes ~w(sunshine moonlight)

  def on_mount(:default, _params, session, socket) do
    if session["admin_authenticated"] do
      socket =
        socket
        |> assign(:admin_user, session["admin_user"])
        |> assign_new(:theme, fn -> @default_theme end)
        |> attach_hook(:admin_theme_switcher, :handle_event, &handle_theme_event/3)

      {:cont, socket}
    else
      {:halt, redirect(socket, to: "/login")}
    end
  end

  defp handle_theme_event("theme_changed", %{"theme" => theme}, socket) do
    {:halt, assign(socket, :theme, normalize_theme(theme))}
  end

  defp handle_theme_event("theme_changed", _params, socket) do
    {:halt, assign(socket, :theme, @default_theme)}
  end

  defp handle_theme_event(_event, _params, socket) do
    {:cont, socket}
  end

  defp normalize_theme(theme) when theme in @supported_themes, do: theme
  defp normalize_theme(_theme), do: @default_theme
end
