defmodule ExStorageServiceWeb.Live.AdminAuth do
  @moduledoc """
  LiveView on_mount hook that verifies admin authentication via session.

  Used to protect LiveView routes that require admin access.
  """

  import Phoenix.LiveView
  import Phoenix.Component

  def on_mount(:default, _params, session, socket) do
    if session["admin_authenticated"] do
      {:cont, assign(socket, :admin_user, session["admin_user"])}
    else
      {:halt, redirect(socket, to: "/login")}
    end
  end
end
