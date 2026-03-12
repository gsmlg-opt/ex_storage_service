defmodule ExStorageServiceWeb.Plugs.RequireAdmin do
  @moduledoc """
  Plug that requires an authenticated admin session.

  Checks for `:admin_authenticated` in the session. If not present,
  redirects to the login page.
  """

  import Plug.Conn
  import Phoenix.Controller, only: [redirect: 2]

  def init(opts), do: opts

  def call(conn, _opts) do
    if get_session(conn, :admin_authenticated) do
      assign(conn, :admin_user, get_session(conn, :admin_user))
    else
      conn
      |> redirect(to: "/login")
      |> halt()
    end
  end
end
