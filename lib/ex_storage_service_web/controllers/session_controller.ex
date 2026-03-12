defmodule ExStorageServiceWeb.SessionController do
  use ExStorageServiceWeb, :controller

  def new(conn, _params) do
    render(conn, :login, error: nil)
  end

  def create(conn, %{"username" => username, "password" => password}) do
    admin_user = Application.get_env(:ex_storage_service, :root_admin_user, "admin")

    admin_password_hash =
      Application.get_env(
        :ex_storage_service,
        :root_admin_password_hash,
        Base.encode16(:crypto.hash(:sha256, "admin"), case: :lower)
      )

    input_hash = Base.encode16(:crypto.hash(:sha256, password), case: :lower)

    if Plug.Crypto.secure_compare(username, admin_user) and
         Plug.Crypto.secure_compare(input_hash, admin_password_hash) do
      conn
      |> configure_session(renew: true)
      |> put_session(:admin_authenticated, true)
      |> put_session(:admin_user, username)
      |> redirect(to: ~p"/dashboard")
    else
      render(conn, :login, error: "Invalid username or password")
    end
  end

  def delete(conn, _params) do
    conn
    |> configure_session(drop: true)
    |> redirect(to: "/login")
  end
end
