defmodule ExStorageServiceWeb.PageController do
  use ExStorageServiceWeb, :controller

  def home(conn, _params) do
    redirect(conn, to: ~p"/dashboard")
  end
end
