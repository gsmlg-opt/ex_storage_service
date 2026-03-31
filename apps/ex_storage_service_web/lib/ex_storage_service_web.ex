defmodule ExStorageServiceWeb do
  @moduledoc """
  The entrypoint for defining your web interface, such
  as controllers, components, channels, and so on.
  """

  def static_paths, do: ~w(assets fonts images favicon.ico robots.txt)

  def router do
    quote do
      use Phoenix.Router, helpers: false

      import Plug.Conn
      import Phoenix.Controller
      import Phoenix.LiveView.Router
    end
  end

  def channel do
    quote do
      use Phoenix.Channel
    end
  end

  def controller do
    quote do
      use Phoenix.Controller,
        formats: [:html, :json]

      import Plug.Conn

      unquote(verified_routes())
    end
  end

  def live_view do
    quote do
      use Phoenix.LiveView,
        layout: {ExStorageServiceWeb.Layouts, :app}

      unquote(html_helpers())
    end
  end

  def live_component do
    quote do
      use Phoenix.LiveComponent

      unquote(html_helpers())
    end
  end

  def html do
    quote do
      use Phoenix.Component

      import Phoenix.HTML

      unquote(html_helpers())
    end
  end

  defp html_helpers do
    quote do
      use PhoenixDuskmoon.Component
      import Phoenix.HTML.Form
      import ExStorageServiceWeb.AppComponents

      unquote(verified_routes())
    end
  end

  def verified_routes do
    quote do
      use Phoenix.VerifiedRoutes,
        endpoint: ExStorageServiceWeb.Endpoint,
        router: ExStorageServiceWeb.Router,
        statics: ExStorageServiceWeb.static_paths()
    end
  end

  @doc """
  When used, dispatch to the appropriate controller/live_view/etc.
  """
  defmacro __using__(which) when is_atom(which) do
    apply(__MODULE__, which, [])
  end
end
