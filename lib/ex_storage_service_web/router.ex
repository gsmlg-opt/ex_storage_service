defmodule ExStorageServiceWeb.Router do
  use ExStorageServiceWeb, :router

  import Phoenix.LiveDashboard.Router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {ExStorageServiceWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :require_admin do
    plug ExStorageServiceWeb.Plugs.RequireAdmin
  end

  # Public routes (login/logout)
  scope "/", ExStorageServiceWeb do
    pipe_through :browser

    get "/", PageController, :home
    get "/login", SessionController, :new
    post "/login", SessionController, :create
    delete "/logout", SessionController, :delete
  end

  # Protected admin routes
  scope "/", ExStorageServiceWeb do
    pipe_through [:browser, :require_admin]

    live_session :admin, on_mount: {ExStorageServiceWeb.Live.AdminAuth, :default} do
      live "/dashboard", DashboardLive
      live "/buckets", BucketLive.Index
      live "/buckets/:name", BucketLive.Show

      live "/users", UserLive.Index
      live "/users/:id", UserLive.Show
      live "/policies", PolicyLive.Index
      live "/policies/:id", PolicyLive.Show
      live "/audit", AuditLive.Index
    end
  end

  # Prometheus metrics endpoint (no auth pipeline, plain text)
  scope "/metrics" do
    get "/", ExStorageServiceWeb.MetricsController, :index
  end

  # Enable LiveDashboard in dev
  if Application.compile_env(:ex_storage_service, :dev_routes) do
    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: ExStorageServiceWeb.Telemetry
    end
  end
end
