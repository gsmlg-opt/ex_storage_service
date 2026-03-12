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

  scope "/", ExStorageServiceWeb do
    pipe_through :browser

    get "/", PageController, :home

    live "/dashboard", DashboardLive
    live "/buckets", BucketLive.Index
    live "/buckets/:name", BucketLive.Show
  end

  # Enable LiveDashboard in dev
  if Application.compile_env(:ex_storage_service, :dev_routes) do
    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: ExStorageServiceWeb.Telemetry
    end
  end
end
