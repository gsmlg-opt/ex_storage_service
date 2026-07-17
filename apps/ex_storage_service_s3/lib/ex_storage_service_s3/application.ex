defmodule ExStorageServiceS3.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    opts = [strategy: :one_for_one, name: ExStorageServiceS3.Supervisor]
    Supervisor.start_link(children(), opts)
  end

  @doc false
  def children(opts \\ []) do
    enabled? =
      Keyword.get_lazy(opts, :enabled, fn ->
        Application.get_env(:ex_storage_service_s3, :enabled, true)
      end)

    if enabled? do
      s3_port =
        Keyword.get_lazy(opts, :port, fn ->
          Application.get_env(:ex_storage_service, :s3_port, 9000)
        end)

      [{Bandit, plug: ExStorageServiceS3.Router, port: s3_port, scheme: :http}]
    else
      []
    end
  end
end
