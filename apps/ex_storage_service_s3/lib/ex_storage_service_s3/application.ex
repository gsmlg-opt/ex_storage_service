defmodule ExStorageServiceS3.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    s3_port = Application.get_env(:ex_storage_service, :s3_port, 9000)

    children = [
      {Bandit, plug: ExStorageServiceS3.Router, port: s3_port, scheme: :http}
    ]

    opts = [strategy: :one_for_one, name: ExStorageServiceS3.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
