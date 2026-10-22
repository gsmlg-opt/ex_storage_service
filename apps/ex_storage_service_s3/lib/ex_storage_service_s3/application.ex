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

    node_role =
      Keyword.get_lazy(opts, :node_role, fn ->
        Application.get_env(:ex_storage_service, :node_role, :data)
      end)

    if enabled? and node_role == :data do
      s3_port =
        Keyword.get_lazy(opts, :port, fn ->
          Application.get_env(:ex_storage_service, :s3_port, 9000)
        end)

      [
        {
          Gaoxy.HTTP,
          # Two 1 MiB Plug windows per admitted transfer plus native buffers.
          plug: {ExStorageServiceS3.Router, []},
          ip: {0, 0, 0, 0},
          port: s3_port,
          protocols: [:http2, :http1],
          h2c: :prior_knowledge,
          limits: %{max_active: 64, max_bytes: 256 * 1024 * 1024},
          http_limits: %{
            max_body_bytes: ExStorageServiceS3.AwsChunked.max_wire_size(max_decoded_body_size()),
            request_timeout: :infinity,
            header_timeout: 5_000,
            read_timeout: 60_000,
            write_timeout: 60_000
          }
        }
      ]
    else
      []
    end
  end

  defp max_decoded_body_size do
    Enum.max([
      Application.get_env(:ex_storage_service, :max_object_size, 5 * 1024 * 1024 * 1024),
      Application.get_env(:ex_storage_service, :max_part_size, 5 * 1024 * 1024 * 1024),
      # Control readers retain their independent 1 MiB allowance.
      1_048_576
    ])
  end
end
