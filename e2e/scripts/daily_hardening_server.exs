# Keep the daily fixture private without changing the production listener.
Application.put_env(:ex_storage_service_s3, :enabled, false)
{:ok, _} = Application.ensure_all_started(:ex_storage_service_s3)

[{Gaoxy.HTTP, listener_opts}] =
  ExStorageServiceS3.Application.children(
    enabled: true,
    node_role: :data,
    port: System.fetch_env!("ESS_S3_PORT") |> String.to_integer()
  )

{:ok, _} =
  Supervisor.start_child(
    ExStorageServiceS3.Supervisor,
    {Gaoxy.HTTP, Keyword.put(listener_opts, :ip, {127, 0, 0, 1})}
  )

Code.require_file("seed_hardening.exs", __DIR__)
