# Keep the daily fixture private without changing the production listener.
Application.put_env(:ex_storage_service_s3, :enabled, false)
{:ok, _} = Application.ensure_all_started(:ex_storage_service_s3)

{:ok, _} =
  Supervisor.start_child(
    ExStorageServiceS3.Supervisor,
    {Bandit,
     plug: ExStorageServiceS3.Router,
     ip: {127, 0, 0, 1},
     port: System.fetch_env!("ESS_S3_PORT") |> String.to_integer()}
  )

Code.require_file("seed_hardening.exs", __DIR__)
