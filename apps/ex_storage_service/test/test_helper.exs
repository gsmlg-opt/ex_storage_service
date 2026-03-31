# Clean up stale Ra/Concord data from previous test runs.
# Ra returns :not_new when attempting to start a cluster with existing state,
# so we need a fresh data directory for each test suite run.
data_root = Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")
ra_dir = Application.get_env(:ra, :data_dir, ~c"/tmp/ex_storage_service/ra") |> to_string()
concord_dir = Application.get_env(:concord, :data_dir, "/tmp/ex_storage_service/concord")

for dir <- [ra_dir, concord_dir, Path.join(data_root, "ra")] do
  if File.exists?(dir) do
    File.rm_rf!(dir)
  end
end

ExUnit.start()
