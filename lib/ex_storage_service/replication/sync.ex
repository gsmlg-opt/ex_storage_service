defmodule ExStorageService.Replication.Sync do
  @moduledoc """
  Anti-entropy sync GenServer that periodically compares local objects with
  remote replicas and reconciles differences.

  Configurable via application env:
    - `:sync_interval` - milliseconds between sync runs (default: 300_000 = 5 minutes)
    - `:delete_orphans` - whether to delete objects on replica that don't exist locally (default: false)
  """

  use GenServer
  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Replication.Config
  alias ExStorageService.Replication.Config.Replica
  alias ExStorageService.Replication.JobQueue

  @default_sync_interval 300_000

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Trigger an immediate sync cycle.
  """
  @spec sync_now() :: :ok
  def sync_now do
    GenServer.cast(__MODULE__, :sync_now)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    sync_interval =
      Keyword.get(
        opts,
        :sync_interval,
        Application.get_env(:ex_storage_service, :sync_interval, @default_sync_interval)
      )

    delete_orphans =
      Keyword.get(
        opts,
        :delete_orphans,
        Application.get_env(:ex_storage_service, :delete_orphans, false)
      )

    state = %{
      sync_interval: sync_interval,
      delete_orphans: delete_orphans
    }

    schedule_sync(sync_interval)
    {:ok, state}
  end

  @impl true
  def handle_cast(:sync_now, state) do
    run_sync(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:sync, state) do
    run_sync(state)
    schedule_sync(state.sync_interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Private

  defp schedule_sync(interval) do
    Process.send_after(self(), :sync, interval)
  end

  defp run_sync(state) do
    Logger.info("Starting replication sync cycle")

    case Metadata.list_buckets() do
      {:ok, buckets} ->
        Enum.each(buckets, fn bucket_meta ->
          bucket_name = bucket_meta[:name] || bucket_meta["name"]

          if bucket_name do
            sync_bucket(bucket_name, state)
          end
        end)

      {:error, reason} ->
        Logger.error("Failed to list buckets for sync: #{inspect(reason)}")
    end

    Logger.info("Replication sync cycle complete")
  end

  defp sync_bucket(bucket, state) do
    case Config.get_bucket_replicas(bucket) do
      {:ok, []} ->
        :ok

      {:ok, replicas} ->
        local_objects = list_local_objects(bucket)

        Enum.each(replicas, fn replica ->
          sync_with_replica(bucket, local_objects, replica, state)
        end)

      {:error, reason} ->
        Logger.error("Failed to get replicas for bucket #{bucket}: #{inspect(reason)}")
    end
  end

  defp sync_with_replica(bucket, local_objects, %Replica{} = replica, state) do
    remote_objects = list_remote_objects(bucket, replica)

    local_map = Map.new(local_objects, fn {key, etag} -> {key, etag} end)
    remote_map = Map.new(remote_objects, fn {key, etag} -> {key, etag} end)

    # Find objects that need to be replicated (missing or changed)
    missing_or_changed =
      Enum.filter(local_map, fn {key, etag} ->
        case Map.get(remote_map, key) do
          nil -> true
          ^etag -> false
          _different -> true
        end
      end)

    # Enqueue replication jobs for missing/changed objects
    Enum.each(missing_or_changed, fn {key, _etag} ->
      JobQueue.enqueue(
        queue: :sync,
        payload: %{
          action: :put,
          bucket: bucket,
          key: key,
          replica: %{
            endpoint: replica.endpoint,
            access_key: replica.access_key,
            secret_key_enc: replica.secret_key_enc,
            bucket: replica.bucket
          }
        }
      )
    end)

    # Optionally delete orphans on remote
    if state.delete_orphans do
      orphans =
        remote_map
        |> Map.keys()
        |> Enum.reject(&Map.has_key?(local_map, &1))

      Enum.each(orphans, fn key ->
        JobQueue.enqueue(
          queue: :sync,
          payload: %{
            action: :delete,
            bucket: bucket,
            key: key,
            replica: %{
              endpoint: replica.endpoint,
              access_key: replica.access_key,
              secret_key_enc: replica.secret_key_enc,
              bucket: replica.bucket
            }
          }
        )
      end)
    end

    count = length(missing_or_changed)

    if count > 0 do
      Logger.info(
        "Sync: enqueued #{count} replication jobs for #{bucket} -> #{replica.endpoint}"
      )
    end
  end

  defp list_local_objects(bucket) do
    case Metadata.list_objects(bucket) do
      {:ok, %{keys: keys}} ->
        Enum.map(keys, fn {key, meta} ->
          etag = meta[:etag] || meta["etag"] || ""
          {key, etag}
        end)

      _ ->
        []
    end
  end

  defp list_remote_objects(bucket, %Replica{} = replica) do
    remote_bucket = replica.bucket || bucket
    url = "#{String.trim_trailing(replica.endpoint, "/")}/#{remote_bucket}?list-type=2"
    headers = auth_headers(replica)

    case Req.get(url, headers: headers) do
      {:ok, %{status: 200, body: body}} ->
        parse_list_response(body)

      {:ok, %{status: status}} ->
        Logger.warning(
          "Failed to list remote objects for #{remote_bucket} on #{replica.endpoint}: HTTP #{status}"
        )

        []

      {:error, reason} ->
        Logger.warning(
          "Failed to list remote objects for #{remote_bucket} on #{replica.endpoint}: #{inspect(reason)}"
        )

        []
    end
  end

  defp parse_list_response(body) when is_binary(body) do
    try do
      {doc, _} = :xmerl_scan.string(String.to_charlist(body), quiet: true)
      extract_contents(doc)
    rescue
      _ ->
        Logger.warning("Failed to parse ListBucketResult XML response")
        []
    catch
      :exit, _ ->
        Logger.warning("Failed to parse ListBucketResult XML response")
        []
    end
  end

  defp parse_list_response(_), do: []

  defp extract_contents(doc) do
    contents = :xmerl_xpath.string(~c"//Contents", doc)

    Enum.map(contents, fn content_elem ->
      key = xpath_text(content_elem, ~c"Key")
      etag = xpath_text(content_elem, ~c"ETag") |> String.trim("\"")
      {key, etag}
    end)
    |> Enum.reject(fn {key, _} -> key == "" end)
  end

  defp xpath_text(parent, tag) do
    case :xmerl_xpath.string(~c"./#{tag}/text()", parent) do
      [text_node | _] ->
        case text_node do
          {:xmlText, _, _, _, value, _} -> List.to_string(value)
          _ -> ""
        end

      [] ->
        ""
    end
  end

  defp auth_headers(%Replica{access_key: access_key})
       when is_binary(access_key) and access_key != "" do
    [{"authorization", "Bearer #{access_key}"}]
  end

  defp auth_headers(_), do: []
end
