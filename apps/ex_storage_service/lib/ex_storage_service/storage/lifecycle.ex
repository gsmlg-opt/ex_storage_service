defmodule ExStorageService.Storage.Lifecycle do
  @moduledoc """
  GenServer that periodically evaluates lifecycle rules for buckets.

  Lifecycle rules support object expiration and internal transition to
  packed storage for cold objects.

  Rules are stored in Concord under "lifecycle:{bucket}" as a list of rule maps.

  Rule format:
    %{
      id: "rule-id",
      prefix: "logs/",          # optional prefix filter
      status: "Enabled",        # "Enabled" or "Disabled"
      expiration_days: 30,      # delete objects older than N days
      transition_days: 7,       # pack objects older than N days
      transition_storage_class: "PACKED"
    }
  """

  use GenServer

  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.{BucketLifecycle, ObjectCommit}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Storage.Pack

  @default_interval :timer.hours(1)

  ## Client API

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Set lifecycle rules for a bucket.
  """
  @spec put_rules(String.t(), [map()]) :: :ok | {:error, term()}
  def put_rules(bucket, rules, opts \\ []) when is_list(rules) do
    validated =
      Enum.map(rules, fn rule ->
        %{
          id: Map.get(rule, :id, Map.get(rule, "id", generate_rule_id())),
          prefix: Map.get(rule, :prefix, Map.get(rule, "prefix", "")),
          status: Map.get(rule, :status, Map.get(rule, "status", "Enabled")),
          expiration_days: Map.get(rule, :expiration_days, Map.get(rule, "expiration_days", 0)),
          transition_days: Map.get(rule, :transition_days, Map.get(rule, "transition_days", 0)),
          transition_storage_class:
            Map.get(
              rule,
              :transition_storage_class,
              Map.get(rule, "transition_storage_class", "PACKED")
            )
        }
      end)

    ExStorageService.Metadata.BucketConfig.put(bucket, "lifecycle", validated, opts)
  end

  @doc """
  Get lifecycle rules for a bucket.
  """
  @spec get_rules(String.t()) :: {:ok, [map()]} | {:error, :not_found}
  def get_rules(bucket) do
    case Concord.get("lifecycle:#{bucket}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, rules} -> {:ok, rules}
      error -> error
    end
  end

  @doc """
  Delete lifecycle rules for a bucket.
  """
  @spec delete_rules(String.t()) :: :ok | {:error, term()}
  def delete_rules(bucket, opts \\ []) do
    ExStorageService.Metadata.BucketConfig.delete(bucket, "lifecycle", opts)
  end

  @doc """
  Evaluate lifecycle rules now (for testing or manual trigger).
  """
  def evaluate_now(server \\ __MODULE__) do
    GenServer.call(server, :evaluate_now, :infinity)
  end

  @doc """
  Evaluate rules for a specific bucket. Can be called directly without GenServer.
  """
  @spec evaluate_bucket(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def evaluate_bucket(bucket) do
    with {:ok, bucket_meta} <- Metadata.get_bucket(bucket),
         {:ok, rules} <- get_rules(bucket) do
      case Enum.filter(rules, &(&1.status == "Enabled")) do
        [] ->
          {:ok, 0}

        enabled ->
          opts = [bucket_generation: BucketLifecycle.generation(bucket_meta)]
          evaluate_pages(bucket, enabled, DateTime.utc_now(), opts, 0)
      end
    else
      {:error, :not_found} -> {:ok, 0}
      error -> error
    end
  end

  ## Server callbacks

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval, @default_interval)

    if interval > 0 do
      Process.send_after(self(), :evaluate, interval)
    end

    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_call(:evaluate_now, _from, state) do
    result = evaluate_all_buckets()
    {:reply, result, state}
  end

  @impl true
  def handle_info(:evaluate, state) do
    evaluate_all_buckets()

    if state.interval > 0 do
      Process.send_after(self(), :evaluate, state.interval)
    end

    {:noreply, state}
  end

  ## Private

  defp evaluate_all_buckets do
    case evaluate_rule_pages(nil, 0) do
      {:ok, total} ->
        Logger.info("Lifecycle evaluation complete: #{total} objects changed")
        {:ok, total}

      {:error, reason} ->
        Logger.error("Failed to evaluate lifecycle rules: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp evaluate_rule_pages(cursor, total) do
    with {:ok, page} <- Backend.list_page("lifecycle:", cursor, 100, consistency: :linearizable) do
      total =
        Enum.reduce(page.entries, total, fn %{key: "lifecycle:" <> bucket}, acc ->
          case evaluate_bucket(bucket) do
            {:ok, count} ->
              acc + count

            {:error, reason} ->
              Logger.warning("Lifecycle evaluation failed for #{bucket}: #{inspect(reason)}")
              acc
          end
        end)

      if page.next_cursor, do: evaluate_rule_pages(page.next_cursor, total), else: {:ok, total}
    end
  end

  defp evaluate_pages(bucket, rules, now, opts, total) do
    with {:ok, page} <- Metadata.list_objects(bucket, opts),
         {:ok, count} <-
           Enum.reduce_while(page.keys, {:ok, total}, fn {key, meta}, {:ok, acc} ->
             case apply_lifecycle_action(bucket, key, meta, rules, now, opts) do
               {:ok, changed} -> {:cont, {:ok, acc + changed}}
               error -> {:halt, error}
             end
           end) do
      if page.is_truncated do
        evaluate_pages(
          bucket,
          rules,
          now,
          Keyword.put(opts, :continuation_token, page.next_continuation_token),
          count
        )
      else
        {:ok, count}
      end
    end
  end

  @doc """
  Check if an object matches any lifecycle expiration rule.
  Exported for testing.
  """
  @spec should_expire?(String.t(), map(), [map()], DateTime.t()) :: boolean()
  def should_expire?(key, meta, rules, now) do
    Enum.any?(rules, fn rule ->
      status = Map.get(rule, :status, "Enabled")
      prefix = Map.get(rule, :prefix, "")
      expiration_days = Map.get(rule, :expiration_days, 0)

      enabled = status == "Enabled"
      prefix_matches = prefix == "" || String.starts_with?(key, prefix)

      enabled && expiration_days > 0 && prefix_matches &&
        object_expired?(meta, expiration_days, now)
    end)
  end

  @doc """
  Check if an object matches any lifecycle transition rule.
  Exported for testing.
  """
  @spec should_transition?(String.t(), map(), [map()], DateTime.t()) :: boolean()
  def should_transition?(key, meta, rules, now) do
    Enum.any?(rules, fn rule ->
      status = Map.get(rule, :status, "Enabled")
      prefix = Map.get(rule, :prefix, "")
      transition_days = Map.get(rule, :transition_days, 0)
      storage_class = Map.get(rule, :transition_storage_class, "PACKED")

      enabled = status == "Enabled"
      prefix_matches = prefix == "" || String.starts_with?(key, prefix)

      enabled && pack_storage_class?(storage_class) && transition_days > 0 && prefix_matches &&
        object_expired?(meta, transition_days, now)
    end)
  end

  defp apply_lifecycle_action(bucket, key, meta, rules, now, opts) do
    cond do
      should_expire?(key, meta, rules, now) ->
        # ContentGC owns shared blob reclamation after metadata expiration.
        case ObjectCommit.delete_current_if(
               bucket,
               key,
               &should_expire?(key, &1, rules, now),
               opts
             ) do
          {:ok, :skipped} -> {:ok, 0}
          {:ok, _commit} -> {:ok, 1}
          error -> error
        end

      should_transition?(key, meta, rules, now) ->
        {:ok, transition_to_pack(meta)}

      true ->
        {:ok, 0}
    end
  end

  defp transition_to_pack(%{content_hash: hash}) when is_binary(hash) do
    case Pack.pack_blobs([hash]) do
      {:ok, %{packed: count}} when count > 0 -> 1
      _ -> 0
    end
  end

  defp transition_to_pack(_meta), do: 0

  defp pack_storage_class?(storage_class) when is_binary(storage_class) do
    String.upcase(storage_class) == "PACKED"
  end

  defp pack_storage_class?(_storage_class), do: false

  defp object_expired?(meta, expiration_days, now) do
    created_at = Map.get(meta, :created_at) || Map.get(meta, :updated_at)

    case parse_datetime(created_at) do
      {:ok, dt} ->
        age_seconds = DateTime.diff(now, dt, :second)
        age_seconds >= expiration_days * 86_400

      _ ->
        false
    end
  end

  defp parse_datetime(nil), do: {:error, nil}

  defp parse_datetime(dt_string) when is_binary(dt_string) do
    case DateTime.from_iso8601(dt_string) do
      {:ok, dt, _} -> {:ok, dt}
      _ -> {:error, :invalid}
    end
  end

  defp parse_datetime(%DateTime{} = dt), do: {:ok, dt}

  defp generate_rule_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
end
