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
  def put_rules(bucket, rules) when is_list(rules) do
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

    Concord.put("lifecycle:#{bucket}", validated)
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
  @spec delete_rules(String.t()) :: :ok
  def delete_rules(bucket) do
    Concord.delete("lifecycle:#{bucket}")
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
  @spec evaluate_bucket(String.t()) :: {:ok, non_neg_integer()}
  def evaluate_bucket(bucket) do
    case get_rules(bucket) do
      {:ok, rules} ->
        deleted_count = do_evaluate_bucket(bucket, rules)
        {:ok, deleted_count}

      {:error, :not_found} ->
        {:ok, 0}
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
    case Concord.get_all() do
      {:ok, all} ->
        buckets =
          all
          |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "lifecycle:") end)
          |> Enum.map(fn {k, v} ->
            bucket = String.replace_prefix(k, "lifecycle:", "")
            {bucket, v}
          end)

        total =
          Enum.reduce(buckets, 0, fn {bucket, rules}, acc ->
            acc + do_evaluate_bucket(bucket, rules)
          end)

        Logger.info("Lifecycle evaluation complete: #{total} objects changed")
        {:ok, total}

      {:error, reason} ->
        Logger.error("Failed to evaluate lifecycle rules: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp do_evaluate_bucket(bucket, rules) do
    enabled_rules = Enum.filter(rules, &(&1.status == "Enabled" || &1[:status] == "Enabled"))

    if enabled_rules == [] do
      0
    else
      case Metadata.list_objects(bucket) do
        {:ok, %{keys: objects}} ->
          now = DateTime.utc_now()

          Enum.reduce(objects, 0, fn {key, meta}, acc ->
            acc + apply_lifecycle_action(bucket, key, meta, enabled_rules, now)
          end)

        _ ->
          0
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

  defp apply_lifecycle_action(bucket, key, meta, rules, now) do
    cond do
      should_expire?(key, meta, rules, now) ->
        Metadata.delete_object_meta(bucket, key)

        # NOTE: We deliberately do NOT call Engine.delete_content here.
        # Content storage is content-addressed: multiple object keys may
        # share the same content hash. Directly deleting the file would
        # corrupt any other key that references the same hash.
        # ContentGC will remove the file in its next pass once no
        # metadata references remain.
        1

      should_transition?(key, meta, rules, now) ->
        transition_to_pack(meta)

      true ->
        0
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
