defmodule ExStorageService.IAM.Audit do
  @moduledoc """
  IAM Audit logging backed by Concord key-value store.

  Audit events are stored with the key pattern: "audit:{timestamp}:{event_id}"
  """

  @type action ::
          :create_user
          | :suspend_user
          | :activate_user
          | :delete_user
          | :create_key
          | :delete_key
          | :create_policy
          | :delete_policy
          | :attach_policy
          | :detach_policy

  @type event :: %{
          id: String.t(),
          actor: String.t(),
          action: action(),
          target: String.t(),
          details: map(),
          timestamp: String.t()
        }

  @doc """
  Logs an audit event.
  """
  @spec log_event(String.t(), action(), String.t(), map()) :: {:ok, event()} | {:error, term()}
  def log_event(actor, action, target, details \\ %{}) do
    event_id = generate_event_id()
    now = DateTime.utc_now()
    timestamp = DateTime.to_iso8601(now)
    # Use Unix timestamp for sortable key
    ts_key = now |> DateTime.to_unix(:microsecond) |> Integer.to_string()

    event = %{
      id: event_id,
      actor: actor,
      action: action,
      target: target,
      details: details,
      timestamp: timestamp
    }

    key = "audit:#{ts_key}:#{event_id}"

    case Concord.put(key, event) do
      :ok -> {:ok, event}
      error -> error
    end
  end

  @doc """
  Lists audit events with optional filtering.

  Options:
    - `:actor` - filter by actor
    - `:action` - filter by action
    - `:target` - filter by target
    - `:limit` - max number of events to return (default 100)
  """
  @spec list_events(keyword()) :: {:ok, [event()]} | {:error, term()}
  def list_events(opts \\ []) do
    actor_filter = Keyword.get(opts, :actor)
    action_filter = Keyword.get(opts, :action)
    target_filter = Keyword.get(opts, :target)
    limit = Keyword.get(opts, :limit, 100)

    case Concord.get_all() do
      {:ok, all} ->
        events =
          all
          |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "audit:") end)
          |> Enum.map(fn {_k, v} -> v end)
          |> maybe_filter(:actor, actor_filter)
          |> maybe_filter(:action, action_filter)
          |> maybe_filter(:target, target_filter)
          |> Enum.sort_by(fn e -> e.timestamp end, :desc)
          |> Enum.take(limit)

        {:ok, events}

      error ->
        error
    end
  end

  # Private helpers

  defp maybe_filter(events, _field, nil), do: events

  defp maybe_filter(events, field, value) do
    Enum.filter(events, fn e -> Map.get(e, field) == value end)
  end

  defp generate_event_id do
    :crypto.strong_rand_bytes(8)
    |> Base.encode16(case: :lower)
  end
end
