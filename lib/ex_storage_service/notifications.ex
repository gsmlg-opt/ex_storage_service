defmodule ExStorageService.Notifications do
  @moduledoc """
  Webhook-based bucket event notifications.

  On object create/delete events, POSTs event JSON to configured webhook endpoints.
  Notifications are delivered asynchronously via Task.Supervisor.

  Configuration stored in Concord: "notification:{bucket}"

  Notification config format:
    %{
      id: "notification-id",
      events: ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
      endpoint: "https://example.com/webhook",
      enabled: true
    }
  """

  require Logger

  @doc """
  Configure notifications for a bucket.
  """
  @spec put_config(String.t(), [map()]) :: :ok | {:error, term()}
  def put_config(bucket, configs) when is_list(configs) do
    validated =
      Enum.map(configs, fn config ->
        %{
          id: Map.get(config, :id, Map.get(config, "id", generate_id())),
          events: Map.get(config, :events, Map.get(config, "events", [])),
          endpoint: Map.get(config, :endpoint, Map.get(config, "endpoint", "")),
          enabled: Map.get(config, :enabled, Map.get(config, "enabled", true))
        }
      end)

    Concord.put("notification:#{bucket}", validated)
  end

  @doc """
  Get notification configuration for a bucket.
  """
  @spec get_config(String.t()) :: {:ok, [map()]} | {:error, :not_found}
  def get_config(bucket) do
    case Concord.get("notification:#{bucket}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, configs} -> {:ok, configs}
      error -> error
    end
  end

  @doc """
  Delete notification configuration for a bucket.
  """
  @spec delete_config(String.t()) :: :ok
  def delete_config(bucket) do
    Concord.delete("notification:#{bucket}")
  end

  @doc """
  Notify about an object event. Sends webhooks asynchronously.

  Event types:
    - "s3:ObjectCreated:Put"
    - "s3:ObjectCreated:Copy"
    - "s3:ObjectCreated:CompleteMultipartUpload"
    - "s3:ObjectRemoved:Delete"
    - "s3:ObjectRemoved:DeleteMarkerCreated"
  """
  @spec notify(String.t(), String.t(), String.t(), map()) :: :ok
  def notify(bucket, key, event_type, extra \\ %{}) do
    case get_config(bucket) do
      {:ok, configs} ->
        matching = matching_configs(configs, event_type)

        Enum.each(matching, fn config ->
          event = build_event(bucket, key, event_type, extra)
          deliver_async(config.endpoint, event)
        end)

        :ok

      {:error, :not_found} ->
        :ok
    end
  end

  @doc """
  Build an S3-style event notification payload.
  """
  @spec build_event(String.t(), String.t(), String.t(), map()) :: map()
  def build_event(bucket, key, event_type, extra \\ %{}) do
    %{
      "Records" => [
        %{
          "eventVersion" => "2.1",
          "eventSource" => "ex-storage-service",
          "eventTime" => DateTime.utc_now() |> DateTime.to_iso8601(),
          "eventName" => event_type,
          "s3" => %{
            "bucket" => %{
              "name" => bucket
            },
            "object" =>
              Map.merge(
                %{"key" => key},
                extra
              )
          }
        }
      ]
    }
  end

  ## Private

  defp matching_configs(configs, event_type) do
    Enum.filter(configs, fn config ->
      config.enabled != false && event_matches?(config.events, event_type)
    end)
  end

  @doc false
  def event_matches?(configured_events, event_type) do
    Enum.any?(configured_events, fn pattern ->
      cond do
        pattern == event_type ->
          true

        String.ends_with?(pattern, ":*") ->
          prefix = String.replace_trailing(pattern, ":*", ":")
          String.starts_with?(event_type, prefix) ||
            String.replace_trailing(pattern, "*", "") == String.replace_trailing(event_type, String.split(event_type, ":") |> List.last(), "")

        pattern == "s3:ObjectCreated:*" ->
          String.starts_with?(event_type, "s3:ObjectCreated:")

        pattern == "s3:ObjectRemoved:*" ->
          String.starts_with?(event_type, "s3:ObjectRemoved:")

        true ->
          false
      end
    end)
  end

  defp deliver_async(endpoint, event) do
    Task.Supervisor.start_child(
      ExStorageService.NotificationTaskSupervisor,
      fn -> deliver_webhook(endpoint, event) end
    )
  rescue
    # If task supervisor isn't running, try spawning directly
    _ ->
      Task.start(fn -> deliver_webhook(endpoint, event) end)
  end

  @max_retries 3
  @initial_backoff_ms 500

  defp deliver_webhook(endpoint, event) do
    deliver_webhook(endpoint, event, 0)
  end

  defp deliver_webhook(endpoint, event, attempt) do
    body = Jason.encode!(event)

    case Req.post(endpoint, body: body, headers: [{"content-type", "application/json"}]) do
      {:ok, %{status: status}} when status in 200..299 ->
        Logger.debug("Notification delivered to #{endpoint}: #{status}")
        :ok

      {:ok, %{status: status}} when status >= 500 and attempt < @max_retries ->
        backoff = @initial_backoff_ms * Integer.pow(2, attempt)
        Logger.warning("Notification to #{endpoint} failed (HTTP #{status}), retrying in #{backoff}ms (attempt #{attempt + 1}/#{@max_retries})")
        Process.sleep(backoff)
        deliver_webhook(endpoint, event, attempt + 1)

      {:ok, %{status: status}} ->
        Logger.warning("Notification delivery failed to #{endpoint}: HTTP #{status} after #{attempt + 1} attempt(s)")
        {:error, {:http_error, status}}

      {:error, reason} when attempt < @max_retries ->
        backoff = @initial_backoff_ms * Integer.pow(2, attempt)
        Logger.warning("Notification to #{endpoint} failed (#{inspect(reason)}), retrying in #{backoff}ms (attempt #{attempt + 1}/#{@max_retries})")
        Process.sleep(backoff)
        deliver_webhook(endpoint, event, attempt + 1)

      {:error, reason} ->
        Logger.warning("Notification delivery failed to #{endpoint}: #{inspect(reason)} after #{attempt + 1} attempt(s)")
        {:error, reason}
    end
  rescue
    e ->
      Logger.warning("Notification delivery error to #{endpoint}: #{inspect(e)}")
      {:error, e}
  end

  defp generate_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
end
