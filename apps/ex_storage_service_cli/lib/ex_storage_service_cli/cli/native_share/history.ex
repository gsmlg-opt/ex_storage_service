defmodule ExStorageServiceCli.CLI.NativeShare.History do
  @moduledoc false
  import Bitwise, only: [band: 2]
  alias ExStorageServiceCli.{Config, Error}
  alias ExStorageServiceCli.Config.Store
  @max_entries 1000

  def record(action, entries, opts) do
    path = path(opts)
    now = now(opts)

    with :ok <- private_file(path) do
      Store.update(path, fn data ->
        with {:ok, existing} <- records(data),
             {:ok, additions} <- additions(action, entries, now) do
          active = Map.filter(existing, fn {_, entry} -> entry["expires_at"] > now end)
          combined = Map.merge(active, additions)

          if map_size(combined) <= @max_entries,
            do: {:ok, %{"version" => 1, "entries" => combined}},
            else: invalid("Share history exceeds its 1000-entry limit", :share_history_limit)
        end
      end)
    end
  end

  def list(action, opts \\ []) do
    limit = opts[:limit] || 100
    cursor = opts[:cursor] || 0

    with :ok <- pagination(limit, cursor),
         :ok <- private_file(path(opts)),
         {:ok, data} <- Store.read(path(opts)),
         {:ok, records} <- records(data) do
      active =
        records
        |> Map.values()
        |> Enum.filter(&(&1["action"] == action and &1["expires_at"] > now(opts)))
        |> Enum.sort_by(&{&1["created_at"], &1["id"]}, :desc)

      page = active |> Enum.drop(cursor) |> Enum.take(limit)

      shares =
        Enum.map(page, fn entry ->
          visible = Map.take(entry, ~w(id action bucket key version_id created_at expires_at))

          if opts[:reveal],
            do: Map.put(visible, "share", JSON.decode!(entry["payload"])),
            else: visible
        end)

      result = %{"Shares" => shares}

      {:ok,
       if(length(active) > cursor + limit,
         do: Map.put(result, "NextCursor", cursor + limit),
         else: result
       )}
    end
  end

  defp additions(action, entries, now) do
    Enum.reduce_while(entries, {:ok, %{}}, fn {share, target, version}, {:ok, result} ->
      lifetime = share["effective_expires"] || share["expires"]

      if is_integer(lifetime) and lifetime > 0 do
        payload = JSON.encode!(share)
        id = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)

        entry = %{
          "id" => id,
          "action" => action,
          "bucket" => target.bucket,
          "key" => target.key,
          "created_at" => now,
          "expires_at" => now + lifetime,
          "payload" => payload
        }

        entry = if version, do: Map.put(entry, "version_id", version), else: entry
        {:cont, {:ok, Map.put(result, id, entry)}}
      else
        {:halt, invalid("Cannot record a share without a valid effective lifetime")}
      end
    end)
  end

  defp records(data) when data == %{}, do: {:ok, %{}}

  defp records(%{"version" => 1, "entries" => entries})
       when is_map(entries) and map_size(entries) <= @max_entries do
    if Enum.all?(entries, fn {id, entry} -> valid_record?(id, entry) end),
      do: {:ok, entries},
      else: invalid("Share history is malformed; original file was preserved")
  end

  defp records(_), do: invalid("Share history is malformed; original file was preserved")

  defp valid_record?(id, entry) when is_map(entry) do
    is_binary(id) and entry["id"] == id and entry["action"] in ~w(download upload) and
      is_binary(entry["bucket"]) and is_binary(entry["key"]) and
      is_integer(entry["created_at"]) and is_integer(entry["expires_at"]) and
      entry["expires_at"] > entry["created_at"] and valid_payload?(entry["payload"])
  end

  defp valid_record?(_, _), do: false

  defp valid_payload?(value) when is_binary(value) do
    case JSON.decode(value) do
      {:ok, map} when is_map(map) -> true
      _ -> false
    end
  end

  defp valid_payload?(_), do: false

  defp private_file(path) do
    case File.lstat(path) do
      {:error, :enoent} ->
        :ok

      {:ok, %{type: :regular, mode: mode}} when band(mode, 0o077) == 0 ->
        case File.stat(Path.dirname(path)) do
          {:ok, %{type: :directory, mode: parent}} when band(parent, 0o077) == 0 -> :ok
          _ -> invalid("Share history directory must be private", :unsafe_share_history)
        end

      _ ->
        invalid("Share history must be a private regular file", :unsafe_share_history)
    end
  end

  defp pagination(limit, cursor)
       when is_integer(limit) and limit in 1..1000 and is_integer(cursor) and cursor >= 0,
       do: :ok

  defp pagination(_, _), do: invalid("Share list requires limit 1..1000 and a nonnegative cursor")
  defp path(opts), do: Path.join(opts[:config_dir] || Config.config_dir(), "shares.toml")
  defp now(opts), do: DateTime.to_unix(opts[:now] || DateTime.utc_now())

  defp invalid(message, code \\ :invalid_share_history),
    do: {:error, Error.new(:config, message, code: code)}
end
