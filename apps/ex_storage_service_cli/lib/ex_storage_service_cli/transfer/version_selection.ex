defmodule ExStorageServiceCli.Transfer.VersionSelection do
  @moduledoc "Bounded historical snapshots, pinned to immutable S3 version identifiers."
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.CLI.Find
  alias ExStorageServiceCli.Transfer.{Coordinator, Inventory, Manifest}

  @doc "Resolves the native bounded UTC/duration grammar to a nanosecond cutoff."
  def cutoff(value, opts \\ []) do
    now = DateTime.to_unix(opts[:now] || DateTime.utc_now(), :nanosecond)

    with true <- is_binary(value) and value != "" and not String.starts_with?(value, "-"),
         {:ok, cutoff} when is_integer(cutoff) <- Find.time_cutoff(value, now) do
      {:ok, cutoff}
    else
      _ ->
        failure(
          :rewind_time,
          "Rewind requires a nonnegative duration or an absolute UTC/RFC3339 timestamp"
        )
    end
  end

  @doc "Builds a complete historical prefix snapshot before any caller performs effects."
  def inventory(context, %Target{kind: :remote} = target, value, opts \\ []) do
    with {:ok, cutoff} <- cutoff(value, opts),
         :ok <- Coordinator.check(context.coordinator),
         {:ok, versions} <- Inventory.versions(context, target, opts) do
      try do
        select(versions, cutoff, Keyword.put(opts, :coordinator, context.coordinator))
      after
        Manifest.cleanup(versions)
      end
    end
  end

  @doc "Selects one existing object version strictly before the requested instant."
  def single(context, target, value, opts \\ []) do
    with {:ok, selected} <- inventory(context, target, value, Keyword.put(opts, :exact, true)) do
      try do
        case Enum.take(Manifest.stream(selected), 2) do
          [entry] -> {:ok, entry}
          [] -> failure(:rewind_not_found, "Object did not exist at the requested instant")
          _ -> failure(:rewind_inventory, "Historical object selection was not unique")
        end
      after
        Manifest.cleanup(selected)
      end
    end
  end

  @doc "Selects from a complete version manifest using one key and candidate in memory."
  def select(%Manifest{} = versions, cutoff, opts \\ []) when is_integer(cutoff) do
    entries =
      versions
      |> Manifest.stream()
      |> Stream.concat([:eof])
      |> Stream.transform({nil, nil}, fn
        _, :failed ->
          {:halt, :failed}

        :eof, {_key, best} ->
          {selected(best, opts), :failed}

        entry, {key, best} ->
          with :ok <- Coordinator.check(opts[:coordinator]),
               :ok <- valid(entry) do
            candidate = choose(if(key == entry["key"], do: best), entry, cutoff)
            emitted = if key != nil and key != entry["key"], do: selected(best, opts), else: []
            {emitted, {entry["key"], candidate}}
          else
            {:error, _} = error -> {[error], :failed}
          end
      end)

    Manifest.build(entries, opts)
  end

  defp choose(best, entry, cutoff) do
    cond do
      entry["modified"] * 1000 >= cutoff ->
        best

      best == nil or entry["modified"] > best["modified"] ->
        entry

      entry["modified"] == best["modified"] ->
        Map.put(best, "_rewind_ambiguous", true)

      true ->
        best
    end
  end

  defp selected(nil, _), do: []

  defp selected(%{"_rewind_ambiguous" => true}, _),
    do: [
      failure(
        :rewind_ambiguous,
        "Provider returned ambiguous versions at the same modification instant"
      )
    ]

  defp selected(%{"delete_marker" => true} = entry, opts),
    do:
      if(opts[:include_delete_markers],
        do: [Map.put(entry, "name", entry["object_name"])],
        else: []
      )

  defp selected(entry, _), do: [Map.put(entry, "name", entry["object_name"])]

  defp valid(%{
         "key" => key,
         "version_id" => version,
         "modified" => modified,
         "object_name" => name,
         "delete_marker" => marker
       })
       when is_binary(key) and is_binary(version) and version != "" and
              is_integer(modified) and modified > 0 and is_binary(name) and is_boolean(marker),
       do: :ok

  defp valid(_),
    do:
      failure(
        :rewind_inventory,
        "Historical selection requires complete version identities and timestamps"
      )

  defp failure(code, message), do: {:error, Error.new(:client, message, code: code)}
end
