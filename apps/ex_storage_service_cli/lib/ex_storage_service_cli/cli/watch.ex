defmodule ExStorageServiceCli.CLI.Watch do
  @moduledoc "Explicit polling of bounded disk inventories; snapshots do not provide event delivery guarantees."
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.Transfer.{Coordinator, Inventory, Manifest}

  @doc "Capture a baseline before reconciliation, then compare complete polling snapshots."
  def run(invocation, target, context, opts \\ []) do
    with :ok <- validate(invocation.options, target),
         settings = %{
           target: target,
           context: context,
           opts: opts,
           interval: Map.get(invocation.options, :interval, 1) * 1000,
           coordinator: opts[:coordinator] || if(context, do: context.coordinator),
           inventory: opts[:inventory] || (&inventory/3),
           sleep: opts[:sleep] || (&Process.sleep/1),
           emit: opts[:emit] || fn event -> IO.puts(JSON.encode!(event)) end,
           reconcile: opts[:on_reconcile] || fn -> :ok end
         },
         {:ok, baseline} <- snapshot(settings) do
      case callback(settings.reconcile, [], :watch_reconcile) do
        :ok ->
          loop(baseline, Map.get(invocation.options, :count, 0), settings)

        error ->
          Manifest.cleanup(baseline)
          error
      end
    end
  end

  defp validate(options, target) do
    cond do
      options[:adapter] != "polling" ->
        failure(:syntax, :watch_adapter, "Watch requires an explicit --adapter polling")

      not is_integer(Map.get(options, :interval, 1)) or
          Map.get(options, :interval, 1) not in 1..3600 ->
        failure(
          :syntax,
          :watch_interval,
          "Polling interval must be an integer from 1 to 3600 seconds"
        )

      not is_integer(Map.get(options, :count, 0)) or Map.get(options, :count, 0) < 0 ->
        failure(:syntax, :watch_count, "Polling count must be a nonnegative integer")

      target.kind not in [:local, :remote] ->
        failure(:syntax, :watch_target, "Watch requires a local directory or S3 bucket/prefix")

      true ->
        :ok
    end
  end

  defp inventory(%Target{kind: :local, path: path}, _context, opts) do
    source = Path.expand(path)
    temporary = Path.expand(opts[:tmp_dir] || System.tmp_dir!())

    if temporary == source or
         String.starts_with?(temporary, String.trim_trailing(source, "/") <> "/") do
      failure(
        :configuration,
        :watch_snapshot_location,
        "Watch snapshots require --tmpdir outside the watched directory"
      )
    else
      Inventory.local(path, Keyword.put(opts, :follow_symlinks, false))
    end
  end

  defp inventory(%Target{kind: :remote} = target, context, opts),
    do: Inventory.remote(context, target, opts)

  defp snapshot(settings) do
    guarded(:watch_inventory, fn ->
      with :ok <- Coordinator.check(settings.coordinator) do
        case settings.inventory.(settings.target, settings.context, settings.opts) do
          {:ok, %Manifest{} = manifest} ->
            case Coordinator.check(settings.coordinator) do
              :ok ->
                {:ok, manifest}

              error ->
                Manifest.cleanup(manifest)
                error
            end

          {:error, %Error{}} = error ->
            error

          _ ->
            failure(:client, :watch_inventory, "Polling inventory returned an invalid result")
        end
      end
    end)
  end

  defp loop(previous, remaining, settings) do
    case step(previous, settings) do
      {:ok, next} ->
        Manifest.cleanup(previous)

        if remaining == 1 do
          Manifest.cleanup(next)
          :ok
        else
          loop(next, max(remaining - 1, 0), settings)
        end

      error ->
        Manifest.cleanup(previous)
        error
    end
  end

  defp step(previous, settings) do
    with :ok <- guarded(:watch_sleep, fn -> idle(settings.interval, settings) end),
         {:ok, next} <- snapshot(settings) do
      result = guarded(:watch_scan, fn -> compare(previous, next, settings) end)

      case result do
        :ok ->
          {:ok, next}

        error ->
          Manifest.cleanup(next)
          error
      end
    end
  end

  defp idle(0, settings), do: Coordinator.check(settings.coordinator)

  defp idle(remaining, settings) do
    duration = min(remaining, 50)

    with :ok <- Coordinator.check(settings.coordinator),
         :ok <- callback(settings.sleep, [duration], :watch_sleep),
         do: idle(remaining - duration, settings)
  end

  defp compare(previous, next, settings) do
    result =
      Manifest.pairs(previous, next)
      |> Enum.reduce_while({:ok, false}, fn {before, after_entry}, {:ok, changed} ->
        with :ok <- Coordinator.check(settings.coordinator),
             :ok <- emit(before, after_entry, settings) do
          {:cont, {:ok, changed or before != after_entry}}
        else
          error -> {:halt, error}
        end
      end)

    with {:ok, changed} <- result,
         :ok <- Coordinator.check(settings.coordinator) do
      if changed, do: callback(settings.reconcile, [], :watch_reconcile), else: :ok
    end
  end

  defp emit(entry, entry, _settings), do: :ok

  defp emit(before, after_entry, settings) do
    event = %{
      "Adapter" => "polling",
      "Event" =>
        cond do
          before == nil -> "create"
          after_entry == nil -> "remove"
          true -> "modify"
        end,
      "Name" => (after_entry || before)["name"],
      "Before" => before,
      "After" => after_entry
    }

    callback(settings.emit, [event], :watch_emit)
  end

  defp callback(function, args, code) do
    guarded(code, fn ->
      case apply(function, args) do
        :ok -> :ok
        {:ok, _} -> :ok
        {:error, %Error{}} = error -> error
        _ -> failure(:client, code, "Polling callback returned an invalid result")
      end
    end)
  end

  defp guarded(code, function) do
    function.()
  rescue
    _ -> failure(:client, code, "Polling watch could not complete")
  catch
    _, _ -> failure(:client, code, "Polling watch could not complete")
  end

  defp failure(category, code, message), do: {:error, Error.new(category, message, code: code)}
end
