defmodule ExStorageService.Operations.Capacity do
  @moduledoc """
  Bounded, read-only capacity diagnostics for release RPC.

  `snapshot/0` probes exactly the blob, temporary and metadata filesystems. Each
  probe has a five-second OS deadline. It does not inventory objects, recurse
  directories, mutate state on disk, or infer readiness from capacity alone.
  Linux requires the existing `df` and `timeout` utilities (including BusyBox).
  Missing tools, uncreated roots, probe failures and deadlines are explicit
  incomplete observations; a failure never publishes healthy zero capacity.
  """

  alias ExStorageService.{InstanceConfig, Metrics}

  @doc "Returns per-root observations and the bounded in-process staging observation."
  def snapshot(opts \\ []) do
    roots = Keyword.get_lazy(opts, :roots, &configured_roots/0)
    probe = Keyword.get(opts, :probe, &filesystem/1)

    observations =
      Map.new([:blob, :tmp, :metadata], fn kind ->
        result =
          case Map.fetch(roots, kind) do
            {:ok, path} -> safe_probe(probe, path)
            :error -> {:error, :missing_root}
          end

        observation =
          case result do
            {:ok, values} -> Map.put(values, :complete, true)
            {:error, reason} -> %{complete: false, error: reason}
          end

        :telemetry.execute(
          [:ex_storage_service, :capacity, :observe],
          Map.take(observation, [:total_bytes, :available_bytes]),
          %{root: kind, complete: observation.complete}
        )

        {kind, observation}
      end)

    %{
      complete: Enum.all?(observations, fn {_, value} -> value.complete end),
      roots: observations,
      staging: Metrics.staging_snapshot()
    }
  end

  defp configured_roots do
    case InstanceConfig.from_application_env() do
      {:ok, config} ->
        %{blob: config.blob_root, tmp: config.tmp_root, metadata: config.metadata_root}

      {:error, _} ->
        %{}
    end
  end

  defp safe_probe(probe, path) do
    case probe.(path) do
      {:ok, %{total_bytes: total, available_bytes: available} = result}
      when is_integer(total) and is_integer(available) and total > 0 and
             available >= 0 and available <= total ->
        {:ok, result}

      {:error, reason}
      when reason in [:unsupported_platform, :deadline, :probe_failed, :invalid_output] ->
        {:error, reason}

      _ ->
        {:error, :probe_failed}
    end
  rescue
    _ -> {:error, :probe_failed}
  catch
    _, _ -> {:error, :probe_failed}
  end

  defp filesystem(path) do
    with timeout when is_binary(timeout) <- System.find_executable("timeout"),
         df when is_binary(df) <- System.find_executable("df") do
      case System.cmd(timeout, ["-s", "KILL", "5", df, "-Pk", "--", Path.expand(path)],
             stderr_to_stdout: true,
             env: [{"LC_ALL", "C"}]
           ) do
        {output, 0} -> parse(output)
        {_, status} when status in [124, 137] -> {:error, :deadline}
        _ -> {:error, :probe_failed}
      end
    else
      _ -> {:error, :unsupported_platform}
    end
  end

  defp parse(output) do
    case output |> String.split("\n", trim: true) |> List.last() |> String.split() do
      [_device, total, _used, available, _percent | _mount] ->
        with {total, ""} <- Integer.parse(total),
             {available, ""} <- Integer.parse(available) do
          {:ok, %{total_bytes: total * 1024, available_bytes: available * 1024}}
        else
          _ -> {:error, :invalid_output}
        end

      _ ->
        {:error, :invalid_output}
    end
  end
end
