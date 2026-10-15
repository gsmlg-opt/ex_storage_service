defmodule Mix.Tasks.Compile.EssSignals do
  @moduledoc "Builds the invocation SIGINT bridge into the build tree, never the source package."
  use Mix.Task.Compiler
  @recursive true
  @source Path.expand("../c_src/ess_signal.c", __DIR__)
  @pager_source Path.expand("../c_src/ess_pager.c", __DIR__)

  def run(args) do
    signal = build_signals(args)
    pager = build_pager(args)
    if signal == {:noop, []} and pager == {:noop, []}, do: {:noop, []}, else: {:ok, []}
  end

  defp build_signals(args) do
    flags =
      case :os.type() do
        {:unix, :linux} -> ["-shared", "-fPIC"]
        {:unix, :darwin} -> ["-dynamiclib", "-undefined", "dynamic_lookup"]
        _ -> Mix.raise("The ess SIGINT bridge requires a supported POSIX build target")
      end

    compiler =
      System.get_env("CC") || System.find_executable("cc") ||
        Mix.raise("A C compiler and OTP headers are required to build ess")

    priv = Path.join(Mix.Project.app_path(), "priv")
    materialize_priv(priv)
    target = Path.join(priv, "native/ess_signals.so")
    metadata = Path.join(priv, "native/ess_signals.json")
    include = Path.join(to_string(:code.root_dir()), "usr/include")
    fingerprint = :crypto.hash(:sha256, File.read!(@source)) |> Base.encode16(case: :lower)

    platform = %{
      os: :os.type() |> elem(1) |> Atom.to_string(),
      architecture:
        :erlang.system_info(:system_architecture) |> to_string() |> String.split("-") |> hd(),
      otp_release: to_string(:erlang.system_info(:otp_release)),
      source_sha256: fingerprint
    }

    old =
      case File.read(metadata) do
        {:ok, bytes} ->
          case JSON.decode(bytes) do
            {:ok, value} -> value
            _ -> %{}
          end

        _ ->
          %{}
      end

    current = Map.new(platform, fn {key, value} -> {Atom.to_string(key), value} end)

    if "--force" not in args and File.regular?(target) and
         Map.take(old, Map.keys(current)) == current and
         old["sha256"] ==
           Base.encode16(:crypto.hash(:sha256, File.read!(target)), case: :lower) do
      {:noop, []}
    else
      File.mkdir_p!(Path.dirname(target))
      temporary = target <> ".tmp-" <> Integer.to_string(System.unique_integer([:positive]))

      options =
        ["-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-pthread", "-I", include] ++
          flags ++ [@source, "-o", temporary]

      try do
        case System.cmd(compiler, options, stderr_to_stdout: true) do
          {_output, 0} ->
            digest = :crypto.hash(:sha256, File.read!(temporary)) |> Base.encode16(case: :lower)
            File.rename!(temporary, target)
            File.write!(metadata, JSON.encode!(Map.put(platform, :sha256, digest)))
            {:ok, []}

          {output, _status} ->
            Mix.raise("Cannot compile ess SIGINT bridge:\n" <> output)
        end
      after
        File.rm(temporary)
      end
    end
  end

  def manifests,
    do:
      Enum.map(
        ["ess_signals.json", "ess_pager.json"],
        &Path.join(Mix.Project.app_path(), "priv/native/" <> &1)
      )

  def clean do
    Enum.each(
      ["ess_signals.so", "ess_signals.json", "ess_pager", "ess_pager.json"],
      &File.rm(Path.join(Mix.Project.app_path(), "priv/native/" <> &1))
    )

    :ok
  end

  defp build_pager(args) do
    compiler =
      System.get_env("CC") || System.find_executable("cc") ||
        Mix.raise("A C compiler is required to build ess pager support")

    priv = Path.join(Mix.Project.app_path(), "priv")
    materialize_priv(priv)
    target = Path.join(priv, "native/ess_pager")
    metadata = target <> ".json"

    platform = %{
      "os" => :os.type() |> elem(1) |> Atom.to_string(),
      "architecture" =>
        :erlang.system_info(:system_architecture) |> to_string() |> String.split("-") |> hd(),
      "source_sha256" =>
        :crypto.hash(:sha256, File.read!(@pager_source)) |> Base.encode16(case: :lower)
    }

    old =
      with {:ok, bytes} <- File.read(metadata),
           {:ok, value} <- JSON.decode(bytes),
           do: value,
           else: (_ -> %{})

    if "--force" not in args and File.regular?(target) and
         Map.take(old, Map.keys(platform)) == platform and
         old["sha256"] == Base.encode16(:crypto.hash(:sha256, File.read!(target)), case: :lower) do
      {:noop, []}
    else
      File.mkdir_p!(Path.dirname(target))
      temporary = target <> ".tmp-" <> Integer.to_string(System.unique_integer([:positive]))

      try do
        case System.cmd(
               compiler,
               ["-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", @pager_source, "-o", temporary],
               stderr_to_stdout: true
             ) do
          {_output, 0} ->
            digest = :crypto.hash(:sha256, File.read!(temporary)) |> Base.encode16(case: :lower)
            File.chmod!(temporary, 0o755)
            File.rename!(temporary, target)
            File.write!(metadata, JSON.encode!(Map.put(platform, "sha256", digest)))
            {:ok, []}

          {output, _} ->
            Mix.raise("Cannot compile ess pager helper:\n" <> output)
        end
      after
        File.rm(temporary)
      end
    end
  end

  defp materialize_priv(priv) do
    case File.lstat(priv) do
      {:ok, %File.Stat{type: :symlink}} ->
        source = Path.expand("../priv", __DIR__)
        File.rm!(priv)
        File.cp_r!(source, priv)

      {:ok, %File.Stat{type: :directory}} ->
        :ok

      {:error, :enoent} ->
        File.mkdir_p!(priv)

      _ ->
        Mix.raise("Unexpected CLI build priv path")
    end
  end
end
