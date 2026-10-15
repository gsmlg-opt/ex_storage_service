defmodule ExStorageServiceCli.Transfer.State do
  @moduledoc "Native durable state location, independent of credential/configuration files."
  def root(env \\ System.get_env()) do
    cond do
      env["ESS_STATE_DIR"] not in [nil, ""] ->
        Path.expand(env["ESS_STATE_DIR"])

      env["XDG_STATE_HOME"] not in [nil, ""] ->
        Path.join(Path.expand(env["XDG_STATE_HOME"]), "ess")

      true ->
        Path.join(env["HOME"] || System.user_home!(), ".local/state/ess")
    end
  end

  def transfers(env \\ System.get_env()), do: Path.join(root(env), "transfers")
end
