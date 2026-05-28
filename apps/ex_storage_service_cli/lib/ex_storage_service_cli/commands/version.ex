defmodule ExStorageServiceCli.Commands.Version do
  @moduledoc """
  Print CLI version.
  """

  def run(_command, _args, ctx) do
    version = ExStorageServiceCli.version()

    if ctx.json do
      IO.puts(Jason.encode!(%{version: version}))
    else
      IO.puts("ess version #{version}")
    end
  end

  def help(_command) do
    IO.puts("""
    #{IO.ANSI.bright()}ess version#{IO.ANSI.reset()} — Print CLI version

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess version
    """)
  end
end
