defmodule ExStorageServiceCli.CLI.Invocation do
  @moduledoc "A parsed command, before configuration, credentials or networking."
  defstruct frontend: :native, command: [], args: [], options: %{}, filters: [], help: false
end
