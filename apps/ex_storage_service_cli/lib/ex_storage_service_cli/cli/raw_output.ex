defmodule ExStorageServiceCli.CLI.RawOutput do
  @moduledoc "Scopes byte-preserving stdout mode to one exclusively owned raw transfer."
  alias ExStorageServiceCli.Error

  def with_stdout(function) do
    with options when is_list(options) <- :io.getopts(:standard_io),
         :ok <- :io.setopts(:standard_io, encoding: :latin1) do
      try do
        function.()
      after
        :io.setopts(:standard_io, encoding: Keyword.get(options, :encoding, :latin1))
      end
    else
      _ -> {:error, Error.new(:transfer, "Cannot configure binary stdout", code: :stdout_mode)}
    end
  end
end
