defmodule ExStorageServiceCli.Commands.Failure do
  @moduledoc false
  alias ExStorageServiceCli.Error

  def reported do
    {:error, Error.new(:client, "Command failed", details: %{reported: true})}
  end
end
