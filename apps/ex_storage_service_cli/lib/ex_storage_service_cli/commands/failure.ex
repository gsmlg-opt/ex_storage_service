defmodule ExStorageServiceCli.Commands.Failure do
  @moduledoc false
  alias ExStorageServiceSdk.Error

  def reported do
    {:error, Error.new(:client, "Command failed", details: %{reported: true})}
  end
end
