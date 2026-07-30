defmodule Mix.Tasks.Ess.Blob.Locate do
  use Mix.Task

  @shortdoc "Show the descriptor and recorded locations for a blob hash"
  @moduledoc """
  Shows the immutable descriptor and recorded locations for a SHA-256 hash:

      mix ess.blob.locate SHA256
  """
  @requirements ["app.start"]

  @impl Mix.Task
  def run([hash]) do
    case ExStorageService.Operations.Blob.locate(hash) do
      {:ok, result} ->
        Mix.shell().info(inspect(result, pretty: true, limit: :infinity))

      {:error, reason} ->
        Mix.raise("blob locate failed: #{inspect(reason)}")
    end
  end

  def run(_args), do: Mix.raise("usage: mix ess.blob.locate SHA256")
end
