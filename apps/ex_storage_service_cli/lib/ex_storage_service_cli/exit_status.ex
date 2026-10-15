defmodule ExStorageServiceCli.ExitStatus do
  @moduledoc "Maps structured failures to the command frontend's exit contract."
  alias ExStorageServiceCli.Error

  def code(:ok, _frontend), do: 0
  def code(%Error{category: :interrupted}, _frontend), do: 130
  def code(%Error{category: :terminated}, _frontend), do: 143
  def code(%Error{}, :native), do: 1

  def code(%Error{details: %{high_level_prefix: _}, category: category}, :s3)
      when category in [:service, :transport, :client],
      do: 1

  def code(%Error{category: :transfer}, _frontend), do: 1
  def code(%Error{category: :skipped}, _frontend), do: 2
  def code(%Error{category: :syntax}, _frontend), do: 252

  def code(%Error{category: category}, _frontend)
      when category in [:config, :configuration, :credentials],
      do: 253

  def code(%Error{category: :service}, _frontend), do: 254
  def code(%Error{}, _frontend), do: 255
end
