defmodule ExStorageServiceCli.CLI.OptionHelp do
  @moduledoc "Value hints for the same declarative types consumed by CLI parsing."

  def suffix(:boolean), do: ""
  def suffix({:flag, _}), do: ""
  def suffix({:optional_string, _, _}), do: " [<value>]"
  def suffix(:optional), do: " [<value>]"
  def suffix(:string_list), do: " <value> [<value> ...]"
  def suffix(:list), do: " <value> [<value> ...]"
  def suffix(_), do: " <value>"

  def choices({:enum, values}), do: values
  def choices({:optional_string, _, values}), do: values
  def choices(_), do: []
end
