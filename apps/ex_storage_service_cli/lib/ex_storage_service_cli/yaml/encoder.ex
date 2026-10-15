defmodule ExStorageServiceCli.YAML.Encoder do
  @moduledoc false
  @type opts :: keyword()
  alias ExStorageServiceCli.YAML.Encode

  def encode(data, level, opts) when is_map(data), do: Encode.map(data, level, opts)
  def encode(data, level, opts) when is_list(data), do: Encode.list(data, level, opts)
  def encode(data, level, _) when is_binary(data), do: Encode.string(data, level)
  def encode(data, _, _) when is_number(data), do: Encode.number(data)
  def encode(nil, _, _), do: "null"
  def encode(true, _, _), do: "true"
  def encode(false, _, _), do: "false"
end
