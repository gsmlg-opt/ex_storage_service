defmodule ExStorageServiceCli.Transfer.SyncPolicy do
  @moduledoc "Distinct AWS sync and native mc mirror comparison policies."

  def compare(_policy, _direction, _source, nil, _options), do: {:copy, :missing}

  def compare(:aws, direction, source, destination, options) do
    cond do
      source["size"] != destination["size"] ->
        {:copy, :size}

      options[:size_only] ->
        {:skip, :same_size}

      direction == :download and options[:exact_timestamps] == true and
          source["modified"] != destination["modified"] ->
        {:copy, :timestamp}

      direction == :download and options[:exact_timestamps] != true and
          destination["modified"] > source["modified"] ->
        {:copy, :timestamp}

      direction != :download and source["modified"] > destination["modified"] ->
        {:copy, :timestamp}

      true ->
        {:skip, :unchanged}
    end
  end

  def compare(:mirror, _direction, source, destination, options) do
    differs =
      source["size"] != destination["size"] or
        source["modified"] != destination["modified"] or
        (source["etag"] != nil and destination["etag"] != nil and
           source["etag"] != destination["etag"])

    cond do
      not differs -> {:skip, :unchanged}
      options[:overwrite] == true -> {:copy, :overwrite}
      true -> {:conflict, :overwrite_required}
    end
  end
end
