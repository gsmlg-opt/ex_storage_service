defmodule ExStorageService.Names do
  @moduledoc """
  Stable local process names for storage instances.

  Arbitrary instance identifiers remain Registry keys; they are never
  converted into atoms. The default instance retains historical module names
  for compatibility with existing callers.
  """

  @registry ExStorageService.Registry

  @spec registry() :: atom()
  def registry, do: @registry

  @spec via(atom() | String.t(), atom()) :: {:via, Registry, {atom(), term()}}
  def via(instance, component),
    do: {:via, Registry, {@registry, {instance, component}}}

  @spec instance_supervisor(atom() | String.t()) :: {:via, Registry, {atom(), term()}}
  def instance_supervisor(instance), do: via(instance, :instance_supervisor)

  @spec process(atom() | String.t(), atom(), atom()) :: atom() | tuple()
  def process(instance, component, legacy_name) do
    if default_instance?(instance), do: legacy_name, else: via(instance, component)
  end

  @spec default_instance?(term()) :: boolean()
  def default_instance?(instance), do: instance in [:default, "default"]
end
