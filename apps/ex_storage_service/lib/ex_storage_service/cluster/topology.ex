defmodule ExStorageService.Cluster.Topology do
  @moduledoc """
  Builds the discovery children for the configured cluster topology.

  VSR membership is never inferred from discovered nodes.
  """

  alias ExStorageService.Cluster.StaticDiscovery
  alias ExStorageService.InstanceConfig

  @spec children(InstanceConfig.t()) :: [Supervisor.child_spec()]
  def children(%InstanceConfig{mode: :standalone}), do: []

  def children(%InstanceConfig{cluster_topology: :static, cluster_seeds: seeds}) do
    [{StaticDiscovery, seeds: seeds}]
  end

  def children(%InstanceConfig{cluster_topology: :dns, cluster_seeds: queries}) do
    [{DNSCluster, query: queries, name: ExStorageService.Cluster.DNSDiscovery}]
  end
end
