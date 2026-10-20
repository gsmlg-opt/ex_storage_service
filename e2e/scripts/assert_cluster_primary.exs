Code.require_file("cluster_primary_lifecycle.exs", __DIR__)
ESSClusterPrimaryLifecycle.run(List.first(System.argv()) || "record")
