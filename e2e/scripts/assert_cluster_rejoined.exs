targets = %{
  data: :"ess_data_a@127.0.0.1",
  metadata: :"ess_meta@127.0.0.1"
}

group_id = "ess-cluster-e2e"
deadline = System.monotonic_time(:millisecond) + 30_000

connect = fn target ->
  case Node.connect(target) do
    true -> :ok
    false -> {:error, {:connect, target}}
  end
end

status = fn target, replica_id ->
  :rpc.call(
    target,
    ViewstampedReplication,
    :status,
    [group_id, replica_id],
    10_000
  )
end

await = fn await ->
  result =
    with :ok <- connect.(targets.data),
         :ok <- connect.(targets.metadata),
         {:ok, data_status} <- status.(targets.data, "data-a"),
         {:ok, metadata_status} <- status.(targets.metadata, "meta"),
         true <- data_status.status == :normal,
         true <- metadata_status.status == :normal,
         true <- metadata_status.configuration_hash == data_status.configuration_hash,
         true <- data_status.applied_number == data_status.commit_number,
         true <- metadata_status.applied_number == metadata_status.commit_number,
         true <- metadata_status.commit_number == data_status.commit_number do
      {:ok, metadata_status}
    else
      other -> {:error, other}
    end

  case result do
    {:ok, metadata_status} ->
      IO.puts(
        "metadata voter rejoined at view=#{metadata_status.view_number} " <>
          "commit=#{metadata_status.commit_number}"
      )

    {:error, reason} ->
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(250)
        await.(await)
      else
        raise "metadata voter did not rejoin and catch up: #{inspect(reason)}"
      end
  end
end

await.(await)
