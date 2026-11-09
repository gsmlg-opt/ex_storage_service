# Manual native check only; never adds records to the full A28 fixture.
# Run with fresh ESS_* roots and MIX_ENV=prod mix run --no-compile --no-start
# e2e/scripts/index_offline_inventory_probe.exs /absolute/retained-receipt.json
Code.require_file("index_setup_diagnostics.exs", __DIR__)
Code.require_file("index_offline_inventory.exs", __DIR__)

defmodule ESSIndexAcceptance.InventoryProbe.KV do
  def revision(opts) do
    [consistency: :linearizable] = opts
    {:ok, revision} = result = Concord.KV.revision(opts)
    Process.put(:inventory_probe_revision, revision)
    Process.put(:inventory_probe_pages, 0)
    Process.put(:inventory_probe_cursor, nil)
    result
  end

  def list(opts) do
    revision = Process.get(:inventory_probe_revision)
    cursor = Process.get(:inventory_probe_cursor)
    selector = if is_nil(cursor), do: [prefix: ""], else: [range: {"", cursor}]
    expected = [consistency: :linearizable, revision: revision, order: :desc] ++ selector
    ^expected = opts
    {:ok, records, page} = result = Concord.KV.list(opts)
    pages = Process.get(:inventory_probe_pages)

    if pages == 0 do
      true = page.has_more
      pending = ["a:overwrite", "a:delete", "a:recreate"]
      true = Enum.all?(pending, fn key -> Enum.all?(records, &(&1.key != key)) end)
      :ok = Concord.put("a:overwrite", :after_snapshot)
      :ok = Concord.delete("a:delete")
      :ok = Concord.delete("a:recreate")
      :ok = Concord.put("a:recreate", :after_recreation)
      :ok = Concord.put("a:inserted", :after_snapshot)
    end

    Process.put(:inventory_probe_pages, pages + 1)
    Process.put(:inventory_probe_cursor, page.last_key)
    result
  end
end

defmodule ESSIndexAcceptance.InventoryProbe do
  alias ESSIndexAcceptance.Inventory
  alias ESSIndexAcceptance.InventoryProbe.KV

  def run(receipt) do
    root = fresh_roots!()
    false = File.exists?(receipt)
    {:ok, started} = ESSIndexSetup.start_core!()

    result =
      try do
        {:ok, empty} = Concord.get_all(consistency: :linearizable)
        0 = map_size(empty)
        fixture!()
        {:ok, original} = Concord.get_all(consistency: :linearizable)
        {:ok, inventory} = Inventory.snapshot()
        ^original = inventory
        verify_values!(inventory)
        original_sizes = sizes(original)
        ^original_sizes = sizes(inventory)

        {:ok, pinned} = Inventory.snapshot(KV)
        ^original = pinned
        ^original_sizes = sizes(pinned)
        pages = Process.get(:inventory_probe_pages)
        true = pages >= 2
        {:ok, changed} = Concord.get_all(consistency: :linearizable)
        false = changed == original
        :after_snapshot = Map.fetch!(changed, "a:overwrite")
        false = Map.has_key?(changed, "a:delete")
        :after_recreation = Map.fetch!(changed, "a:recreate")
        :after_snapshot = Map.fetch!(changed, "a:inserted")
        {:ok, current} = Inventory.snapshot()
        ^changed = current

        {sha, 0} = System.cmd("git", ["rev-parse", "HEAD"])

        %{
          status: "passed",
          scope: "small native VSR inventory API equivalence; not full A28 acceptance",
          source_head: String.trim(sha),
          source_sha256: source_hashes(),
          elixir: System.version(),
          otp: System.otp_release(),
          data_root: root,
          records: map_size(original),
          pinned_revision: Process.get(:inventory_probe_revision),
          pinned_pages: pages,
          inventory_sizes: original_sizes,
          raw_stored_value_equivalence: true,
          quiescent_map_equivalence: true,
          fixed_revision_after_mutations: true,
          current_map_after_mutations: true,
          default_operation_deadlines: true
        }
      after
        for app <- Enum.reverse(started), do: :ok = Application.stop(app)
      end

    nil = Process.whereis(Concord.Engine.VSR)
    nil = Process.whereis(ExStorageService.Supervisor)
    nil = Process.whereis(ViewstampedReplication.Registry)
    contents = JSON.encode!(Map.put(result, :runtime_stopped, true)) <> "\n"
    File.write!(receipt, contents, [:exclusive])
    IO.puts("PASS native offline inventory; receipt: " <> receipt)
  end

  defp fresh_roots! do
    root = System.fetch_env!("ESS_DATA_ROOT") |> Path.expand()
    true = String.starts_with?(root, Path.join(System.tmp_dir!(), "ess-inventory-probe-"))
    [] = File.ls!(root)
    nil = Process.whereis(Concord.Engine.VSR)
    nil = Process.whereis(ExStorageService.Supervisor)
    {:ok, config} = ExStorageService.InstanceConfig.from_application_env()
    :standalone = config.mode
    true = config.auto_start
    false = config.web_enabled
    false = config.public_s3_enabled
    :v2 = config.metadata_schema
    ^root = config.data_root

    for {field, suffix, variable} <- [
          {:blob_root, "blob", "ESS_BLOB_ROOT"},
          {:tmp_root, "tmp", "ESS_TMP_ROOT"},
          {:metadata_root, "metadata", "ESS_METADATA_ROOT"},
          {:ra_root, "ra", "ESS_RA_ROOT"}
        ] do
      expected = Path.join(root, suffix)
      ^expected = System.fetch_env!(variable)
      ^expected = Map.fetch!(config, field)
    end

    metadata = Path.join(root, "metadata")
    ^metadata = Application.fetch_env!(:concord, :data_dir)
    :file = Application.fetch_env!(:concord, :vsr) |> Keyword.fetch!(:storage)
    root
  end

  defp fixture! do
    for number <- 1..1_005 do
      key = "unrelated:" <> String.pad_leading(Integer.to_string(number), 4, "0")
      :ok = Concord.put(key, %{ordinal: number})
    end

    for {key, value} <- [
          {String.duplicate("z", 4_096), :maximum_length},
          {<<0>>, :zero_key},
          {"a" <> <<0>>, :zero_suffix},
          {"bucket:fixture", %{generation: "fixture"}},
          {"bucket:unrelated", %{generation: "unrelated"}},
          {"ess:v2:list_index:fixture:root", %{schema: 1}},
          {"a:overwrite", :initial},
          {"a:delete", :retained_before_capture},
          {"a:recreate", :initial},
          {"a:removed", :removed}
        ] do
      :ok = Concord.put(key, value)
    end

    :ok = Concord.put("a:overwrite", :before_snapshot)
    :ok = Concord.delete("a:removed")
    :ok = Concord.delete("a:recreate")
    :ok = Concord.put("a:recreate", :before_recreation)
    :ok = Concord.put("a:raw", String.duplicate("raw-value-", 1_024), compress: true)
  end

  defp verify_values!(inventory) do
    :maximum_length = Map.fetch!(inventory, String.duplicate("z", 4_096))
    :zero_key = Map.fetch!(inventory, <<0>>)
    :zero_suffix = Map.fetch!(inventory, "a" <> <<0>>)
    :before_snapshot = Map.fetch!(inventory, "a:overwrite")
    :before_recreation = Map.fetch!(inventory, "a:recreate")
    false = Map.has_key?(inventory, "a:removed")
    {:compressed, :zlib, binary} = raw = Map.fetch!(inventory, "a:raw")
    true = is_binary(binary)
    logical = String.duplicate("raw-value-", 1_024)
    ^logical = Concord.Compression.decompress(raw)
  end

  defp sizes(snapshot) do
    nodes =
      Enum.filter(snapshot, fn {key, _} -> String.starts_with?(key, "ess:v2:list_index:") end)

    %{
      source_serialized_bytes: :erlang.external_size(snapshot),
      index_serialized_bytes: :erlang.external_size(nodes),
      index_records: length(nodes)
    }
  end

  defp source_hashes do
    Map.new(["index_offline_inventory.exs", "index_offline_inventory_probe.exs"], fn name ->
      digest = :crypto.hash(:sha256, File.read!(Path.join(__DIR__, name)))
      {name, Base.encode16(digest, case: :lower)}
    end)
  end
end

[receipt] = System.argv()
true = Path.type(receipt) == :absolute
ESSIndexAcceptance.InventoryProbe.run(receipt)
