defmodule ExStorageService.CloudCache.LocalStoreConcurrencyTest do
  use ExUnit.Case, async: false

  alias ExStorageService.CloudCache.{Config, LocalStore}
  alias ExStorageService.Metadata.ListIndex

  @moduletag :tmp_dir

  defmodule Backend do
    def get(key, opts), do: Agent.get(opts[:engine], &{:ok, &1.records[key]})

    def transaction(spec, opts) do
      Agent.get_and_update(opts[:engine], fn state ->
        state = interfere(state)

        if Enum.all?(spec.compare, &compare(&1, state.records)) do
          revision = state.revision + 1

          records =
            Enum.reduce(spec.success, state.records, fn {:put, key, value, _}, records ->
              Map.put(records, key, %{value: value, mod_revision: revision})
            end)

          {{:ok, %{succeeded: true}}, %{state | records: records, revision: revision}}
        else
          {{:ok, %{succeeded: false}}, state}
        end
      end)
    end

    defp interfere(%{race: nil} = state), do: state

    defp interfere(%{race: {key, value}} = state) do
      record = %{value: value, mod_revision: state.revision + 1}

      %{
        state
        | records: Map.put(state.records, key, record),
          race: nil,
          revision: state.revision + 1
      }
    end

    defp compare({:exists, key, :==, value}, records), do: Map.has_key?(records, key) == value

    defp compare({:mod_revision, key, :==, value}, records),
      do: match?(%{mod_revision: ^value}, records[key])
  end

  setup %{tmp_dir: directory} do
    previous = Application.get_env(:ex_storage_service, :data_root)
    Application.put_env(:ex_storage_service, :data_root, directory)
    on_exit(fn -> Application.put_env(:ex_storage_service, :data_root, previous) end)
    bucket = "cache-#{System.unique_integer([:positive])}"
    root_key = ListIndex.root_key(bucket, "generation")

    records = %{
      "bucket:#{bucket}" => %{value: %{generation: "generation"}, mod_revision: 1},
      root_key => %{value: ListIndex.empty("generation"), mod_revision: 1}
    }

    {:ok, engine} = Agent.start_link(fn -> %{records: records, revision: 1, race: nil} end)

    %{
      bucket: bucket,
      root_key: root_key,
      engine: engine,
      directory: directory,
      opts: [backend: Backend, engine: engine],
      config: %Config{cache_enabled: true, cache_max_bytes: 1024}
    }
  end

  test "four concurrent fills retain every successful entry without orphaning evicted files",
       context do
    results =
      1..4
      |> Task.async_stream(
        fn number ->
          fill(context, "key-#{number}", "content-#{number}", 30)
        end,
        max_concurrency: 4,
        timeout: 5000
      )
      |> Enum.to_list()

    assert Enum.all?(results, &match?({:ok, {:ok, _}}, &1))
    index = Agent.get(context.engine, & &1.records["cloud_cache_index:#{context.bucket}"].value)
    assert map_size(index) == 4
    assert Enum.all?(results, fn {:ok, {:ok, path}} -> File.regular?(path) end)
    assert length(Path.wildcard(Path.join(context.directory, "#{context.bucket}/cache/*/*"))) == 4
  end

  test "a competing cache CAS update is retained on retry", context do
    existing = %{content_hash: hash("other"), size: 5, last_accessed_at: "old", validator: nil}

    Agent.update(
      context.engine,
      &%{&1 | race: {"cloud_cache_index:#{context.bucket}", %{"other" => existing}}}
    )

    assert {:ok, _} = fill(context, "new", "new")
    index = Agent.get(context.engine, & &1.records["cloud_cache_index:#{context.bucket}"].value)
    assert Map.keys(index) |> Enum.sort() == ["new", "other"]
  end

  test "index cutover race rejects cache commit and removes only its unpublished bytes",
       context do
    root = %{ListIndex.empty("generation") | state: :building}
    Agent.update(context.engine, &%{&1 | race: {context.root_key, root}})
    assert {:error, :listing_index_unavailable} = fill(context, "new", "new")

    refute Agent.get(
             context.engine,
             &Map.has_key?(&1.records, "cloud_cache_index:#{context.bucket}")
           )

    refute Enum.any?(
             Path.wildcard(Path.join(context.directory, "#{context.bucket}/cache/*/*")),
             &File.regular?/1
           )
  end

  test "generation replacement cannot be accepted by a cache CAS retry", context do
    Agent.update(
      context.engine,
      &%{&1 | race: {"bucket:#{context.bucket}", %{generation: "replacement"}}}
    )

    assert {:error, :bucket_generation_changed} = fill(context, "new", "new")
  end

  test "cache contention returns immediately and eviction preserves an opened reader", context do
    lock = {{LocalStore, context.bucket}, self()}
    assert :global.set_lock(lock, [node()], 0)
    task = Task.async(fn -> fill(context, "busy", "busy") end)
    assert {:error, :cache_busy} = Task.await(task, 1000)
    :global.del_lock(lock, [node()])

    context = %{context | config: %{context.config | cache_max_bytes: 8}}
    assert {:ok, first} = fill(context, "a", "12345678")
    {:ok, reader} = File.open(first, [:read, :raw, :binary])

    try do
      assert {:ok, _} = fill(context, "b", "abcdefgh")
      refute File.exists?(first)
      assert IO.binread(reader, 8) == "12345678"
      index = Agent.get(context.engine, & &1.records["cloud_cache_index:#{context.bucket}"].value)
      assert Enum.sum(for {_, entry} <- index, do: entry.size) <= 8
    after
      File.close(reader)
    end
  end

  defp fill(context, key, bytes, attempts \\ 1) do
    source = Path.join(context.directory, "stage-" <> key)
    File.write!(source, bytes)

    case LocalStore.put_file(
           context.bucket,
           key,
           source,
           hash(bytes),
           byte_size(bytes),
           context.config,
           context.opts
         ) do
      {:error, :cache_busy} when attempts > 1 ->
        Process.sleep(5)
        fill(context, key, bytes, attempts - 1)

      result ->
        result
    end
  end

  defp hash(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
end
