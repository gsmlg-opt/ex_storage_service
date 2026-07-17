defmodule ExStorageService.ObjectServiceTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.ObjectService

  defmodule MetadataStub do
    def head_bucket(_bucket), do: :ok
  end

  defmodule NoPack do
    def locate(_hash), do: {:error, :not_found}
  end

  defmodule VersioningStub do
    def child_spec(_opts) do
      %{id: __MODULE__, start: {__MODULE__, :start_link, []}}
    end

    def start_link do
      Agent.start_link(fn -> %{next_version: 1, current: %{}, versions: %{}, calls: []} end)
    end

    def put_version(bucket, key, metadata, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        {version_id, state} = next_version(state)
        version = Map.put(metadata, :version_id, version_id)

        state =
          state
          |> put_in([:current, {bucket, key}], version)
          |> put_in([:versions, {bucket, key, version_id}], version)
          |> Map.update!(:calls, &[{:put, bucket, key, metadata, opts} | &1])

        {{:ok, version_id}, state}
      end)
    end

    def get_version(bucket, key, version_id, opts) do
      Agent.get(engine(opts), fn state ->
        version =
          if is_nil(version_id),
            do: Map.get(state.current, {bucket, key}),
            else: Map.get(state.versions, {bucket, key, version_id})

        if version, do: {:ok, version}, else: {:error, :not_found}
      end)
    end

    def delete_version(bucket, key, nil, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        {version_id, state} = next_version(state)

        marker = %{
          version_id: version_id,
          is_delete_marker: true,
          delete_marker: true,
          created_at: "2026-07-18T00:00:00Z"
        }

        state =
          state
          |> put_in([:current, {bucket, key}], marker)
          |> put_in([:versions, {bucket, key, version_id}], marker)
          |> Map.update!(:calls, &[{:delete, bucket, key, nil, opts} | &1])

        {{:ok, version_id, :delete_marker}, state}
      end)
    end

    def delete_version(bucket, key, version_id, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        versions = Map.delete(state.versions, {bucket, key, version_id})
        current = Map.get(state.current, {bucket, key})

        current_by_key =
          if current && current.version_id == version_id,
            do: Map.delete(state.current, {bucket, key}),
            else: state.current

        state = %{
          state
          | versions: versions,
            current: current_by_key,
            calls: [{:delete, bucket, key, version_id, opts} | state.calls]
        }

        {{:ok, version_id, :deleted}, state}
      end)
    end

    def calls(engine), do: Agent.get(engine, &Enum.reverse(&1.calls))

    defp next_version(state) do
      version_id = "v#{state.next_version}"
      {version_id, %{state | next_version: state.next_version + 1}}
    end

    defp engine(opts), do: Keyword.fetch!(opts, :engine)
  end

  @tag :tmp_dir
  test "put and get return domain results with a send-file source", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)
    opts = service_opts(tmp_dir, engine, operation_id: "put-op")
    body = "object-service-put"
    hash = sha256(body)

    assert {:ok,
            %{
              version_id: "v1",
              metadata: %{
                content_hash: ^hash,
                size: 18,
                etag: etag,
                content_type: "text/plain",
                metadata: %{"color" => "blue"},
                version_id: "v1"
              },
              ready_blob: %{hash: ^hash, source: {:file, path, 0, 18}}
            }} =
             ObjectService.put(
               "bucket",
               "key",
               ["object-", "service-", "put"],
               "text/plain",
               %{"color" => "blue"},
               opts
             )

    assert etag == md5(body)
    assert File.read!(path) == body

    assert {:ok,
            %{
              version_id: "v1",
              delete_marker: false,
              metadata: %{content_hash: ^hash},
              source: {:file, ^path, 0, 18}
            }} = ObjectService.get("bucket", "key", nil, opts)

    assert [{:put, "bucket", "key", %{content_hash: ^hash}, metadata_opts}] =
             VersioningStub.calls(engine)

    assert metadata_opts[:operation_id] == "put-op"
  end

  @tag :tmp_dir
  test "metadata failure leaves one recoverable orphan and no visible object", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)
    parent = self()

    fault = fn context ->
      send(parent, {:metadata_commit, context})
      {:error, :injected_metadata_failure}
    end

    opts =
      service_opts(tmp_dir, engine,
        operation_id: "orphan-op",
        faults: [metadata_commit: fault]
      )

    assert {:error, :injected_metadata_failure} =
             ObjectService.put("bucket", "orphan", "durable orphan", "text/plain", %{}, opts)

    assert_received {:metadata_commit,
                     %{
                       bucket: "bucket",
                       key: "orphan",
                       operation_id: "orphan-op",
                       metadata: %{content_hash: hash},
                       ready_blob: %{path: path, hash: ready_hash}
                     }}

    assert ready_hash == hash
    assert File.read!(path) == "durable orphan"
    assert VersioningStub.calls(engine) == []
    assert {:error, :object_not_found} = ObjectService.get("bucket", "orphan", nil, opts)
  end

  @tag :tmp_dir
  test "delete returns a marker result and latest GET exposes no blob source", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)
    opts = service_opts(tmp_dir, engine)

    assert {:ok, %{version_id: "v1"}} =
             ObjectService.put("bucket", "key", "delete-me", "text/plain", %{}, opts)

    assert {:ok, %{version_id: "v2", kind: :delete_marker}} =
             ObjectService.delete("bucket", "key", nil, opts)

    assert {:ok,
            %{
              version_id: "v2",
              delete_marker: true,
              metadata: %{is_delete_marker: true},
              source: nil
            }} = ObjectService.get("bucket", "key", nil, opts)

    assert File.exists?(LocalCAS.blob_path(sha256("delete-me"), opts[:blob_store_opts]))
  end

  @tag :tmp_dir
  test "copy reuses the immutable blob and returns destination metadata", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)
    opts = service_opts(tmp_dir, engine)
    body = "copy-without-buffering"
    hash = sha256(body)

    assert {:ok, %{version_id: "v1", ready_blob: source_ready}} =
             ObjectService.put("source", "key", body, "text/plain", %{"origin" => "source"}, opts)

    assert {:ok,
            %{
              version_id: "v2",
              metadata: %{
                content_hash: ^hash,
                size: 22,
                content_type: "text/plain",
                metadata: %{"origin" => "source"}
              },
              ready_blob: %{hash: ^hash, source: {:file, path, 0, 22}}
            }} = ObjectService.copy("source", "key", "destination", "copy", opts)

    assert path == source_ready.path

    assert [^path] =
             Path.wildcard(
               Path.join([opts[:blob_store_opts][:root], "objects", "sha256", "*", "*"])
             )

    assert {:ok, %{source: {:file, ^path, 0, 22}, metadata: %{content_hash: ^hash}}} =
             ObjectService.get("destination", "copy", nil, opts)
  end

  defp service_opts(tmp_dir, engine, extra \\ []) do
    root = Path.join(tmp_dir, "cas")

    [
      metadata: MetadataStub,
      versioning: VersioningStub,
      metadata_opts: [engine: engine],
      blob_store: LocalCAS,
      blob_store_opts: [
        root: root,
        tmp_dir: Path.join([root, "tmp", "uploads"]),
        pack_module: NoPack
      ],
      side_effects: false,
      timestamp: "2026-07-18T00:00:00Z"
    ]
    |> Keyword.merge(extra)
    |> then(fn opts ->
      operation_id = Keyword.get(extra, :operation_id)

      if operation_id,
        do: Keyword.update!(opts, :metadata_opts, &Keyword.put(&1, :operation_id, operation_id)),
        else: opts
    end)
  end

  defp sha256(data),
    do: :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)

  defp md5(data),
    do: :md5 |> :crypto.hash(data) |> Base.encode16(case: :lower)
end
