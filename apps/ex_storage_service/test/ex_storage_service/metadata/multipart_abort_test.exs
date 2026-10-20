defmodule ExStorageService.Metadata.MultipartAbortTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.{BucketLifecycle, Keys, ListIndex, MultipartCommit}
  alias ExStorageService.Storage.Multipart

  defmodule InterleavingBackend do
    alias ExStorageService.Metadata.Backend.Concord, as: Backend
    alias ExStorageService.Metadata.ListIndex
    alias ExStorageService.Storage.Multipart

    def get(key, opts), do: Backend.get(key, Keyword.delete(opts, :engine))

    def transaction(spec, opts) do
      state =
        Agent.get_and_update(Keyword.fetch!(opts, :engine), fn state ->
          {state, %{state | remaining: max(state.remaining - 1, 0), attempts: state.attempts + 1}}
        end)

      if state.remaining > 0 do
        case state.change do
          :unrelated ->
            {:ok, _id} = Multipart.init_upload(state.bucket, "unrelated-#{state.remaining}")

          status when status in [:completing, :aborted] ->
            {:ok, record} = Backend.get(state.key)

            {:ok, %{succeeded: true}} =
              ListIndex.transaction(Backend, state.bucket, %{
                compare: [{:mod_revision, state.key, :==, record.mod_revision}],
                success: [{:put, state.key, Map.put(record.value, :status, status), %{}}],
                failure: []
              })

          :generation ->
            {:ok, %{value: bucket}} = Backend.get("bucket:#{state.bucket}")
            :ok = Backend.put("bucket:#{state.bucket}", Map.put(bucket, :generation, "recreated"))

          {:error, reason} ->
            throw({:backend_error, reason})
        end
      end

      Backend.transaction(spec, Keyword.delete(opts, :engine))
    catch
      {:backend_error, reason} -> {:error, reason}
    end
  end

  for legacy? <- [true, false] do
    @legacy legacy?
    test "unrelated index compare loss retries abort for legacy=#{legacy?}" do
      {bucket, upload_id, key} = fixture(@legacy)
      opts = interleaving(bucket, key, :unrelated, 1)
      assert :ok = MultipartCommit.abort_upload(bucket, upload_id, opts)
      assert Agent.get(opts[:engine], & &1.attempts) == 2
      assert {:ok, %{value: %{status: :aborted}}} = Backend.get(key)
      assert {:ok, %{active_upload_count: 1}} = ListIndex.status(bucket)
    end
  end

  for status <- [:completing, :aborted] do
    @status status
    test "a concurrent same-upload #{@status} transition remains a real conflict" do
      {bucket, upload_id, key} = fixture(true)
      opts = interleaving(bucket, key, @status, 1)

      assert {:error, :multipart_completion_conflict} =
               MultipartCommit.abort_upload(bucket, upload_id, opts)

      assert Agent.get(opts[:engine], & &1.attempts) == 1
      assert {:ok, %{value: %{status: @status}}} = Backend.get(key)
    end
  end

  test "unrelated compare exhaustion reports availability and leaves the target writable" do
    {bucket, upload_id, key} = fixture(true)
    opts = interleaving(bucket, key, :unrelated, 8)

    assert {:error, :metadata_quorum_unavailable} =
             MultipartCommit.abort_upload(bucket, upload_id, opts)

    assert Agent.get(opts[:engine], & &1.attempts) == 8
    assert {:ok, %{value: %{status: :initiated}}} = Backend.get(key)
    assert {:ok, %{active_upload_count: 9}} = ListIndex.status(bucket)
  end

  test "a recreated bucket generation cannot inherit an old upload abort" do
    {bucket, upload_id, key} = fixture(true)
    opts = interleaving(bucket, key, :generation, 1)

    assert {:error, :bucket_generation_changed} =
             MultipartCommit.abort_upload(bucket, upload_id, opts)

    assert {:ok, %{value: %{status: :initiated}}} = Backend.get(key)
  end

  test "backend errors propagate without compare retries or cleanup" do
    {bucket, upload_id, key} = fixture(true)
    opts = interleaving(bucket, key, {:error, :injected_failure}, 1)
    assert {:error, :injected_failure} = MultipartCommit.abort_upload(bucket, upload_id, opts)
    assert Agent.get(opts[:engine], & &1.attempts) == 1
    assert {:ok, %{value: %{status: :initiated}}} = Backend.get(key)
  end

  test "public abort cleans parts only after its metadata transition succeeds" do
    {bucket, upload_id, key} = fixture(true)
    assert {:ok, _} = Multipart.store_part(bucket, upload_id, 1, "part bytes")
    assert :ok = Multipart.abort_upload(bucket, upload_id)
    assert {:ok, %{value: %{status: :aborted, parts_count: 0}}} = Backend.get(key)
    assert {:ok, nil} = Backend.get("mpu_part:#{bucket}:#{upload_id}:1")
    assert {:error, :not_found} = Multipart.get_upload(bucket, upload_id)
    assert {:error, :multipart_completion_conflict} = Multipart.abort_upload(bucket, upload_id)
  end

  defp fixture(legacy?) do
    bucket = "abort-retry-#{System.unique_integer([:positive, :monotonic])}"
    :ok = Metadata.create_bucket(bucket)

    if legacy? do
      {:ok, upload_id} = Multipart.init_upload(bucket, "target")
      {bucket, upload_id, "mpu:#{bucket}:#{upload_id}"}
    else
      upload_id = "v2-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, %{value: metadata}} = Backend.get("bucket:#{bucket}")

      upload = %{
        bucket: bucket,
        key: "target",
        upload_id: upload_id,
        bucket_generation: BucketLifecycle.generation(metadata),
        status: :initiated,
        created_at: "2026-10-18T00:00:00Z",
        updated_at: "2026-10-18T00:00:00Z"
      }

      key = Keys.multipart_upload(upload_id)

      {:ok, %{succeeded: true}} =
        ListIndex.transaction(Backend, bucket, %{
          compare: [],
          success: [{:put, key, upload, %{}}],
          failure: []
        })

      {bucket, upload_id, key}
    end
  end

  defp interleaving(bucket, key, change, remaining) do
    {:ok, engine} =
      Agent.start_link(fn ->
        %{bucket: bucket, key: key, change: change, remaining: remaining, attempts: 0}
      end)

    [backend: InterleavingBackend, engine: engine]
  end
end
