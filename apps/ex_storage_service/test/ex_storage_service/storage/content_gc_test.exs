defmodule ExStorageService.Storage.ContentGCTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.CAS
  alias ExStorageService.Storage.ContentGC

  defmodule FailingBackend do
    def get_all, do: {:error, :metadata_unavailable}
  end

  test "GC never touches blobs under the reserved cas/ root" do
    data = "gc-must-not-touch-#{System.unique_integer()}"
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    path = CAS.blob_path(hash)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, data)

    # Backdate far past the orphan grace window (600s); the blob has no
    # obj:/obj_ver: metadata, so under the legacy rules it would look
    # like a deletable orphan.
    old = System.os_time(:second) - 24 * 3600
    File.touch!(path, old)

    assert {:ok, _deleted} = ContentGC.run_now()

    assert File.exists?(path), "ContentGC must not delete global CAS blobs"
  end

  @tag :tmp_dir
  test "metadata failures abort the sweep instead of treating every blob as orphaned", %{
    tmp_dir: tmp_dir
  } do
    path = legacy_blob(tmp_dir, "bucket", String.duplicate("a", 64), "referenced")
    File.touch!(path, System.os_time(:second) - 172_800)

    assert {:error, :metadata_unavailable} =
             ContentGC.run_once(
               data_root: tmp_dir,
               backend: FailingBackend,
               orphan_grace_seconds: 1
             )

    assert File.read!(path) == "referenced"
  end

  @tag :tmp_dir
  test "configured orphan grace protects recent legacy content", %{tmp_dir: tmp_dir} do
    hash = String.duplicate("b", 64)
    path = legacy_blob(tmp_dir, "bucket", hash, "recent")
    File.touch!(path, System.os_time(:second) - 120)

    assert {:ok, 0} =
             ContentGC.run_once(data_root: tmp_dir, orphan_grace_seconds: 600)

    assert File.read!(path) == "recent"
  end

  defp legacy_blob(root, bucket, <<prefix::binary-size(2), rest::binary>>, data) do
    path = Path.join([root, bucket, "objects", prefix, rest])
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, data)
    path
  end
end
