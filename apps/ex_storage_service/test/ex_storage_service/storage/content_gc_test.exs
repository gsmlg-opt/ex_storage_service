defmodule ExStorageService.Storage.ContentGCTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.CAS
  alias ExStorageService.Storage.ContentGC

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
end
