defmodule ExStorageService.CloudCache.LocalStoreIntegrityTest do
  use ExUnit.Case, async: false
  alias ExStorageService.CloudCache.{Config, LocalStore}
  @moduletag :tmp_dir

  setup %{tmp_dir: directory} do
    previous = Application.get_env(:ex_storage_service, :data_root)
    Application.put_env(:ex_storage_service, :data_root, directory)
    on_exit(fn -> Application.put_env(:ex_storage_service, :data_root, previous) end)
    %{directory: directory}
  end

  test "corrupt, truncated and oversized staged files never publish cache bytes", %{
    directory: directory
  } do
    expected = Base.encode16(:crypto.hash(:sha256, "correct"), case: :lower)
    source = Path.join(directory, "stage")

    for bytes <- ["corrupt", "short", "too long"] do
      File.write!(source, bytes)

      assert {:error, :staged_checksum_mismatch} =
               LocalStore.put_file("bucket", "key", source, expected, 7, %Config{})

      refute Enum.any?(Path.wildcard(Path.join(directory, "bucket/cache/**/*")), &File.regular?/1)
    end
  end
end
