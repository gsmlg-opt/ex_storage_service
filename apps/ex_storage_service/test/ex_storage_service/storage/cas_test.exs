defmodule ExStorageService.Storage.CASTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.CAS

  defp random_content, do: :crypto.strong_rand_bytes(64)
  defp sha256_hex(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)

  defp write_tmp(data) do
    tmp = CAS.tmp_upload_path()
    File.write!(tmp, data)
    tmp
  end

  test "blob_path/1 shards by first two hex chars under the reserved cas root" do
    hash = sha256_hex("hello")
    <<prefix::binary-size(2), rest::binary>> = hash

    assert CAS.blob_path(hash) ==
             Path.join([CAS.data_root(), "cas", "objects", "sha256", prefix, rest])
  end

  test "commit_blob/2 moves the tmp file into the CAS and has_blob?/1 sees it" do
    data = random_content()
    hash = sha256_hex(data)
    tmp = write_tmp(data)

    refute CAS.has_blob?(hash)
    assert :ok = CAS.commit_blob(tmp, hash)
    assert CAS.has_blob?(hash)
    refute File.exists?(tmp)
    assert File.read!(CAS.blob_path(hash)) == data
  end

  test "commit_blob/2 is idempotent: second commit discards the tmp file" do
    data = random_content()
    hash = sha256_hex(data)

    assert :ok = CAS.commit_blob(write_tmp(data), hash)
    tmp2 = write_tmp(data)
    assert :ok = CAS.commit_blob(tmp2, hash)
    refute File.exists?(tmp2)
    assert File.read!(CAS.blob_path(hash)) == data
  end

  test "verify_blob/1 detects intact, corrupt, and missing blobs" do
    data = random_content()
    hash = sha256_hex(data)
    assert :ok = CAS.commit_blob(write_tmp(data), hash)
    assert :ok = CAS.verify_blob(hash)

    File.write!(CAS.blob_path(hash), "tampered")
    assert {:error, :corrupt} = CAS.verify_blob(hash)

    missing_hash = sha256_hex(random_content())
    assert {:error, :missing} = CAS.verify_blob(missing_hash)
  end
end
