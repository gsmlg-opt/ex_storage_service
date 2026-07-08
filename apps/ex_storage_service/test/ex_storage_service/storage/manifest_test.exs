defmodule ExStorageService.Storage.ManifestTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.Manifest

  defp parts do
    [
      %{number: 2, hash: String.duplicate("b", 64), size: 4, etag: "e2"},
      %{number: 1, hash: String.duplicate("a", 64), size: 5_242_880, etag: "e1"}
    ]
  end

  test "create_manifest is deterministic regardless of part order and idempotent" do
    {:ok, h1} = Manifest.create_manifest(parts(), 5_242_884, "combo-2")
    {:ok, h2} = Manifest.create_manifest(Enum.reverse(parts()), 5_242_884, "combo-2")
    assert h1 == h2

    # content-addressed file exists and hashes to its own name
    path = Manifest.manifest_path(h1)
    assert File.exists?(path)
    assert Base.encode16(:crypto.hash(:sha256, File.read!(path)), case: :lower) == h1
  end

  test "get_manifest returns the record with parts sorted by number" do
    {:ok, hash} = Manifest.create_manifest(parts(), 5_242_884, "combo-2")

    assert {:ok, record} = Manifest.get_manifest(hash)
    assert record.format == "ess-manifest-v1"
    assert record.total_size == 5_242_884
    assert record.etag == "combo-2"
    assert [%{number: 1}, %{number: 2}] = record.parts
  end

  test "get_manifest on unknown hash returns not_found" do
    missing =
      Base.encode16(:crypto.hash(:sha256, "nope-#{System.unique_integer()}"), case: :lower)

    assert {:error, :not_found} = Manifest.get_manifest(missing)
  end
end
