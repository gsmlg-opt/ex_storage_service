defmodule ExStorageService.Storage.Manifest do
  @moduledoc """
  Immutable, content-addressed manifests describing multipart objects.

  A manifest lists the CAS part blobs a multipart object was assembled
  from. It is stored twice: a canonical JSON file at
  `{data_root}/cas/manifests/sha256/{p2}/{rest}` (the content the hash
  addresses) and a `manifest:sha256:{hash}` Concord record for fast reads.

  Serving does not use manifests (completed multipart objects are
  materialized as whole CAS blobs so Content-Length/Range/sendfile
  semantics are preserved); manifests exist for audit, repair, and future
  replication/pack phases.

  The canonical form is a JSON array — arrays are order-stable, Elixir
  map key order is not:

      ["ess-manifest-v1", etag, total_size, [[number, hash, size, etag], ...]]
  """

  alias ExStorageService.Storage.CAS

  @format "ess-manifest-v1"

  def manifest_path(manifest_hash) do
    <<prefix::binary-size(2), rest::binary>> = manifest_hash
    Path.join([CAS.data_root(), CAS.reserved_root(), "manifests", "sha256", prefix, rest])
  end

  def create_manifest(parts, total_size, etag) do
    sorted = Enum.sort_by(parts, & &1.number)

    canonical =
      JSON.encode!([
        @format,
        etag,
        total_size,
        Enum.map(sorted, fn p -> [p.number, p.hash, p.size, p.etag] end)
      ])

    manifest_hash = Base.encode16(:crypto.hash(:sha256, canonical), case: :lower)

    write_manifest_file(manifest_hash, canonical)

    record = %{
      hash: "sha256:#{manifest_hash}",
      format: @format,
      parts: sorted,
      total_size: total_size,
      etag: etag,
      created_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }

    case Concord.put("manifest:sha256:#{manifest_hash}", record) do
      :ok -> {:ok, manifest_hash}
      {:ok, _} -> {:ok, manifest_hash}
      error -> error
    end
  end

  def get_manifest(manifest_hash) do
    case Concord.get("manifest:sha256:#{manifest_hash}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, record} -> {:ok, record}
      error -> error
    end
  end

  defp write_manifest_file(manifest_hash, canonical) do
    dest = manifest_path(manifest_hash)

    unless File.exists?(dest) do
      File.mkdir_p!(Path.dirname(dest))
      tmp = dest <> ".tmp-#{:erlang.unique_integer([:positive])}"
      File.write!(tmp, canonical)
      File.rename!(tmp, dest)
    end

    :ok
  end
end
