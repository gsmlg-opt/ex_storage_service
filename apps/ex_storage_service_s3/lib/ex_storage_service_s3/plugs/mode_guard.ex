defmodule ExStorageServiceS3.Plugs.ModeGuard do
  @moduledoc "Rejects unsupported gateway combinations before any mutation."
  @behaviour Plug
  import Plug.Conn
  alias ExStorageService.CloudCache.Config
  alias ExStorageServiceS3.Handlers.Shared

  @hosted_only ~w(get_object_version list_object_versions get_bucket_versioning put_bucket_versioning create_multipart_upload upload_part upload_part_copy complete_multipart_upload abort_multipart_upload list_parts list_multipart_uploads get_object_tagging put_object_tagging delete_object_tagging)a
  def init(opts), do: opts

  def call(%{assigns: %{s3_operation: %{bucket: bucket} = operation}} = conn, _opts)
      when is_binary(bucket) do
    case Config.get_config(bucket) do
      {:ok, %{enabled: true}} ->
        gateway(conn, operation)

      {:ok, _} ->
        conn

      {:error, :not_found} ->
        conn

      {:error, reason} ->
        conn
        |> Shared.storage_error_response(reason, conn.request_path, Shared.request_id(conn))
        |> halt()
    end
  end

  def call(conn, _opts), do: conn

  defp gateway(conn, operation) do
    cond do
      operation.name in @hosted_only or operation.version_id != nil ->
        reject(
          conn,
          "NotImplemented",
          "The requested operation is unavailable for gateway buckets."
        )

      operation.name in [:put_object, :copy_object, :delete_object, :delete_objects] and
          cluster?() ->
        reject(
          conn,
          "ServiceUnavailable",
          "Cloud-backed writes are unavailable in active cluster mode."
        )

      true ->
        conn
    end
  end

  defp cluster? do
    case ExStorageService.Context.default() do
      {:ok, %{config: %{mode: :cluster}}} -> true
      _ -> false
    end
  end

  defp reject(conn, code, message) do
    conn
    |> Shared.error_response(code, message, conn.request_path, Shared.request_id(conn))
    |> halt()
  end
end
