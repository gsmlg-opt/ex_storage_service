defmodule ExStorageServiceS3.Handlers.Location do
  @moduledoc false
  import Plug.Conn
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageService.Metadata
  alias ExStorageServiceS3.XML

  def region, do: Application.get_env(:ex_storage_service, :s3_region, "us-east-1")

  def get_bucket_location(conn, bucket) do
    id = request_id(conn)

    case Metadata.head_bucket(bucket) do
      :ok ->
        location = if region() == "us-east-1", do: "", else: XML.escape(region())

        body =
          ~s(<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">#{location}</LocationConstraint>)

        conn |> put_resp_header("x-amz-bucket-region", region()) |> xml_response(200, body, id)

      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchBucket",
          "The specified bucket does not exist.",
          "/#{bucket}",
          id
        )

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}", id)
    end
  end
end
