defmodule ExStorageService.S3.Router do
  @moduledoc """
  Main Plug.Router for the S3-compatible API.

  Implements path-style S3 routing where the bucket name is the first
  path segment and the object key follows.

  All responses include standard S3 headers:
    - x-amz-request-id
    - x-amz-id-2
    - Server: ExStorageService
  """

  use Plug.Router
  use Plug.ErrorHandler

  alias ExStorageService.S3.Handlers
  alias ExStorageService.S3.MultipartHandlers

  plug :assign_request_id
  plug :fetch_query
  plug :match
  plug :dispatch

  # Health check endpoint
  get "/health" do
    conn
    |> put_resp_header("content-type", "application/json")
    |> send_resp(200, ~s({"status":"ok"}))
  end

  # GET / - ListBuckets
  get "/" do
    Handlers.list_buckets(conn)
  end

  # PUT /:bucket - CreateBucket (no key segments)
  put "/:bucket" do
    Handlers.create_bucket(conn, bucket)
  end

  # DELETE /:bucket - DeleteBucket or DeleteObjects (multi-delete)
  delete "/:bucket" do
    Handlers.delete_bucket(conn, bucket)
  end

  # HEAD /:bucket - HeadBucket
  head "/:bucket" do
    Handlers.head_bucket(conn, bucket)
  end

  # GET /:bucket - ListObjectsV2, or GET /:bucket/*key - GetObject / ListParts
  # We use a forward match for the bucket, then check for key segments.
  get "/:bucket/*key" do
    case key do
      [] ->
        Handlers.list_objects(conn, bucket)

      key_parts ->
        object_key = Enum.join(key_parts, "/")
        params = conn.query_params

        if Map.has_key?(params, "uploadId") do
          MultipartHandlers.list_parts(conn, bucket, object_key)
        else
          Handlers.get_object(conn, bucket, object_key)
        end
    end
  end

  # HEAD /:bucket/*key - HeadObject
  head "/:bucket/*key" do
    case key do
      [] ->
        Handlers.head_bucket(conn, bucket)

      key_parts ->
        object_key = Enum.join(key_parts, "/")
        Handlers.head_object(conn, bucket, object_key)
    end
  end

  # PUT /:bucket/*key - PutObject, CopyObject, or UploadPart
  put "/:bucket/*key" do
    object_key = Enum.join(key, "/")
    params = conn.query_params

    cond do
      Map.has_key?(params, "partNumber") and Map.has_key?(params, "uploadId") ->
        MultipartHandlers.upload_part(conn, bucket, object_key)

      Plug.Conn.get_req_header(conn, "x-amz-copy-source") != [] ->
        Handlers.copy_object(conn, bucket, object_key)

      true ->
        Handlers.put_object(conn, bucket, object_key)
    end
  end

  # DELETE /:bucket/*key - DeleteObject or AbortMultipartUpload
  delete "/:bucket/*key" do
    object_key = Enum.join(key, "/")
    params = conn.query_params

    if Map.has_key?(params, "uploadId") do
      MultipartHandlers.abort_multipart_upload(conn, bucket, object_key)
    else
      Handlers.delete_object(conn, bucket, object_key)
    end
  end

  # POST /:bucket/*key - CreateMultipartUpload (?uploads) or CompleteMultipartUpload (?uploadId=X)
  # POST /:bucket?delete - DeleteObjects (multi-delete)
  post "/:bucket/*key" do
    params = conn.query_params

    case key do
      [] ->
        # No key segments — bucket-level POST
        if Map.has_key?(params, "delete") do
          Handlers.delete_objects(conn, bucket)
        else
          request_id = conn.assigns[:request_id] || generate_request_id()

          body =
            ExStorageService.S3.XML.error_response(
              "InvalidArgument",
              "Unsupported POST operation.",
              "/#{bucket}",
              request_id
            )

          conn
          |> put_resp_header("content-type", "application/xml")
          |> put_resp_header("x-amz-request-id", request_id)
          |> put_resp_header("x-amz-id-2", request_id)
          |> put_resp_header("server", "ExStorageService")
          |> send_resp(400, body)
        end

      key_parts ->
        object_key = Enum.join(key_parts, "/")

        cond do
          Map.has_key?(params, "uploads") ->
            MultipartHandlers.create_multipart_upload(conn, bucket, object_key)

          Map.has_key?(params, "uploadId") ->
            MultipartHandlers.complete_multipart_upload(conn, bucket, object_key)

          true ->
            request_id = conn.assigns[:request_id] || generate_request_id()

            body =
              ExStorageService.S3.XML.error_response(
                "InvalidArgument",
                "Unsupported POST operation.",
                "/#{bucket}/#{object_key}",
                request_id
              )

            conn
            |> put_resp_header("content-type", "application/xml")
            |> put_resp_header("x-amz-request-id", request_id)
            |> put_resp_header("x-amz-id-2", request_id)
            |> put_resp_header("server", "ExStorageService")
            |> send_resp(400, body)
        end
    end
  end

  # Catch-all for unmatched routes
  match _ do
    request_id = conn.assigns[:request_id] || generate_request_id()

    body =
      ExStorageService.S3.XML.error_response(
        "MethodNotAllowed",
        "The specified method is not allowed against this resource.",
        conn.request_path,
        request_id
      )

    conn
    |> put_resp_header("content-type", "application/xml")
    |> put_resp_header("x-amz-request-id", request_id)
    |> put_resp_header("x-amz-id-2", request_id)
    |> put_resp_header("server", "ExStorageService")
    |> send_resp(405, body)
  end

  # Plug that assigns a unique request ID to the connection
  defp assign_request_id(conn, _opts) do
    request_id = generate_request_id()
    Plug.Conn.assign(conn, :request_id, request_id)
  end

  defp fetch_query(conn, _opts) do
    Plug.Conn.fetch_query_params(conn)
  end

  defp generate_request_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)
  end

  @impl Plug.ErrorHandler
  def handle_errors(conn, %{kind: _kind, reason: reason, stack: _stack}) do
    request_id = conn.assigns[:request_id] || generate_request_id()

    body =
      ExStorageService.S3.XML.error_response(
        "InternalError",
        "Internal server error: #{inspect(reason)}",
        conn.request_path,
        request_id
      )

    conn
    |> Plug.Conn.put_resp_header("content-type", "application/xml")
    |> Plug.Conn.put_resp_header("x-amz-request-id", request_id)
    |> Plug.Conn.put_resp_header("server", "ExStorageService")
    |> Plug.Conn.send_resp(500, body)
  end
end
