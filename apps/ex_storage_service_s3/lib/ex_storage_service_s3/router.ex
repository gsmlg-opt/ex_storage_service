defmodule ExStorageServiceS3.Router do
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

  alias ExStorageServiceS3.Handlers
  alias ExStorageServiceS3.MultipartHandlers
  alias ExStorageServiceS3.Presigned

  plug :assign_request_id
  plug :fetch_query
  plug :check_presigned_auth
  plug ExStorageServiceS3.Auth.SigV4
  plug ExStorageServiceS3.Plugs.RateLimiter
  plug ExStorageServiceS3.Plugs.Authorize
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

  # PUT /:bucket - CreateBucket, or bucket config operations
  put "/:bucket" do
    params = conn.query_params

    cond do
      Map.has_key?(params, "versioning") ->
        Handlers.put_bucket_versioning(conn, bucket)

      Map.has_key?(params, "lifecycle") ->
        Handlers.put_bucket_lifecycle(conn, bucket)

      Map.has_key?(params, "notification") ->
        Handlers.put_bucket_notification(conn, bucket)

      true ->
        Handlers.create_bucket(conn, bucket)
    end
  end

  # DELETE /:bucket - DeleteBucket, DeleteObjects, or config deletions
  delete "/:bucket" do
    params = conn.query_params

    cond do
      Map.has_key?(params, "lifecycle") ->
        Handlers.delete_bucket_lifecycle(conn, bucket)

      Map.has_key?(params, "notification") ->
        Handlers.delete_bucket_notification(conn, bucket)

      true ->
        Handlers.delete_bucket(conn, bucket)
    end
  end

  # HEAD /:bucket - HeadBucket
  head "/:bucket" do
    Handlers.head_bucket(conn, bucket)
  end

  # GET /:bucket - ListObjectsV2, or GET /:bucket/*key - GetObject / ListParts
  # We use a forward match for the bucket, then check for key segments.
  get "/:bucket/*key" do
    params = conn.query_params

    case key do
      [] ->
        cond do
          Map.has_key?(params, "versioning") ->
            Handlers.get_bucket_versioning(conn, bucket)

          Map.has_key?(params, "lifecycle") ->
            Handlers.get_bucket_lifecycle(conn, bucket)

          Map.has_key?(params, "notification") ->
            Handlers.get_bucket_notification(conn, bucket)

          true ->
            Handlers.list_objects(conn, bucket)
        end

      key_parts ->
        object_key = Enum.join(key_parts, "/")

        cond do
          Map.has_key?(params, "uploadId") ->
            MultipartHandlers.list_parts(conn, bucket, object_key)

          Map.has_key?(params, "versionId") ->
            Handlers.get_object_version(conn, bucket, object_key, params["versionId"])

          true ->
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
            ExStorageServiceS3.XML.error_response(
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
              ExStorageServiceS3.XML.error_response(
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
      ExStorageServiceS3.XML.error_response(
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

  # Check for pre-signed URL authentication via query parameters
  defp check_presigned_auth(conn, _opts) do
    if Map.has_key?(conn.query_params, "X-Amz-Signature") do
      # Pre-signed URL request — validate signature
      get_secret_fn = fn access_key_id ->
        case ExStorageService.IAM.AccessKey.lookup_by_access_key_id(access_key_id) do
          {:ok, %{secret_access_key: secret, status: :active}} -> secret
          _ -> nil
        end
      end

      case Presigned.validate_presigned(conn, get_secret_fn) do
        {:ok, conn} ->
          # Mark as presigned-authenticated, skip further auth
          Plug.Conn.assign(conn, :presigned_auth, true)

        {:error, reason} ->
          request_id = conn.assigns[:request_id] || generate_request_id()

          body =
            ExStorageServiceS3.XML.error_response(
              "AccessDenied",
              reason,
              conn.request_path,
              request_id
            )

          conn
          |> Plug.Conn.put_resp_header("content-type", "application/xml")
          |> Plug.Conn.put_resp_header("x-amz-request-id", request_id)
          |> Plug.Conn.send_resp(403, body)
          |> Plug.Conn.halt()
      end
    else
      conn
    end
  end

  defp generate_request_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)
  end

  @impl Plug.ErrorHandler
  def handle_errors(conn, %{kind: _kind, reason: reason, stack: _stack}) do
    request_id = conn.assigns[:request_id] || generate_request_id()

    body =
      ExStorageServiceS3.XML.error_response(
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
