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

  alias ExStorageServiceS3.Handlers.{
    Bucket,
    Cors,
    Health,
    Lifecycle,
    Location,
    Notification,
    Object,
    Tagging,
    Versioning
  }

  alias ExStorageServiceS3.MultipartHandlers
  alias ExStorageServiceS3.Presigned

  plug :assign_request_id
  plug :fetch_query
  plug ExStorageServiceS3.Plugs.Cors
  plug :check_presigned_auth
  plug ExStorageServiceS3.Auth.SigV4
  plug ExStorageServiceS3.Plugs.RateLimiter
  plug :resolve_operation
  plug ExStorageServiceS3.Plugs.Authorize
  plug ExStorageServiceS3.Plugs.ModeGuard
  plug ExStorageServiceS3.Plugs.ReadResponse
  plug :match
  plug :dispatch

  # Health check endpoint
  get "/health" do
    Health.live(conn)
  end

  get "/health/ready" do
    Health.ready(conn)
  end

  get "/health/status" do
    Health.status(conn)
  end

  # Dev-only: POST /health/cloud_cache — configure cloud cache inline
  if Code.ensure_loaded?(Mix) and Mix.env() == :dev do
    post "/health/cloud_cache" do
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      params = JSON.decode!(body)
      bucket = params["local_bucket"] || params["bucket"]

      result =
        ExStorageService.CloudCache.Config.set_config(bucket, %{
          enabled: true,
          provider: String.to_atom(params["provider"] || "minio"),
          endpoint: params["endpoint"],
          region: params["region"] || "us-east-1",
          bucket: params["remote_bucket"] || params["bucket"],
          access_key_id: params["access_key_id"],
          secret_access_key: params["secret_access_key"],
          cache_max_bytes: params["cache_max_bytes"] || 10_737_418_240,
          cache_enabled: true
        })

      body = JSON.encode!(%{result: inspect(result)})

      conn
      |> put_resp_header("content-type", "application/json")
      |> send_resp(200, body)
    end
  end

  # Authorization and dispatch consume the same validated operation identity.
  match _ do
    dispatch_operation(conn, conn.assigns.s3_operation)
  end

  defp resolve_operation(%{request_path: path} = conn, _opts)
       when path in ["/health", "/health/ready", "/health/status", "/health/cloud_cache"],
       do: conn

  defp resolve_operation(conn, _opts) do
    case ExStorageServiceS3.Operation.resolve(conn) do
      {:ok, operation} ->
        assign(conn, :s3_operation, operation)

      {:error, code, message} ->
        conn
        |> ExStorageServiceS3.Handlers.Shared.error_response(
          code,
          message,
          conn.request_path,
          conn.assigns.request_id
        )
        |> halt()
    end
  end

  defp dispatch_operation(conn, %{name: :list_buckets}), do: Bucket.list_buckets(conn)

  defp dispatch_operation(conn, %{name: :create_bucket, bucket: bucket}),
    do: Bucket.create_bucket(conn, bucket)

  defp dispatch_operation(conn, %{name: :delete_bucket, bucket: bucket}),
    do: Bucket.delete_bucket(conn, bucket)

  defp dispatch_operation(conn, %{name: :head_bucket, bucket: bucket}),
    do: Bucket.head_bucket(conn, bucket)

  defp dispatch_operation(conn, %{name: :list_objects, bucket: bucket}),
    do: Object.list_objects(conn, bucket)

  defp dispatch_operation(conn, %{name: :list_object_versions, bucket: bucket}),
    do: ExStorageServiceS3.Handlers.Versions.list_object_versions(conn, bucket)

  defp dispatch_operation(conn, %{name: :list_multipart_uploads, bucket: bucket}),
    do: MultipartHandlers.list_multipart_uploads(conn, bucket)

  defp dispatch_operation(conn, %{name: :get_bucket_location, bucket: bucket}),
    do: Location.get_bucket_location(conn, bucket)

  for {name, handler, function} <- [
        {:get_bucket_cors, Cors, :get_bucket_cors},
        {:put_bucket_cors, Cors, :put_bucket_cors},
        {:delete_bucket_cors, Cors, :delete_bucket_cors},
        {:get_bucket_tagging, Tagging, :get_bucket_tagging},
        {:put_bucket_tagging, Tagging, :put_bucket_tagging},
        {:delete_bucket_tagging, Tagging, :delete_bucket_tagging}
      ] do
    defp dispatch_operation(conn, %{name: unquote(name), bucket: bucket}),
      do: unquote(handler).unquote(function)(conn, bucket)
  end

  for {name, function} <- [
        {:get_object_tagging, :get_object_tagging},
        {:put_object_tagging, :put_object_tagging},
        {:delete_object_tagging, :delete_object_tagging}
      ] do
    defp dispatch_operation(conn, %{name: unquote(name), bucket: bucket, key: key}),
      do: Tagging.unquote(function)(conn, bucket, key)
  end

  defp dispatch_operation(conn, %{name: :delete_objects, bucket: bucket}),
    do: Object.delete_objects(conn, bucket)

  defp dispatch_operation(conn, %{name: :get_bucket_versioning, bucket: bucket}),
    do: Versioning.get_bucket_versioning(conn, bucket)

  defp dispatch_operation(conn, %{name: :put_bucket_versioning, bucket: bucket}),
    do: Versioning.put_bucket_versioning(conn, bucket)

  defp dispatch_operation(conn, %{name: :get_bucket_lifecycle, bucket: bucket}),
    do: Lifecycle.get_bucket_lifecycle(conn, bucket)

  defp dispatch_operation(conn, %{name: :put_bucket_lifecycle, bucket: bucket}),
    do: Lifecycle.put_bucket_lifecycle(conn, bucket)

  defp dispatch_operation(conn, %{name: :delete_bucket_lifecycle, bucket: bucket}),
    do: Lifecycle.delete_bucket_lifecycle(conn, bucket)

  defp dispatch_operation(conn, %{name: :get_bucket_notification, bucket: bucket}),
    do: Notification.get_bucket_notification(conn, bucket)

  defp dispatch_operation(conn, %{name: :put_bucket_notification, bucket: bucket}),
    do: Notification.put_bucket_notification(conn, bucket)

  defp dispatch_operation(conn, %{name: :delete_bucket_notification, bucket: bucket}),
    do: Notification.delete_bucket_notification(conn, bucket)

  defp dispatch_operation(conn, %{name: :get_object, bucket: bucket, key: key}),
    do: Object.get_object(conn, bucket, key)

  defp dispatch_operation(conn, %{name: :head_object, bucket: bucket, key: key}),
    do: Object.head_object(conn, bucket, key)

  defp dispatch_operation(conn, %{name: :put_object, bucket: bucket, key: key}),
    do: Object.put_object(conn, bucket, key)

  defp dispatch_operation(conn, %{name: :copy_object, bucket: bucket, key: key}),
    do: Object.copy_object(conn, bucket, key)

  defp dispatch_operation(conn, %{name: :delete_object, bucket: bucket, key: key}),
    do: Object.delete_object(conn, bucket, key)

  defp dispatch_operation(conn, %{name: :create_multipart_upload, bucket: bucket, key: key}),
    do: MultipartHandlers.create_multipart_upload(conn, bucket, key)

  defp dispatch_operation(conn, %{name: :upload_part, bucket: bucket, key: key}),
    do: MultipartHandlers.upload_part(conn, bucket, key)

  defp dispatch_operation(conn, %{name: :upload_part_copy, bucket: bucket, key: key}),
    do: MultipartHandlers.upload_part_copy(conn, bucket, key)

  defp dispatch_operation(conn, %{name: :complete_multipart_upload, bucket: bucket, key: key}),
    do: MultipartHandlers.complete_multipart_upload(conn, bucket, key)

  defp dispatch_operation(conn, %{name: :abort_multipart_upload, bucket: bucket, key: key}),
    do: MultipartHandlers.abort_multipart_upload(conn, bucket, key)

  defp dispatch_operation(conn, %{name: :list_parts, bucket: bucket, key: key}),
    do: MultipartHandlers.list_parts(conn, bucket, key)

  defp dispatch_operation(conn, %{
         name: :get_object_version,
         bucket: bucket,
         key: key,
         version_id: version
       }),
       do: Object.get_object_version(conn, bucket, key, version)

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
      # Pre-signed URL request — validate signature and resolve identity.
      # The signature establishes *who* is making the request; IAM (Authorize plug)
      # then checks *whether* they are allowed to perform the operation.
      get_secret_fn = fn access_key_id ->
        case ExStorageService.IAM.AccessKey.lookup_by_access_key_id(access_key_id) do
          {:ok, %{secret_access_key: secret, status: :active}} -> secret
          _ -> nil
        end
      end

      case Presigned.validate_presigned(conn, get_secret_fn) do
        {:ok, conn} ->
          # Resolve user from the access key embedded in the presigned credential
          case get_presigned_identity(conn) do
            {:ok, {user_id, access_key_id}} ->
              case ExStorageService.IAM.User.get_user(user_id) do
                {:ok, %{status: :active}} ->
                  conn
                  |> Plug.Conn.assign(:presigned_auth, true)
                  |> Plug.Conn.assign(:user_id, user_id)
                  |> Plug.Conn.assign(:access_key_id, access_key_id)

                {:ok, %{status: :suspended}} ->
                  presigned_error(conn, "User account is suspended")

                _ ->
                  presigned_error(conn, "InvalidAccessKeyId")
              end

            {:error, reason} ->
              presigned_error(conn, reason)
          end

        {:error, reason} ->
          presigned_error(conn, reason)
      end
    else
      conn
    end
  end

  # Extract the access key ID from the X-Amz-Credential query param and
  # resolve it to a (user_id, access_key_id) pair.
  defp get_presigned_identity(conn) do
    params = conn.query_params

    case Map.fetch(params, "X-Amz-Credential") do
      {:ok, credential_str} ->
        case String.split(credential_str, "/") do
          [access_key_id | _] ->
            case ExStorageService.IAM.AccessKey.lookup_by_access_key_id(access_key_id) do
              {:ok, %{user_id: user_id, status: :active}} ->
                {:ok, {user_id, access_key_id}}

              {:ok, %{status: :inactive}} ->
                {:error, "Access key is inactive"}

              _ ->
                {:error, "InvalidAccessKeyId"}
            end

          _ ->
            {:error, "Malformed X-Amz-Credential"}
        end

      :error ->
        {:error, "Missing X-Amz-Credential"}
    end
  end

  defp presigned_error(conn, reason) do
    request_id = conn.assigns[:request_id] || generate_request_id()

    conn
    |> ExStorageServiceS3.Handlers.Shared.error_response(
      "AccessDenied",
      reason,
      conn.request_path,
      request_id
    )
    |> Plug.Conn.halt()
  end

  defp generate_request_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)
  end

  @impl Plug.ErrorHandler
  def handle_errors(conn, %{kind: _kind, reason: _reason, stack: _stack}) do
    request_id = conn.assigns[:request_id] || generate_request_id()

    ExStorageServiceS3.Handlers.Shared.error_response(
      conn,
      "InternalError",
      "An internal server error occurred.",
      conn.request_path,
      request_id
    )
  end
end
