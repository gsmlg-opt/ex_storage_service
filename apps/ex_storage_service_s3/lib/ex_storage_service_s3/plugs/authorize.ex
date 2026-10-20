defmodule ExStorageServiceS3.Plugs.Authorize do
  @moduledoc """
  Authorization plug for S3 API requests.

  Maps HTTP method + path to S3 actions and evaluates the user's IAM policies.
  If auth is disabled, all requests are allowed.
  If the user is the root admin, all requests are allowed.
  Otherwise, Policy.evaluate/3 determines access.

  Presigned URL requests are no longer exempt from IAM checks. The signature
  verifies *identity*; the IAM policy verifies *authorization*.
  """

  import Plug.Conn
  alias ExStorageService.IAM.Policy

  @behaviour Plug

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, _opts) do
    cond do
      public_health?(conn) ->
        conn

      auth_enabled?() ->
        authorize_user(conn)

      true ->
        conn
    end
  end

  defp public_health?(%Plug.Conn{method: "GET", request_path: path}),
    do: path in ["/health", "/health/ready", "/health/status"]

  defp public_health?(_conn), do: false

  defp authorize_user(%{assigns: %{user_id: "root"}} = conn), do: conn

  defp authorize_user(conn) do
    case conn.assigns[:s3_operation] do
      %{name: :delete_objects} ->
        # The control document is authorized entry by entry after bounded parsing.
        conn

      %{name: _} = operation ->
        {action, legacy_action} = permission(operation)
        resource = resource_arn(operation.bucket, operation.key)

        if allowed?(conn, action, resource, legacy_action) and source_allowed?(conn, operation) and
             upload_tags_allowed?(conn, operation) do
          conn
        else
          deny(conn)
        end

      _ ->
        deny(conn)
    end
  end

  @doc """
  Checks an individual resource using the authenticated request identity.
  Batch handlers must call this for every entry, including explicit versions.
  """
  def allowed?(conn, action, resource, legacy_action \\ nil) do
    not auth_enabled?() or conn.assigns[:user_id] == "root" or
      Policy.evaluate(conn.assigns[:user_id], action, resource,
        legacy_action: legacy_action || action
      ) == :allow
  end

  @doc false
  def authorize_copy_tags(_conn, _bucket, _key, tags) when is_map(tags) and map_size(tags) == 0,
    do: :ok

  def authorize_copy_tags(conn, bucket, key, tags) when is_map(tags) do
    if allowed?(conn, "s3:PutObjectTagging", resource_arn(bucket, key)),
      do: :ok,
      else: {:error, :access_denied}
  end

  defp upload_tags_allowed?(conn, %{name: name} = operation)
       when name in [:put_object, :create_multipart_upload] do
    get_req_header(conn, "x-amz-tagging") == [] or
      allowed?(conn, "s3:PutObjectTagging", resource_arn(operation.bucket, operation.key))
  end

  defp upload_tags_allowed?(_conn, _operation), do: true

  defp source_allowed?(conn, %{copy_source: %{bucket: bucket, key: key} = source}) do
    action = if source.version_id, do: "s3:GetObjectVersion", else: "s3:GetObject"
    allowed?(conn, action, resource_arn(bucket, key), "s3:GetObject")
  end

  defp source_allowed?(_conn, _operation), do: true

  @doc """
  Returns the AWS policy action for a resolved operation, retaining an explicit
  legacy operation alias for old stored policies.
  """
  def permission(%{name: name} = operation) do
    case name do
      :list_buckets ->
        {"s3:ListAllMyBuckets", "s3:ListAllMyBuckets"}

      :list_objects ->
        {"s3:ListBucket", "s3:ListBucket"}

      :head_bucket ->
        {"s3:ListBucket", "s3:HeadBucket"}

      :create_bucket ->
        {"s3:CreateBucket", "s3:CreateBucket"}

      :delete_bucket ->
        {"s3:DeleteBucket", "s3:DeleteBucket"}

      :get_bucket_versioning ->
        {"s3:GetBucketVersioning", "s3:GetBucketVersioning"}

      :put_bucket_versioning ->
        {"s3:PutBucketVersioning", "s3:PutBucketVersioning"}

      :get_bucket_lifecycle ->
        {"s3:GetLifecycleConfiguration", "s3:GetLifecycleConfiguration"}

      name when name in [:put_bucket_lifecycle, :delete_bucket_lifecycle] ->
        {"s3:PutLifecycleConfiguration", "s3:PutLifecycleConfiguration"}

      :get_bucket_notification ->
        {"s3:GetBucketNotification", "s3:GetBucketNotification"}

      name when name in [:put_bucket_notification, :delete_bucket_notification] ->
        {"s3:PutBucketNotification", "s3:PutBucketNotification"}

      :get_bucket_location ->
        {"s3:GetBucketLocation", "s3:Unknown"}

      :list_object_versions ->
        {"s3:ListBucketVersions", "s3:ListBucketVersions"}

      :list_multipart_uploads ->
        {"s3:ListBucketMultipartUploads", "s3:Unknown"}

      :get_bucket_cors ->
        {"s3:GetBucketCORS", "s3:Unknown"}

      name when name in [:put_bucket_cors, :delete_bucket_cors] ->
        {"s3:PutBucketCORS", "s3:Unknown"}

      :get_bucket_tagging ->
        {"s3:GetBucketTagging", "s3:Unknown"}

      name when name in [:put_bucket_tagging, :delete_bucket_tagging] ->
        {"s3:PutBucketTagging", "s3:Unknown"}

      :get_object ->
        {"s3:GetObject", "s3:GetObject"}

      :get_object_version ->
        {"s3:GetObjectVersion", "s3:GetObject"}

      :head_object ->
        {version_action(operation, "GetObject", "GetObjectVersion"), "s3:HeadObject"}

      name when name in [:put_object, :copy_object] ->
        {"s3:PutObject", "s3:PutObject"}

      :delete_object ->
        {version_action(operation, "DeleteObject", "DeleteObjectVersion"), "s3:DeleteObject"}

      :get_object_tagging ->
        {version_action(operation, "GetObjectTagging", "GetObjectVersionTagging"), "s3:Unknown"}

      name when name in [:put_object_tagging, :delete_object_tagging] ->
        prefix = if name == :put_object_tagging, do: "Put", else: "Delete"

        {version_action(operation, "#{prefix}ObjectTagging", "#{prefix}ObjectVersionTagging"),
         "s3:Unknown"}

      :create_multipart_upload ->
        {"s3:PutObject", "s3:CreateMultipartUpload"}

      name when name in [:upload_part, :upload_part_copy] ->
        {"s3:PutObject", "s3:UploadPart"}

      :complete_multipart_upload ->
        {"s3:PutObject", "s3:CompleteMultipartUpload"}

      :abort_multipart_upload ->
        {"s3:AbortMultipartUpload", "s3:AbortMultipartUpload"}

      :list_parts ->
        {"s3:ListMultipartUploadParts", "s3:ListMultipartUploadParts"}

      :delete_objects ->
        {"s3:DeleteObject", "s3:DeleteObject"}

      _ ->
        {"s3:Unknown", "s3:Unknown"}
    end
  end

  defp version_action(operation, current, version),
    do: "s3:" <> if(Map.get(operation, :version_id), do: version, else: current)

  @doc """
  Maps an HTTP request to its AWS S3 policy action using the shared resolver.
  Unsupported requests return `s3:Unknown` and cannot reach dispatch.
  """
  def map_action(method, path_info, query_params \\ %{}) do
    conn = %Plug.Conn{
      method: method,
      request_path: "/" <> Enum.join(path_info, "/"),
      query_string: URI.encode_query(query_params)
    }

    case ExStorageServiceS3.Operation.resolve(conn) do
      {:ok, operation} -> operation |> permission() |> elem(0)
      {:error, _, _} -> "s3:Unknown"
    end
  end

  @doc "Builds a canonical S3 ARN from decoded path segments."
  @spec build_resource_arn([String.t()]) :: String.t()
  def build_resource_arn([]), do: "arn:aws:s3:::*"
  def build_resource_arn([bucket]), do: resource_arn(bucket, nil)

  def build_resource_arn([bucket | key_parts]),
    do: resource_arn(bucket, Enum.join(key_parts, "/"))

  @doc false
  def resource_arn(nil, nil), do: "arn:aws:s3:::*"
  def resource_arn(bucket, nil), do: "arn:aws:s3:::#{bucket}"
  def resource_arn(bucket, key), do: "arn:aws:s3:::#{bucket}/#{key}"

  defp deny(conn) do
    request_id =
      conn.assigns[:request_id] ||
        :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)

    body =
      ExStorageServiceS3.XML.error_response(
        "AccessDenied",
        "Access Denied",
        conn.request_path,
        request_id
      )

    conn
    |> put_resp_header("content-type", "application/xml")
    |> put_resp_header("x-amz-request-id", request_id)
    |> send_resp(403, if(conn.method == "HEAD", do: "", else: body))
    |> halt()
  end

  defp auth_enabled? do
    Application.get_env(:ex_storage_service, :s3_auth_enabled, false) == true
  end
end
