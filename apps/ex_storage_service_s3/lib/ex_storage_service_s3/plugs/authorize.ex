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
      conn.request_path == "/health" ->
        conn

      auth_enabled?() ->
        authorize_user(conn)

      true ->
        conn
    end
  end

  defp authorize_user(conn) do
    user_id = conn.assigns[:user_id]

    # Root admin bypasses all IAM checks
    if user_id == "root" do
      conn
    else
      action = map_action(conn.method, conn.path_info, conn.query_params)
      resource = build_resource_arn(conn.path_info)

      case Policy.evaluate(user_id, action, resource) do
        :allow ->
          conn

        :deny ->
          deny(conn)
      end
    end
  end

  @doc """
  Maps an HTTP method and path segments to an S3 action string.
  Optionally takes query_params to disambiguate operations.
  """
  def map_action(method, path_info, query_params \\ %{}) do
    case {method, path_info} do
      # ── Service-level ───────────────────────────────────────────────────────
      {"GET", []} ->
        "s3:ListAllMyBuckets"

      # ── Bucket-level ─────────────────────────────────────────────────────────
      {"GET", [_bucket]} ->
        cond do
          Map.has_key?(query_params, "versioning") -> "s3:GetBucketVersioning"
          Map.has_key?(query_params, "lifecycle") -> "s3:GetLifecycleConfiguration"
          Map.has_key?(query_params, "notification") -> "s3:GetBucketNotification"
          Map.has_key?(query_params, "versions") -> "s3:ListBucketVersions"
          true -> "s3:ListBucket"
        end

      {"HEAD", [_bucket]} ->
        "s3:HeadBucket"

      {"PUT", [_bucket]} ->
        cond do
          Map.has_key?(query_params, "versioning") -> "s3:PutBucketVersioning"
          Map.has_key?(query_params, "lifecycle") -> "s3:PutLifecycleConfiguration"
          Map.has_key?(query_params, "notification") -> "s3:PutBucketNotification"
          true -> "s3:CreateBucket"
        end

      {"DELETE", [_bucket]} ->
        cond do
          Map.has_key?(query_params, "lifecycle") -> "s3:PutLifecycleConfiguration"
          Map.has_key?(query_params, "notification") -> "s3:PutBucketNotification"
          true -> "s3:DeleteBucket"
        end

      {"POST", [_bucket]} ->
        # Bucket-level POST is only used for DeleteObjects
        "s3:DeleteObject"

      # ── Object-level ─────────────────────────────────────────────────────────
      {"GET", [_bucket | _key]} ->
        if Map.has_key?(query_params, "uploadId") do
          # ListParts
          "s3:ListMultipartUploadParts"
        else
          "s3:GetObject"
        end

      {"HEAD", [_bucket | _key]} ->
        "s3:HeadObject"

      {"PUT", [_bucket | _key]} ->
        cond do
          Map.has_key?(query_params, "partNumber") -> "s3:UploadPart"
          true -> "s3:PutObject"
        end

      {"DELETE", [_bucket | _key]} ->
        if Map.has_key?(query_params, "uploadId") do
          "s3:AbortMultipartUpload"
        else
          "s3:DeleteObject"
        end

      {"POST", [_bucket | _key]} ->
        cond do
          Map.has_key?(query_params, "uploads") -> "s3:CreateMultipartUpload"
          Map.has_key?(query_params, "uploadId") -> "s3:CompleteMultipartUpload"
          true -> "s3:PutObject"
        end

      _ ->
        "s3:Unknown"
    end
  end

  @doc """
  Builds an ARN from path segments.
  Format: arn:ess:::{bucket} or arn:ess:::{bucket}/{key}
  """
  @spec build_resource_arn([String.t()]) :: String.t()
  def build_resource_arn(path_info) do
    case path_info do
      [] -> "arn:ess:::*"
      [bucket] -> "arn:ess:::#{bucket}"
      [bucket | key_parts] -> "arn:ess:::#{bucket}/#{Enum.join(key_parts, "/")}"
    end
  end

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
    |> send_resp(403, body)
    |> halt()
  end

  defp auth_enabled? do
    Application.get_env(:ex_storage_service, :s3_auth_enabled, false)
  end
end
