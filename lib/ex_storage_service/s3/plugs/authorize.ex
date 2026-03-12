defmodule ExStorageService.S3.Plugs.Authorize do
  @moduledoc """
  Authorization plug for S3 API requests.

  Maps HTTP method + path to S3 actions and evaluates the user's IAM policies.
  If auth is disabled, all requests are allowed.
  If the user is the root admin, all requests are allowed.
  Otherwise, Policy.evaluate/3 determines access.
  """

  import Plug.Conn
  alias ExStorageService.IAM.Policy

  @behaviour Plug

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, _opts) do
    if auth_enabled?() do
      authorize(conn)
    else
      conn
    end
  end

  defp authorize(conn) do
    user_id = conn.assigns[:user_id]

    # Root admin bypass
    if user_id == "root" do
      conn
    else
      action = map_action(conn.method, conn.path_info)
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
  """
  @spec map_action(String.t(), [String.t()]) :: String.t()
  def map_action(method, path_info) do
    case {method, path_info} do
      {"GET", []} -> "s3:ListAllMyBuckets"
      {"GET", [_bucket]} -> "s3:ListBucket"
      {"GET", [_bucket | _key]} -> "s3:GetObject"
      {"HEAD", [_bucket]} -> "s3:HeadBucket"
      {"HEAD", [_bucket | _key]} -> "s3:HeadObject"
      {"PUT", [_bucket]} -> "s3:CreateBucket"
      {"PUT", [_bucket | _key]} -> "s3:PutObject"
      {"DELETE", [_bucket]} -> "s3:DeleteBucket"
      {"DELETE", [_bucket | _key]} -> "s3:DeleteObject"
      {"POST", [_bucket]} -> "s3:DeleteObject"
      _ -> "s3:Unknown"
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
      ExStorageService.S3.XML.error_response(
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
