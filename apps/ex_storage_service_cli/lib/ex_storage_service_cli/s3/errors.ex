defmodule ExStorageServiceCli.S3.Errors do
  @moduledoc "Structured service errors with bounded, redacted diagnostic text."
  alias ExStorageServiceCli.{Context, Error, S3.Response, S3.XML}

  def from_response(operation, %Response{} = response, %Context{} = context) do
    {code, message, region, request_id} =
      case XML.parse(response.body || "", max_bytes: 8_388_608) do
        {:ok, node} ->
          case error_node(node) do
            nil ->
              {"HTTP#{response.status}", "Storage service returned HTTP #{response.status}", nil,
               nil}

            error ->
              {XML.child_text(error, "Code"), XML.child_text(error, "Message"),
               XML.child_text(error, "Region"), XML.child_text(node, "RequestId")}
          end

        _ ->
          {"HTTP#{response.status}", "Storage service returned HTTP #{response.status}", nil, nil}
      end

    code = if code == "", do: "HTTP#{response.status}", else: safe(code, context, 128)
    message = safe(message, context, 1024)

    Error.new(:service, "#{code}: #{message} (HTTP #{response.status})",
      operation: operation,
      code: code,
      http_status: response.status,
      request_id: safe(response.request_id || request_id || "", context, 128),
      details: %{
        bucket_region: region_header(response.headers) || region,
        service_message:
          if(code == "HTTP#{response.status}", do: reason(response.status), else: message)
      },
      retryable: code in ["SlowDown", "InternalError", "ServiceUnavailable", "RequestTimeout"]
    )
  end

  defp error_node(node) do
    case XML.local_name(node.name) do
      "Error" ->
        node

      "ErrorResponse" ->
        case XML.children(node, "Error") do
          [error] -> error
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp reason(400), do: "Bad Request"
  defp reason(401), do: "Unauthorized"
  defp reason(403), do: "Forbidden"
  defp reason(404), do: "Not Found"
  defp reason(405), do: "Method Not Allowed"
  defp reason(409), do: "Conflict"
  defp reason(412), do: "Precondition Failed"
  defp reason(416), do: "Requested Range Not Satisfiable"
  defp reason(429), do: "Too Many Requests"
  defp reason(500), do: "Internal Server Error"
  defp reason(501), do: "Not Implemented"
  defp reason(502), do: "Bad Gateway"
  defp reason(503), do: "Service Unavailable"
  defp reason(504), do: "Gateway Timeout"
  defp reason(status), do: "Storage service returned HTTP #{status}"

  defp region_header(headers) do
    case for {name, value} <- headers,
             String.downcase(name) == "x-amz-bucket-region",
             entry <- List.wrap(value),
             do: entry do
      [region] -> region
      [] -> nil
      _ -> :ambiguous
    end
  end

  defp safe(value, context, length) do
    value =
      Enum.reduce([context.secret_access_key, context.session_token], value, fn
        secret, result when is_binary(secret) and secret != "" ->
          String.replace(result, secret, "[redacted]")

        _, result ->
          result
      end)

    value |> String.replace(~r/[\x00-\x1F\x7F-\x9F]/u, "?") |> String.slice(0, length)
  end
end
