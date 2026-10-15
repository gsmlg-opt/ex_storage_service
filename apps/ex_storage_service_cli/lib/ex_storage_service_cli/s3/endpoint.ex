defmodule ExStorageServiceCli.S3.Endpoint do
  @moduledoc "Basic custom S3 endpoints with explicit path or virtual-hosted addressing."
  alias ExStorageServiceCli.{
    Context,
    Error,
    S3.Encoding,
    S3.EndpointRules,
    S3.Request,
    S3.WriteResponse
  }

  @doc "Resolves the pinned operation endpoint and its separate authentication contract."
  def resolve(context, request, input, opts \\ []) do
    with {:ok, contract} <- EndpointRules.resolve(context, request.operation, input, opts),
         {:ok, uri} <- host_prefix(URI.parse(contract.endpoint), request.operation, input) do
      suffix =
        if contract.strip_bucket,
          do: String.replace_prefix(request.path, "/" <> Encoding.encode(input["Bucket"]), ""),
          else: request.path

      path = String.trim_trailing(uri.path || "", "/") <> suffix
      query = Encoding.query(request.query)

      {:ok, URI.to_string(%{uri | path: path, query: if(query == "", do: nil, else: query)}),
       contract}
    end
  end

  defp host_prefix(uri, "WriteGetObjectResponse", input) do
    with :ok <- WriteResponse.route(input["RequestRoute"]),
         do: {:ok, %{uri | host: input["RequestRoute"] <> "." <> uri.host}}
  end

  defp host_prefix(uri, _operation, _input), do: {:ok, uri}

  def url(%Context{} = context, %Request{} = request, input) do
    with :ok <- Context.validate_endpoint(context.endpoint),
         {:ok, uri, path} <-
           address(URI.parse(context.endpoint), context.addressing, request.path, input["Bucket"]) do
      path = String.trim_trailing(uri.path || "", "/") <> path
      query = Encoding.query(request.query)
      {:ok, URI.to_string(%{uri | path: path, query: if(query == "", do: nil, else: query)})}
    end
  end

  defp address(uri, _style, path, nil), do: {:ok, uri, path}

  defp address(_uri, _style, _path, "arn:" <> _),
    do: failure("ARN endpoints require the special-endpoint milestone")

  defp address(_uri, _style, _path, bucket) when not is_binary(bucket),
    do: failure("Invalid S3 bucket")

  defp address(uri, :path, path, _bucket), do: {:ok, uri, path}

  defp address(uri, :virtual, path, bucket) do
    prefix = "/" <> Encoding.encode(bucket)

    if Regex.match?(~r/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/, bucket) and
         not String.contains?(bucket, ["..", "--x-s3"]) and String.starts_with?(path, prefix) do
      suffix = String.replace_prefix(path, prefix, "")
      {:ok, %{uri | host: bucket <> "." <> uri.host}, if(suffix == "", do: "/", else: suffix)}
    else
      failure("Virtual-hosted addressing requires a DNS-compatible general-purpose bucket")
    end
  end

  defp address(_uri, _style, _path, _bucket), do: failure("Unknown S3 addressing mode")
  defp failure(message), do: {:error, Error.new(:configuration, message)}
end
