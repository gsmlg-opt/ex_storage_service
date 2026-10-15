defmodule ExStorageServiceCli.Credentials.Remote do
  @moduledoc false
  alias ExStorageServiceCli.{Error, S3.Request, Transport.HTTPFetch}
  @limit 65_536

  def request(url, method, headers, body, opts \\ []) do
    case response(url, method, headers, body, opts) do
      {:ok, status, body} when status in 200..299 -> {:ok, body}
      {:ok, status, _body} -> failure(:credential_service_error, status)
      error -> error
    end
  end

  def response(url, method, headers, body, opts \\ []) do
    request = %Request{method: method, headers: headers, body: body}

    with {:ok, ssl} <- tls(opts) do
      transport = [
        timeout: opts[:provider_timeout] || 5000,
        connect_timeout: opts[:provider_connect_timeout] || 1000,
        max_control_bytes: @limit,
        ssl: ssl
      ]

      case HTTPFetch.request(url, request, transport) do
        {:ok, %{status: status, body: body}} ->
          {:ok, status, body}

        {:error, _} = error ->
          if opts[:metadata_transport_errors],
            do: error,
            else: failure(:credential_transport_error)
      end
    end
  end

  defp tls(opts) do
    case opts[:provider_ca_bundle] do
      nil ->
        {:ok, [verify: :verify_peer]}

      path when is_binary(path) ->
        case File.open(path, [:read, :binary]) do
          {:ok, file} ->
            File.close(file)
            {:ok, [verify: :verify_peer, cacertfile: String.to_charlist(path)]}

          _ ->
            failure(:credential_ca_bundle_invalid)
        end

      _ ->
        failure(:credential_ca_bundle_invalid)
    end
  end

  def json(body) do
    case JSON.decode(body) do
      {:ok, value} when is_map(value) -> {:ok, value}
      _ -> failure(:invalid_credential_response)
    end
  end

  def file(path) when is_binary(path) do
    case File.open(path, [:read, :binary]) do
      {:ok, file} ->
        result = IO.binread(file, @limit + 1)
        File.close(file)

        case result do
          data when is_binary(data) and byte_size(data) in 1..@limit -> {:ok, data}
          _ -> failure(:credential_file_invalid)
        end

      _ ->
        failure(:credential_file_invalid)
    end
  end

  def file(_), do: failure(:credential_file_invalid)

  def failure(code, status \\ nil),
    do:
      {:error,
       Error.new(:credentials, "AWS credential provider failed", code: code, http_status: status)}
end
