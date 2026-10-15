defmodule ExStorageServiceCli.Credentials.Metadata do
  @moduledoc "Bounded container credentials, IMDSv2, and policy-controlled IMDSv1 fallback."
  alias ExStorageServiceCli.Context
  alias ExStorageServiceCli.Credentials.{Record, Remote}
  alias ExStorageServiceCli.Credentials.Process, as: CredentialProcess
  @container_hosts ~w(localhost 127.0.0.1 ::1 169.254.170.2 169.254.170.23 fd00:ec2::23)
  @metadata_hosts ~w(169.254.169.254 fd00:ec2::254)

  def container(env, opts \\ []) do
    endpoint =
      if env["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"],
        do: "http://169.254.170.2" <> env["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"],
        else: env["AWS_CONTAINER_CREDENTIALS_FULL_URI"]

    with :ok <- allowed(endpoint, @container_hosts, true),
         {:ok, headers} <- authorization(env),
         {:ok, body} <- Remote.request(endpoint, "GET", headers, nil, opts),
         {:ok, data} <- Remote.json(body) do
      credentials(data, :container, opts)
    end
  end

  def imds(env, opts \\ []) do
    config = opts[:metadata_config] || %{}

    mode =
      env["AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE"] ||
        config["ec2_metadata_service_endpoint_mode"] || "IPv4"

    default =
      if String.downcase(mode) == "ipv6",
        do: "http://[fd00:ec2::254]",
        else: "http://169.254.169.254"

    endpoint =
      opts[:metadata_endpoint] || env["AWS_EC2_METADATA_SERVICE_ENDPOINT"] ||
        config["ec2_metadata_service_endpoint"] || default

    hosts =
      if opts[:metadata_endpoint],
        do: @metadata_hosts ++ ~w(localhost 127.0.0.1 ::1),
        else: @metadata_hosts

    with false <- String.downcase(env["AWS_EC2_METADATA_DISABLED"] || "false") == "true",
         true <- String.downcase(mode) in ["ipv4", "ipv6"],
         :ok <- allowed(endpoint, hosts, false),
         {:ok, disabled} <- v1_disabled(env, config),
         {:ok, token} <- token(endpoint, disabled, opts),
         headers = if(token, do: [{"x-aws-ec2-metadata-token", token}], else: []),
         {:ok, role} <-
           Remote.request(
             endpoint <> "/latest/meta-data/iam/security-credentials/",
             "GET",
             headers,
             nil,
             opts
           ),
         role = String.trim(role),
         true <- Regex.match?(~r/^[A-Za-z0-9+=,.@_-]+$/, role),
         {:ok, body} <-
           Remote.request(
             endpoint <> "/latest/meta-data/iam/security-credentials/" <> role,
             "GET",
             headers,
             nil,
             opts
           ),
         {:ok, data} <- Remote.json(body),
         true <- data["Code"] == "Success" do
      credentials(data, :imds, opts)
    else
      {:error, _} = error -> error
      _ -> Remote.failure(:invalid_credential_response)
    end
  end

  defp v1_disabled(env, config) do
    case String.downcase(
           env["AWS_EC2_METADATA_V1_DISABLED"] || config["ec2_metadata_v1_disabled"] || "false"
         ) do
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> Remote.failure(:invalid_metadata_config)
    end
  end

  defp token(endpoint, disabled, opts) do
    case Remote.response(
           endpoint <> "/latest/api/token",
           "PUT",
           [{"x-aws-ec2-metadata-token-ttl-seconds", "21600"}],
           nil,
           Keyword.put(opts, :metadata_transport_errors, true)
         ) do
      {:ok, 200, token} ->
        if token != "" and String.valid?(token) and
             not String.contains?(token, ["\r", "\n", <<0>>]),
           do: {:ok, token},
           else: Remote.failure(:invalid_credential_response)

      {:ok, 400, _} ->
        Remote.failure(:credential_service_error, 400)

      {:ok, status, _} when status in 300..399 ->
        Remote.failure(:credential_service_error, status)

      {:ok, _status, _} ->
        fallback_v1(disabled)

      {:error, %ExStorageServiceCli.Error{category: :transport, retryable: true}} ->
        fallback_v1(disabled)

      {:error, _} ->
        Remote.failure(:credential_transport_error)
    end
  end

  defp fallback_v1(true), do: Remote.failure(:imds_v1_disabled)
  defp fallback_v1(false), do: {:ok, nil}

  defp credentials(data, provider, opts) do
    with true <- is_binary(data["Expiration"]),
         {:ok, record} <-
           Record.new(
             %{
               access_key_id: data["AccessKeyId"],
               secret_access_key: data["SecretAccessKey"],
               session_token: data["Token"],
               expires_at: data["Expiration"]
             },
             %{provider: provider}
           ),
         true <- is_binary(record.session_token),
         :ok <- CredentialProcess.expiry(record, opts[:now] || DateTime.utc_now()) do
      {:ok, record}
    else
      {:error, _} = error -> error
      _ -> Remote.failure(:invalid_credential_response)
    end
  end

  defp authorization(env) do
    value =
      if env["AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"],
        do: Remote.file(env["AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"]),
        else: {:ok, env["AWS_CONTAINER_AUTHORIZATION_TOKEN"]}

    case value do
      {:ok, nil} ->
        {:ok, []}

      {:ok, token} when is_binary(token) ->
        if String.valid?(token) and not String.contains?(token, ["\r", "\n", <<0>>]),
          do: {:ok, [{"Authorization", token}]},
          else: Remote.failure(:invalid_container_authorization)

      error ->
        error
    end
  end

  defp allowed(endpoint, hosts, https_any?) do
    with {:ok, %URI{} = uri} <- URI.new(endpoint || ""),
         :ok <- Context.validate_endpoint(URI.to_string(%{uri | query: nil})),
         true <- https_any? or uri.query == nil,
         true <-
           (https_any? and uri.scheme == "https") or host_allowed?(uri.host, hosts, https_any?) do
      :ok
    else
      _ -> Remote.failure(:unsafe_metadata_endpoint)
    end
  end

  defp host_allowed?(host, hosts, loopback?) do
    host = String.downcase(host)

    if host in hosts do
      true
    else
      case :inet.parse_address(String.to_charlist(host)) do
        {:ok, {127, _, _, _}} -> loopback?
        _ -> false
      end
    end
  end
end
