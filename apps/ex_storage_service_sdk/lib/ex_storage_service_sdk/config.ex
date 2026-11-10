defmodule ExStorageServiceSdk.Config do
  @moduledoc """
  Immutable connection settings captured by one client.

  Every request made with a config uses its own endpoint, region, credentials and
  transport, independent of global application settings. Build one with
  `ExStorageServiceSdk.new/1`.

  Extension points:

    * `:credential_provider` - zero-arity function returning
      `{:ok, %ExStorageServiceSdk.Credentials{}}` or `{:error, %ExStorageServiceSdk.Error{}}`.
      It is called before each signed request, so it can rotate temporary credentials.
    * `:session_provider` - S3 Express directory-bucket session resolver,
      `fn config, bucket, endpoint, mode -> {:ok, credentials} | {:error, error} end`.
    * `:transport` - module implementing `ExStorageServiceSdk.Transport`.
    * `:coordinator` - optional `ExStorageServiceSdk.Transfer.Coordinator` bounding request
      memory and propagating cancellation.
    * `:options` - map of protocol options (for example `:request_headers`,
      `:request_checksum_calculation`, `:debug`). Unknown keys are ignored by the SDK.
  """
  alias ExStorageServiceSdk.{Credentials, Error}

  @derive {Inspect,
           except: [
             :access_key_id,
             :secret_access_key,
             :session_token,
             :credentials,
             :credential_provider,
             :session_provider,
             :options
           ]}
  defstruct frontend: :native,
            endpoint: "http://localhost:9000",
            region: "us-east-1",
            access_key_id: nil,
            secret_access_key: nil,
            session_token: nil,
            credentials: nil,
            credential_provider: nil,
            session_provider: nil,
            coordinator: nil,
            unsigned: false,
            addressing: :path,
            tls: [verify: :verify_peer],
            timeout: 60_000,
            connect_timeout: 60_000,
            read_idle_timeout: nil,
            write_idle_timeout: nil,
            capabilities: %{},
            transport: ExStorageServiceSdk.Transport.HTTPFetch,
            options: %{}

  @type t :: %__MODULE__{}

  @doc "Refreshes the credential record before request signing."
  def refresh(%__MODULE__{unsigned: true} = config), do: {:ok, config}

  def refresh(%__MODULE__{credential_provider: nil} = config) do
    with :ok <- credential_expiry(config.credentials), do: {:ok, config}
  end

  def refresh(%__MODULE__{credential_provider: provider} = config)
      when is_function(provider, 0) do
    with {:ok, %Credentials{} = credentials} <- provider.(),
         do: {:ok, with_credentials(config, credentials)}
  end

  @doc "Replaces the active credential record and its derived signing fields."
  def with_credentials(config, nil), do: config

  def with_credentials(config, %Credentials{} = credentials) do
    %{
      config
      | credentials: credentials,
        access_key_id: credentials.access_key_id,
        secret_access_key: credentials.secret_access_key,
        session_token: credentials.session_token
    }
  end

  @doc false
  def validate_endpoint(endpoint) when is_binary(endpoint) do
    case URI.new(endpoint) do
      {:ok,
       %URI{scheme: scheme, host: host, port: port, userinfo: nil, fragment: nil, query: nil}}
      when scheme in ["http", "https"] and is_binary(host) and host != "" and
             is_integer(port) and port > 0 and port <= 65_535 ->
        :ok

      _ ->
        invalid("Endpoint must be an HTTP(S) URL without credentials, query or fragment")
    end
  rescue
    _ in ArgumentError -> invalid("Endpoint must be an HTTP(S) URL")
  end

  def validate_endpoint(_endpoint), do: invalid("Endpoint must be an HTTP(S) URL")

  defp credential_expiry(nil), do: :ok
  defp credential_expiry(record), do: Credentials.expiry(record, DateTime.utc_now())

  defp invalid(message), do: {:error, Error.new(:configuration, message)}
end
