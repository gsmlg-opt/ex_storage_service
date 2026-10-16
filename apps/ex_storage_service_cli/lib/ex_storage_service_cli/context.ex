defmodule ExStorageServiceCli.Context do
  @moduledoc "Immutable settings for one endpoint; remote transfer targets resolve independently."
  alias ExStorageServiceCli.{Aliases, Config, Error, Target}
  alias ExStorageServiceCli.Credentials.{Owner, Provider, Record}
  alias ExStorageServiceCli.Credentials.Process, as: CredentialProcess
  alias ExStorageServiceCli.CLI.Runtime

  @derive {Inspect,
           except: [:access_key_id, :secret_access_key, :session_token, :credentials, :options]}
  defstruct frontend: :native,
            endpoint: "http://localhost:9000",
            region: "us-east-1",
            profile: "default",
            access_key_id: nil,
            secret_access_key: nil,
            session_token: nil,
            credentials: nil,
            credential_owner: nil,
            coordinator: nil,
            directory_sessions: nil,
            unsigned: false,
            addressing: :path,
            tls: [verify: :verify_peer],
            timeout: 60_000,
            connect_timeout: 60_000,
            read_idle_timeout: nil,
            write_idle_timeout: nil,
            capabilities: %{},
            json: false,
            options: %{}

  @doc "Resolves one native target using one immutable configuration snapshot."
  def native(target, options, config, opts \\ []) do
    with {:ok, alias_record} <- settings(target, options, config),
         {:ok, profile} <- credential_profile(alias_record, config, options, opts),
         unsigned = options[:no_sign_request] == true or alias_record["provider"] == "anonymous",
         {:ok, credentials} <- native_credentials(profile, alias_record, options, unsigned),
         :ok <- credential_expiry(credentials),
         endpoint =
           options[:endpoint] || alias_record["endpoint"] || profile[:endpoint] ||
             "http://localhost:9000",
         :ok <- validate_endpoint(endpoint),
         {:ok, tls} <- tls(options, profile[:ca_bundle]) do
      {:ok,
       with_credentials(
         %__MODULE__{
           endpoint: endpoint,
           region: options[:region] || alias_record["region"] || profile[:region] || "us-east-1",
           profile: alias_record["profile"] || options[:profile] || "default",
           unsigned: unsigned,
           credential_owner: profile[:credential_owner],
           coordinator: if(opts[:runtime], do: opts[:runtime].coordinator),
           directory_sessions: if(opts[:runtime], do: opts[:runtime].directory_sessions),
           addressing: profile[:addressing] || :path,
           tls: tls,
           timeout: :infinity,
           read_idle_timeout: Map.get(options, :read_idle_timeout, 600_000),
           write_idle_timeout: Map.get(options, :write_idle_timeout, 600_000),
           json: options[:json] || false,
           capabilities:
             ExStorageServiceCli.S3.Capabilities.record(
               options[:service] || alias_record["service"] ||
                 if(alias_record["provider"] == "ess-profile", do: "ess", else: "unknown"),
               alias_record["service_version"],
               alias_record["bucket_type"] || "general"
             ),
           options:
             options
             |> Map.put(:s3_settings, profile[:s3_settings] || %{})
             |> runtime_options(opts)
         },
         credentials
       )}
    end
  end

  @doc "Resolves AWS settings independently of the ESS configuration store."
  def aws(options, opts \\ []) do
    with {:ok, opts} <- owner_options(options, opts),
         {:ok, credentials, settings} <- Provider.resolve(options, opts),
         options = Map.merge(Map.get(settings, :options, %{}), options),
         region = settings.region || "us-east-1",
         :ok <- validate_region(region),
         endpoint = settings.endpoint_url || regional_endpoint(region),
         :ok <- validate_endpoint(endpoint),
         {:ok, addressing} <- addressing(settings.s3, settings.endpoint_url),
         {:ok, tls} <- tls(options, settings.ca_bundle),
         {:ok, read_timeout} <- timeout(options[:cli_read_timeout]),
         {:ok, connect_timeout} <- timeout(options[:cli_connect_timeout]) do
      {:ok,
       with_credentials(
         %__MODULE__{
           frontend: :aws,
           endpoint: endpoint,
           region: region,
           profile: settings.profile,
           unsigned: settings.unsigned,
           credential_owner: opts[:credential_owner],
           coordinator: if(opts[:runtime], do: opts[:runtime].coordinator),
           directory_sessions: if(opts[:runtime], do: opts[:runtime].directory_sessions),
           addressing: addressing,
           tls: tls,
           timeout: :infinity,
           read_idle_timeout: if(read_timeout == :infinity, do: nil, else: read_timeout),
           connect_timeout: connect_timeout,
           json: options[:output] in [nil, "json"],
           capabilities:
             ExStorageServiceCli.S3.Capabilities.record(
               if(settings.endpoint_url, do: "s3", else: "aws")
             ),
           options:
             options
             |> Map.put(:s3_settings, settings.s3)
             |> Map.put(:resolved_endpoint, settings.endpoint_url)
             |> runtime_options(opts)
         },
         credentials
       )}
    end
  end

  @doc "Refreshes one invocation's coherent credential record before request signing."
  def refresh(%__MODULE__{unsigned: true} = context), do: {:ok, context}

  def refresh(%__MODULE__{credential_owner: nil} = context) do
    with :ok <- credential_expiry(context.credentials), do: {:ok, context}
  end

  def refresh(%__MODULE__{} = context) do
    with {:ok, %Record{} = credentials, _settings} <- Owner.resolve(context.credential_owner) do
      {:ok, with_credentials(context, credentials)}
    end
  end

  defp owner_options(options, opts) do
    cond do
      options[:no_sign_request] == true ->
        {:ok, Keyword.delete(opts, :credential_owner)}

      opts[:credential_owner] ->
        {:ok, opts}

      opts[:runtime] ->
        with {:ok, owner} <- Runtime.provider(opts[:runtime], options, opts),
             do: {:ok, Keyword.put(opts, :credential_owner, owner)}

      true ->
        {:ok, opts}
    end
  end

  defp runtime_options(options, opts) do
    if opts[:runtime],
      do:
        Map.merge(options, %{
          retry_state: opts[:runtime].retry_state,
          rate_limiter: opts[:runtime].rate_limiter,
          task_supervisor: opts[:runtime].tasks,
          coordinator: opts[:runtime].coordinator
        }),
      else: options
  end

  defp credential_expiry(nil), do: :ok
  defp credential_expiry(record), do: CredentialProcess.expiry(record, DateTime.utc_now())

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

  defp with_credentials(context, nil), do: context

  defp with_credentials(context, credentials) do
    %{
      context
      | credentials: credentials,
        access_key_id: credentials.access_key_id,
        secret_access_key: credentials.secret_access_key,
        session_token: credentials.session_token
    }
  end

  defp settings(%Target{alias: name}, _options, config) when is_binary(name),
    do: Aliases.from_config(config, name)

  defp settings(_target, options, _config),
    do: {:ok, %{"provider" => "ess-profile", "profile" => options[:profile] || "default"}}

  defp credential_profile(
         %{"provider" => "ess-profile", "profile" => name},
         config,
         _options,
         _opts
       ),
       do: Config.profile(config, name)

  defp credential_profile(%{"provider" => "static"} = record, _config, _options, _opts),
    do:
      {:ok,
       %{
         access_key_id: record["access_key_id"],
         secret_access_key: record["secret_access_key"],
         session_token: record["session_token"]
       }}

  defp credential_profile(%{"provider" => "anonymous"}, _config, _options, _opts), do: {:ok, %{}}

  defp credential_profile(%{"provider" => "environment"}, _config, _options, opts) do
    env = Keyword.get(opts, :env, System.get_env())

    {:ok,
     %{
       access_key_id: env["AWS_ACCESS_KEY_ID"],
       secret_access_key: env["AWS_SECRET_ACCESS_KEY"],
       session_token: env["AWS_SECURITY_TOKEN"] || env["AWS_SESSION_TOKEN"],
       expires_at: env["AWS_CREDENTIAL_EXPIRATION"]
     }}
  end

  defp credential_profile(
         %{"provider" => "aws-profile", "profile" => profile},
         _config,
         options,
         opts
       ) do
    provider_options = %{
      profile: profile,
      region: options[:region],
      no_sign_request: options[:no_sign_request] == true or explicit_credentials?(options)
    }

    with {:ok, opts} <- owner_options(provider_options, opts),
         {:ok, credentials, settings} <- Provider.resolve(provider_options, opts),
         {:ok, addressing} <- addressing(settings.s3, true) do
      {:ok,
       %{
         credentials: credentials,
         credential_owner: opts[:credential_owner],
         region: settings.region,
         ca_bundle: settings.ca_bundle,
         s3_settings: settings.s3,
         addressing: addressing
       }}
    end
  end

  defp native_credentials(_profile, _record, _options, true), do: {:ok, nil}

  defp native_credentials(profile, record, options, false) do
    cond do
      explicit_credentials?(options) ->
        Record.new(
          %{
            access_key_id: options[:access_key],
            secret_access_key: options[:secret_key],
            session_token: options[:session_token]
          },
          %{provider: :explicit}
        )

      options[:session_token] != nil ->
        invalid("A session token requires an explicit complete credential pair")

      profile[:credentials] ->
        {:ok, profile.credentials}

      true ->
        Record.new(profile, %{provider: record["provider"], profile: record["profile"]})
    end
  end

  defp explicit_credentials?(options),
    do: options[:access_key] != nil or options[:secret_key] != nil

  defp regional_endpoint(region) do
    suffix = if String.starts_with?(region, "cn-"), do: "amazonaws.com.cn", else: "amazonaws.com"
    "https://s3.#{region}.#{suffix}"
  end

  defp validate_region(region) when is_binary(region) do
    if Regex.match?(~r/^[a-z0-9]+(?:-[a-z0-9]+)+$/, region),
      do: :ok,
      else: invalid("Invalid AWS region")
  end

  defp validate_region(_region), do: invalid("Invalid AWS region")

  defp addressing(s3, endpoint) do
    case s3["addressing_style"] do
      "path" -> {:ok, :path}
      "virtual" -> {:ok, :virtual}
      value when value in [nil, "auto"] -> {:ok, if(endpoint, do: :path, else: :virtual)}
      _ -> invalid("Invalid S3 addressing style")
    end
  end

  defp tls(options, ca_bundle) do
    ca_bundle = options[:ca_bundle] || ca_bundle

    base =
      cond do
        options[:no_verify_ssl] == true ->
          {:ok, [verify: :verify_none]}

        ca_bundle == nil ->
          {:ok, [verify: :verify_peer]}

        is_binary(ca_bundle) ->
          case File.stat(ca_bundle) do
            {:ok, %{type: :regular, size: size}} when size > 0 ->
              {:ok, [verify: :verify_peer, cacertfile: String.to_charlist(ca_bundle)]}

            _ ->
              invalid("CA bundle must be a readable nonempty file")
          end

        true ->
          invalid("Invalid CA bundle")
      end

    with {:ok, ssl} <- base, {:ok, certificate} <- client_identity(options) do
      {:ok, ssl ++ certificate}
    end
  end

  defp client_identity(%{client_cert: certificate, client_key: key})
       when is_binary(certificate) and is_binary(key) do
    if Enum.all?([certificate, key], fn path ->
         case File.stat(path) do
           {:ok, %{type: :regular, size: size}} when size > 0 -> true
           _ -> false
         end
       end),
       do: {:ok, [certfile: String.to_charlist(certificate), keyfile: String.to_charlist(key)]},
       else: invalid("Client certificate and key must be readable nonempty files")
  end

  defp client_identity(options) do
    if options[:client_cert] == nil and options[:client_key] == nil,
      do: {:ok, []},
      else: invalid("Client certificate and key must be configured together")
  end

  defp timeout(nil), do: {:ok, 60_000}
  defp timeout(0), do: {:ok, :infinity}
  defp timeout(seconds) when is_integer(seconds) and seconds > 0, do: {:ok, seconds * 1000}

  defp timeout(_seconds),
    do: invalid("CLI timeout must be a nonnegative integer number of seconds")

  defp invalid(message), do: {:error, Error.new(:configuration, message)}
end
