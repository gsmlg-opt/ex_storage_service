defmodule ExStorageServiceCli.Credentials.Provider do
  @moduledoc "Pinned AWS credential precedence with coherent providers and invocation-owned refresh."
  alias ExStorageServiceCli.Credentials.{
    AssumeRole,
    Legacy,
    Metadata,
    Owner,
    Record,
    SharedConfig,
    SSO
  }

  alias ExStorageServiceCli.Credentials.Process, as: CredentialProcess
  alias ExStorageServiceCli.Error

  @doc "Performs explicit IAM Identity Center PKCE or device login for the selected profile."
  def login(options, opts \\ []) do
    env = Keyword.get(opts, :env, System.get_env())
    profile = options[:profile] || env["AWS_PROFILE"] || env["AWS_DEFAULT_PROFILE"] || "default"

    with {:ok, stores} <-
           SharedConfig.load(
             opts
             |> Keyword.put(:env, env)
             |> Keyword.put(:load_credentials, false)
           ),
         :ok <- profile_exists(stores, profile, true),
         :ok <- validate_profile(stores, profile, true),
         {:ok, settings} <-
           settings(stores, profile, Map.take(options, [:profile, :region, :ca_bundle]), env) do
      SSO.login(
        stores.config[profile] || %{},
        stores,
        env,
        Keyword.put(opts, :provider_ca_bundle, settings.ca_bundle || opts[:provider_ca_bundle])
      )
    end
  end

  @doc "Removes only the cached IAM Identity Center session selected by this profile."
  def logout(options, opts \\ []) do
    env = Keyword.get(opts, :env, System.get_env())
    profile = options[:profile] || env["AWS_PROFILE"] || env["AWS_DEFAULT_PROFILE"] || "default"

    with {:ok, stores} <-
           SharedConfig.load(
             opts
             |> Keyword.put(:env, env)
             |> Keyword.put(:load_credentials, false)
           ),
         :ok <- profile_exists(stores, profile, true),
         :ok <- validate_profile(stores, profile, true) do
      SSO.logout(stores.config[profile] || %{}, stores, env, opts)
    end
  end

  def start_link(options, opts \\ []) do
    owner_opts =
      Keyword.take(opts, [
        :clock,
        :expiry_skew,
        :advisory_timeout,
        :mandatory_timeout,
        :refresh_attempt_timeout,
        :max_waiters,
        :invocation_owner
      ])

    Owner.start_link(
      fn -> resolve(options, Keyword.delete(opts, :credential_owner)) end,
      owner_opts
    )
  end

  def resolve(options, opts \\ []) do
    if opts[:credential_owner],
      do: Owner.resolve(opts[:credential_owner]),
      else: resolve_uncached(options, opts)
  end

  defp resolve_uncached(options, opts) do
    env = Keyword.get(opts, :env, System.get_env())

    profile =
      options[:profile] || present(env["AWS_PROFILE"]) || present(env["AWS_DEFAULT_PROFILE"]) ||
        "default"

    unsigned = options[:no_sign_request] == true

    with {:ok, stores} <-
           SharedConfig.load(
             opts
             |> Keyword.put(:env, env)
             |> Keyword.put(:load_credentials, not unsigned)
           ),
         :ok <-
           profile_exists(
             stores,
             profile,
             options[:profile] != nil or present(env["AWS_PROFILE"]) != nil or
               present(env["AWS_DEFAULT_PROFILE"]) != nil
           ),
         :ok <- validate_profile(stores, profile, unsigned),
         {:ok, settings} <- settings(stores, profile, options, env) do
      if unsigned do
        {:ok, nil, %{settings | unsigned: true}}
      else
        with :ok <- cycle(profile, opts),
             opts = Keyword.update(opts, :visited_profiles, [profile], &[profile | &1]),
             opts =
               Keyword.put(
                 opts,
                 :provider_ca_bundle,
                 settings.ca_bundle || opts[:provider_ca_bundle]
               ),
             opts = Keyword.put(opts, :region, settings.region),
             {:ok, record} <- credentials(stores, profile, options, env, opts),
             :ok <- CredentialProcess.expiry(record, Keyword.get(opts, :now, DateTime.utc_now())) do
          {:ok, record, settings}
        end
      end
    end
  end

  defp profile_exists(stores, name, selected) do
    if (name == "default" and not selected) or Map.has_key?(stores.config, name) or
         Map.has_key?(stores.credentials, name),
       do: :ok,
       else: failure(:profile_not_found)
  end

  defp settings(stores, profile, options, env) do
    config = Map.merge(stores.credentials[profile] || %{}, stores.config[profile] || %{})
    s3 = config["s3"] || %{}

    if is_map(s3) do
      with {:ok, endpoint} <- endpoint(stores, config, options, env),
           {:ok, resolved_options} <- option_settings(config, options, env) do
        {:ok,
         %{
           region:
             options[:region] || present(env["AWS_REGION"]) || present(env["AWS_DEFAULT_REGION"]) ||
               present(config["region"]),
           endpoint_url: endpoint,
           ca_bundle:
             options[:ca_bundle] || present(env["AWS_CA_BUNDLE"]) || present(config["ca_bundle"]),
           s3: s3,
           profile: profile,
           options: resolved_options,
           unsigned: false
         }}
      end
    else
      failure(:invalid_shared_config)
    end
  end

  defp endpoint(stores, config, options, env) do
    cond do
      options[:endpoint_url] ->
        {:ok, options[:endpoint_url]}

      String.downcase(
        env["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] || config["ignore_configured_endpoint_urls"] ||
            "false"
      ) == "true" ->
        {:ok, nil}

      present(env["AWS_ENDPOINT_URL_S3"]) ->
        {:ok, env["AWS_ENDPOINT_URL_S3"]}

      present(env["AWS_ENDPOINT_URL"]) ->
        {:ok, env["AWS_ENDPOINT_URL"]}

      config["services"] ->
        case stores.sections["services " <> config["services"]] do
          nil ->
            failure(:service_config_not_found)

          section ->
            case section["s3"] do
              s3 when is_map(s3) ->
                if is_binary(s3["endpoint_url"]) or is_nil(s3["endpoint_url"]),
                  do: {:ok, present(s3["endpoint_url"]) || present(config["endpoint_url"])},
                  else: failure(:invalid_shared_config)

              nil ->
                {:ok, present(config["endpoint_url"])}

              _ ->
                failure(:invalid_shared_config)
            end
        end

      true ->
        {:ok, present(config["endpoint_url"])}
    end
  end

  defp credentials(stores, profile, options, env, opts) do
    config = stores.config[profile] || %{}

    opts =
      opts
      |> Keyword.put(:metadata_config, config)
      |> Keyword.put(:credential_sections, stores.sections)

    stored = stores.credentials[profile] || %{}
    explicit = options[:profile] != nil

    cond do
      not explicit and present(env["AWS_ACCESS_KEY_ID"]) ->
        Record.new(
          %{
            access_key_id: env["AWS_ACCESS_KEY_ID"],
            secret_access_key: env["AWS_SECRET_ACCESS_KEY"],
            session_token:
              present(env["AWS_SECURITY_TOKEN"]) || present(env["AWS_SESSION_TOKEN"]),
            expires_at: present(env["AWS_CREDENTIAL_EXPIRATION"])
          },
          %{provider: :environment}
        )

      present(config["role_arn"]) != nil and present(config["web_identity_token_file"]) == nil ->
        AssumeRole.role(config, stores, profile, env, opts)

      present(config["web_identity_token_file"]) != nil or
          (not explicit and present(env["AWS_WEB_IDENTITY_TOKEN_FILE"])) ->
        AssumeRole.web_identity(config, env, explicit, opts)

      Enum.any?(
        ["sso_session", "sso_start_url", "sso_account_id", "sso_role_name"],
        &present(config[&1])
      ) ->
        SSO.resolve(config, stores, env, opts)

      present(stored["aws_access_key_id"]) ->
        static(stored, :shared_credentials, profile)

      present(config["credential_process"]) ->
        CredentialProcess.run(
          config["credential_process"],
          opts |> Keyword.put(:env, env) |> Keyword.put(:profile, profile)
        )

      present(config["aws_access_key_id"]) ->
        static(config, :shared_config, profile)

      true ->
        fallback(env, opts)
    end
  end

  defp fallback(env, opts) do
    with {:ok, record} <- Legacy.resolve(env, opts) do
      cond do
        record ->
          {:ok, record}

        present(env["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"]) != nil or
            present(env["AWS_CONTAINER_CREDENTIALS_FULL_URI"]) != nil ->
          Metadata.container(env, opts)

        String.downcase(env["AWS_EC2_METADATA_DISABLED"] || "false") != "true" ->
          Metadata.imds(env, opts)

        true ->
          failure(:credentials_not_found)
      end
    end
  end

  defp static(values, provider, profile),
    do:
      Record.new(
        %{
          access_key_id: values["aws_access_key_id"],
          secret_access_key: values["aws_secret_access_key"],
          session_token:
            present(values["aws_security_token"]) || present(values["aws_session_token"])
        },
        %{provider: provider, profile: profile}
      )

  defp cycle(profile, opts) do
    if profile in Keyword.get(opts, :visited_profiles, []),
      do:
        {:error,
         Error.new(:credentials, "AWS role source profiles contain a cycle",
           code: :credential_profile_cycle
         )},
      else: :ok
  end

  defp validate_profile(stores, profile, unsigned) do
    settings_keys =
      ~w(region endpoint_url services ignore_configured_endpoint_urls ca_bundle output cli_binary_format retry_mode max_attempts cli_read_timeout cli_connect_timeout use_fips_endpoint use_dualstack_endpoint request_checksum_calculation response_checksum_validation)

    credential_keys =
      ~w(aws_access_key_id aws_secret_access_key aws_session_token aws_security_token credential_process role_arn web_identity_token_file source_profile credential_source sso_session sso_start_url sso_region sso_account_id sso_role_name role_session_name duration_seconds external_id mfa_serial sts_regional_endpoints ec2_metadata_v1_disabled ec2_metadata_service_endpoint ec2_metadata_service_endpoint_mode)

    keys = settings_keys ++ if(unsigned, do: [], else: credential_keys)

    valid =
      Enum.all?([stores.config[profile] || %{}, stores.credentials[profile] || %{}], fn values ->
        Enum.all?(keys, fn key -> is_nil(values[key]) or is_binary(values[key]) end) and
          (is_nil(values["s3"]) or
             (is_map(values["s3"]) and
                Enum.all?(values["s3"], fn {_key, value} -> is_binary(value) end)))
      end)

    if valid, do: :ok, else: failure(:invalid_shared_config)
  end

  defp option_settings(config, options, env) do
    fields = [
      {:output, "output", "AWS_DEFAULT_OUTPUT"},
      {:cli_binary_format, "cli_binary_format", nil},
      {:retry_mode, "retry_mode", "AWS_RETRY_MODE"},
      {:max_attempts, "max_attempts", "AWS_MAX_ATTEMPTS"},
      {:cli_read_timeout, "cli_read_timeout", nil},
      {:cli_connect_timeout, "cli_connect_timeout", nil},
      {:use_fips_endpoint, "use_fips_endpoint", "AWS_USE_FIPS_ENDPOINT"},
      {:use_dualstack_endpoint, "use_dualstack_endpoint", "AWS_USE_DUALSTACK_ENDPOINT"},
      {:request_checksum_calculation, "request_checksum_calculation",
       "AWS_REQUEST_CHECKSUM_CALCULATION"},
      {:response_checksum_validation, "response_checksum_validation",
       "AWS_RESPONSE_CHECKSUM_VALIDATION"}
    ]

    Enum.reduce_while(fields, {:ok, %{}}, fn {key, setting, variable}, {:ok, result} ->
      value =
        if Map.has_key?(options, key) and options[key] != nil,
          do: options[key],
          else: present(env[variable]) || present(config[setting])

      case normalize_option(key, value) do
        {:ok, nil} -> {:cont, {:ok, result}}
        {:ok, value} -> {:cont, {:ok, Map.put(result, key, value)}}
        error -> {:halt, error}
      end
    end)
  end

  defp normalize_option(_key, nil), do: {:ok, nil}

  defp normalize_option(key, value)
       when key in [:cli_read_timeout, :cli_connect_timeout, :max_attempts] do
    minimum = if key == :max_attempts, do: 1, else: 0

    parsed =
      cond do
        is_integer(value) -> {value, ""}
        is_binary(value) -> Integer.parse(value)
        true -> :error
      end

    case parsed do
      {number, ""} when number >= minimum -> {:ok, number}
      _ -> failure(:invalid_shared_config)
    end
  end

  defp normalize_option(key, value) when key in [:use_fips_endpoint, :use_dualstack_endpoint] do
    case value do
      value when is_boolean(value) -> {:ok, value}
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> failure(:invalid_shared_config)
    end
  end

  defp normalize_option(:output, value) when value in ~w(json text table yaml yaml-stream),
    do: {:ok, value}

  defp normalize_option(:cli_binary_format, value) when value in ~w(base64 raw-in-base64-out),
    do: {:ok, value}

  defp normalize_option(:retry_mode, value) when value in ~w(standard adaptive),
    do: {:ok, value}

  defp normalize_option(key, value)
       when key in [:request_checksum_calculation, :response_checksum_validation] and
              value in ~w(when_supported when_required),
       do: {:ok, value}

  defp normalize_option(_key, _value), do: failure(:invalid_shared_config)

  defp present(value) when is_binary(value),
    do: if(String.trim(value) == "", do: nil, else: value)

  defp present(_value), do: nil

  defp failure(code),
    do:
      {:error,
       Error.new(:configuration, "AWS profile settings or credentials are unavailable or invalid",
         code: code
       )}
end
