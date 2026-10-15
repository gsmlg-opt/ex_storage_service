defmodule ExStorageServiceCli.Credentials.AssumeRole do
  @moduledoc "STS role credentials from one coherent source or a bounded web-identity token."
  alias ExStorageServiceCli.{Context, SigV4}
  alias ExStorageServiceCli.Credentials.{Metadata, Provider, Record, Remote}
  alias ExStorageServiceCli.Credentials.Process, as: CredentialProcess
  alias ExStorageServiceCli.S3.XML
  alias ExStorageServiceCli.CLI.Signals.Native

  def role(config, stores, profile, env, opts) do
    with {:ok, source} <- source(config, stores, profile, env, opts),
         :ok <- CredentialProcess.expiry(source, opts[:now] || DateTime.utc_now()),
         {:ok, mfa} <- mfa(config, opts) do
      parameters =
        [
          {"Action", "AssumeRole"},
          {"Version", "2011-06-15"},
          {"RoleArn", config["role_arn"]},
          {"RoleSessionName", session_name(config, env, opts, false)}
        ] ++
          optional(config, [
            {"external_id", "ExternalId"},
            {"duration_seconds", "DurationSeconds"}
          ]) ++ mfa

      request(parameters, source, config, env, :assume_role, opts)
    end
  end

  def web_identity(config, env, explicit, opts) do
    token_file =
      if(not explicit, do: env["AWS_WEB_IDENTITY_TOKEN_FILE"]) ||
        config["web_identity_token_file"]

    role_arn = if(not explicit, do: env["AWS_ROLE_ARN"]) || config["role_arn"]

    with true <- is_binary(role_arn) and role_arn != "",
         {:ok, token} <- Remote.file(token_file) do
      parameters =
        [
          {"Action", "AssumeRoleWithWebIdentity"},
          {"Version", "2011-06-15"},
          {"RoleArn", role_arn},
          {"RoleSessionName", session_name(config, env, opts, not explicit)},
          {"WebIdentityToken", token}
        ] ++ optional(config, [{"duration_seconds", "DurationSeconds"}])

      request(parameters, nil, config, env, :web_identity, opts)
    else
      {:error, _} = error -> error
      _ -> Remote.failure(:invalid_role_config)
    end
  end

  defp source(config, stores, profile, env, opts) do
    source_profile = config["source_profile"]
    credential_source = config["credential_source"]

    cond do
      source_profile && credential_source ->
        Remote.failure(:invalid_role_config)

      source_profile == profile ->
        values =
          if stores.credentials[profile] && stores.credentials[profile]["aws_access_key_id"],
            do: stores.credentials[profile],
            else: config

        Record.new(
          %{
            access_key_id: values["aws_access_key_id"],
            secret_access_key: values["aws_secret_access_key"],
            session_token: values["aws_session_token"] || values["aws_security_token"]
          },
          %{provider: :role_source}
        )

      source_profile ->
        case Provider.resolve(%{profile: source_profile}, Keyword.put(opts, :env, env)) do
          {:ok, record, _settings} -> {:ok, record}
          error -> error
        end

      credential_source == "Environment" ->
        Record.new(
          %{
            access_key_id: env["AWS_ACCESS_KEY_ID"],
            secret_access_key: env["AWS_SECRET_ACCESS_KEY"],
            session_token: env["AWS_SECURITY_TOKEN"] || env["AWS_SESSION_TOKEN"],
            expires_at: env["AWS_CREDENTIAL_EXPIRATION"]
          },
          %{provider: :environment}
        )

      credential_source == "EcsContainer" ->
        Metadata.container(env, opts)

      credential_source == "Ec2InstanceMetadata" ->
        Metadata.imds(env, opts)

      true ->
        Remote.failure(:invalid_role_config)
    end
  end

  defp request(parameters, source, config, env, provider, opts) do
    region =
      opts[:region] || env["AWS_REGION"] || env["AWS_DEFAULT_REGION"] || config["region"] ||
        "us-east-1"

    regional = env["AWS_STS_REGIONAL_ENDPOINTS"] || config["sts_regional_endpoints"] || "legacy"

    partition =
      if String.starts_with?(region, "cn-"), do: "amazonaws.com.cn", else: "amazonaws.com"

    regional? = regional == "regional" or String.starts_with?(region, ["cn-", "us-gov-"])

    endpoint =
      opts[:sts_endpoint] || env["AWS_ENDPOINT_URL_STS"] ||
        if(regional?, do: "https://sts.#{region}.#{partition}", else: "https://sts.amazonaws.com")

    signing_region =
      if regional? or opts[:sts_endpoint] != nil or env["AWS_ENDPOINT_URL_STS"] != nil,
        do: region,
        else: "us-east-1"

    with :ok <- Context.validate_endpoint(endpoint),
         true <- URI.parse(endpoint).scheme == "https" or opts[:sts_endpoint] != nil,
         true <- Enum.all?(parameters, fn {_key, value} -> is_binary(value) end) do
      body = URI.encode_query(parameters)
      headers = [{"Content-Type", "application/x-www-form-urlencoded; charset=utf-8"}]

      headers =
        if source do
          headers ++
            SigV4.sign_headers("POST", endpoint, headers, body,
              access_key_id: source.access_key_id,
              secret_access_key: source.secret_access_key,
              session_token: source.session_token,
              region: signing_region,
              service: "sts",
              now: opts[:now] || DateTime.utc_now()
            )
        else
          headers
        end

      with {:ok, response} <- Remote.request(endpoint, "POST", headers, body, opts),
           {:ok, root} <- XML.parse(response, max_bytes: 65_536),
           [result] <-
             XML.children(
               root,
               parameters |> List.keyfind("Action", 0) |> elem(1) |> Kernel.<>("Result")
             ),
           [credentials] <- XML.children(result, "Credentials"),
           {:ok, record} <-
             Record.new(
               %{
                 access_key_id: XML.child_text(credentials, "AccessKeyId"),
                 secret_access_key: XML.child_text(credentials, "SecretAccessKey"),
                 session_token: XML.child_text(credentials, "SessionToken"),
                 expires_at: XML.child_text(credentials, "Expiration")
               },
               %{provider: provider}
             ),
           :ok <- CredentialProcess.expiry(record, opts[:now] || DateTime.utc_now()) do
        {:ok, record}
      else
        {:error, _} = error -> error
        _ -> Remote.failure(:invalid_credential_response)
      end
    else
      _ -> Remote.failure(:invalid_role_config)
    end
  end

  defp session_name(config, env, opts, allow_env?) do
    if(allow_env?, do: env["AWS_ROLE_SESSION_NAME"]) || config["role_session_name"] ||
      "ess-#{DateTime.to_unix(opts[:now] || DateTime.utc_now())}"
  end

  defp optional(config, fields),
    do: for({key, parameter} <- fields, is_binary(config[key]), do: {parameter, config[key]})

  defp mfa(%{"mfa_serial" => serial}, opts) when is_binary(serial) do
    with {:ok, code} <- mfa_code(serial, opts) do
      if is_binary(code) and Regex.match?(~r/^\d{6}$/, code),
        do: {:ok, [{"SerialNumber", serial}, {"TokenCode", code}]},
        else: Remote.failure(:invalid_mfa_code)
    end
  end

  defp mfa(_config, _opts), do: {:ok, []}

  defp mfa_code(serial, opts) do
    cond do
      is_binary(opts[:mfa_code]) ->
        {:ok, opts[:mfa_code]}

      is_function(opts[:mfa_prompt], 1) ->
        opts[:mfa_prompt].(serial)

      true ->
        case :io.getopts(:standard_io) do
          options when is_list(options) ->
            if options[:terminal] do
              with {:ok, terminal} <- Native.suppress_stdin_echo() do
                try do
                  IO.write(:stderr, "MFA token code: ")

                  case IO.gets(:standard_io, "") do
                    code when is_binary(code) -> {:ok, String.trim(code)}
                    _ -> Remote.failure(:mfa_required)
                  end
                after
                  Native.restore_terminal(terminal)
                  IO.write(:stderr, "\n")
                end
              end
            else
              Remote.failure(:mfa_required)
            end

          _ ->
            Remote.failure(:mfa_required)
        end
    end
  end
end
