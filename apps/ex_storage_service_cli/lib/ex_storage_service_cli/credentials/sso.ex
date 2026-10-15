defmodule ExStorageServiceCli.Credentials.SSO do
  @moduledoc "IAM Identity Center role credentials and bounded cached-session token refresh."
  alias ExStorageServiceCli.Context
  alias ExStorageServiceCli.Credentials.{Browser, PKCE, Record, Remote}

  @doc "Starts PKCE login by default, or explicit device login with use_device_code: true."
  def login(config, stores, env, opts \\ []) do
    if opts[:use_device_code] do
      device_login(config, stores, env, opts)
    else
      with {:ok, settings} <- settings(config, stores, false),
           {:ok, oidc} <- endpoint(settings.region, :oidc, opts),
           {:ok, registration, response} <- PKCE.authorize(settings, oidc, opts) do
        publish_login(settings, registration, response, env, opts)
      end
    end
  end

  defp device_login(config, stores, env, opts) do
    with {:ok, settings} <- settings(config, stores, false),
         {:ok, oidc} <- endpoint(settings.region, :oidc, opts),
         {:ok, registration} <-
           post(
             oidc <> "/client/register",
             %{
               "clientName" => "ess-cli",
               "clientType" => "public",
               "scopes" => settings.scopes
             },
             opts
           ),
         true <-
           is_binary(registration["clientId"]) and is_binary(registration["clientSecret"]) and
             is_integer(registration["clientSecretExpiresAt"]),
         {:ok, device} <-
           post(
             oidc <> "/device_authorization",
             %{
               "clientId" => registration["clientId"],
               "clientSecret" => registration["clientSecret"],
               "startUrl" => settings.start_url
             },
             opts
           ),
         true <-
           is_binary(device["deviceCode"]) and is_binary(device["verificationUri"]) and
             is_binary(device["userCode"]) and is_integer(device["expiresIn"]) and
             device["expiresIn"] > 0,
         :ok <- Context.validate_endpoint(device["verificationUri"]),
         true <- URI.parse(device["verificationUri"]).scheme == "https" do
      Browser.notify(
        %{
          verification_uri: device["verificationUri"],
          user_code: device["userCode"],
          verification_uri_complete: device["verificationUriComplete"]
        },
        opts
      )

      deadline =
        System.monotonic_time(:millisecond) +
          min(device["expiresIn"] * 1000, opts[:login_timeout] || 600_000)

      interval =
        if is_integer(device["interval"]) and device["interval"] > 0,
          do: device["interval"] * 1000,
          else: 5000

      with {:ok, response} <- poll(oidc, registration, device, deadline, interval, opts) do
        publish_login(settings, registration, response, env, opts)
      end
    else
      {:error, _} = error -> error
      _ -> Remote.failure(:invalid_sso_login)
    end
  end

  defp publish_login(settings, registration, response, env, opts) do
    with true <-
           is_binary(response["accessToken"]) and is_integer(response["expiresIn"]) and
             response["expiresIn"] > 0,
         {:ok, registration_expiry} <-
           DateTime.from_unix(registration["clientSecretExpiresAt"]),
         {:ok, path} <- login_path(settings, env, opts) do
      now = opts[:now] || DateTime.utc_now()

      token = %{
        "startUrl" => settings.start_url,
        "region" => settings.region,
        "accessToken" => response["accessToken"],
        "expiresAt" => DateTime.add(now, response["expiresIn"], :second) |> DateTime.to_iso8601(),
        "clientId" => registration["clientId"],
        "clientSecret" => registration["clientSecret"],
        "registrationExpiresAt" => DateTime.to_iso8601(registration_expiry)
      }

      token =
        if is_binary(response["refreshToken"]),
          do: Map.put(token, "refreshToken", response["refreshToken"]),
          else: token

      cache(path, token)
    else
      {:error, _} = error -> error
      _ -> Remote.failure(:invalid_sso_response)
    end
  end

  defp post(url, data, opts) do
    with {:ok, body} <-
           Remote.request(
             url,
             "POST",
             [{"Content-Type", "application/json"}],
             JSON.encode!(data),
             opts
           ),
         do: Remote.json(body)
  end

  defp poll(oidc, registration, device, deadline, interval, opts) do
    if System.monotonic_time(:millisecond) >= deadline do
      Remote.failure(:sso_login_timeout)
    else
      body =
        JSON.encode!(%{
          "clientId" => registration["clientId"],
          "clientSecret" => registration["clientSecret"],
          "deviceCode" => device["deviceCode"],
          "grantType" => "urn:ietf:params:oauth:grant-type:device_code"
        })

      case Remote.response(
             oidc <> "/token",
             "POST",
             [{"Content-Type", "application/json"}],
             body,
             opts
           ) do
        {:ok, status, body} when status in 200..299 ->
          Remote.json(body)

        {:ok, 400, body} ->
          with {:ok, data} <- Remote.json(body),
               error = data["error"] || data["__type"],
               true <-
                 error in [
                   "AuthorizationPendingException",
                   "authorization_pending",
                   "SlowDownException",
                   "slow_down"
                 ] do
            next_interval =
              if error in ["SlowDownException", "slow_down"], do: interval + 5000, else: interval

            wait = min(next_interval, max(deadline - System.monotonic_time(:millisecond), 0))
            Process.sleep(wait)
            poll(oidc, registration, device, deadline, next_interval, opts)
          else
            _ -> Remote.failure(:sso_login_refused)
          end

        {:ok, status, _} ->
          Remote.failure(:credential_service_error, status)

        error ->
          error
      end
    end
  end

  defp login_path(settings, env, opts) do
    home = opts[:home] || env["HOME"] || System.user_home!()
    dir = Path.join([home, ".aws", "sso", "cache"])
    existed = File.dir?(dir)

    with :ok <- File.mkdir_p(dir),
         :ok <- if(existed, do: :ok, else: File.chmod(dir, 0o700)) do
      key =
        Base.encode16(:crypto.hash(:sha, settings.session || settings.start_url), case: :lower)

      {:ok, Path.join(dir, key <> ".json")}
    else
      _ -> Remote.failure(:sso_cache_write_failed)
    end
  end

  @doc "Deletes only the selected profile's session token; unrelated cache entries remain."
  def logout(config, stores, env, opts \\ []) do
    with {:ok, settings} <- settings(config, stores, false) do
      home = opts[:home] || env["HOME"] || System.user_home!()
      directory = Path.join([home, ".aws", "sso", "cache"])

      key =
        Base.encode16(:crypto.hash(:sha, settings.session || settings.start_url), case: :lower)

      path = Path.join(directory, key <> ".json")

      case File.lstat(directory) do
        {:error, :enoent} ->
          {:ok, %{removed: false}}

        {:ok, %File.Stat{type: :directory}} ->
          case File.lstat(path) do
            {:error, :enoent} ->
              {:ok, %{removed: false}}

            {:ok, %File.Stat{type: :regular}} ->
              case File.rm(path) do
                :ok -> {:ok, %{removed: true}}
                {:error, :enoent} -> {:ok, %{removed: false}}
                _ -> Remote.failure(:sso_cache_delete_failed)
              end

            _ ->
              Remote.failure(:sso_cache_delete_failed)
          end

        _ ->
          Remote.failure(:sso_cache_delete_failed)
      end
    end
  end

  def resolve(config, stores, env, opts) do
    with {:ok, settings} <- settings(config, stores),
         {:ok, token, path} <- cached_token(settings, env, opts),
         {:ok, token} <- refresh_if_needed(token, path, settings, opts),
         {:ok, endpoint} <- endpoint(settings.region, :sso, opts),
         query =
           URI.encode_query(%{"account_id" => settings.account, "role_name" => settings.role}),
         {:ok, body} <-
           Remote.request(
             endpoint <> "/federation/credentials?" <> query,
             "GET",
             [{"x-amz-sso_bearer_token", token["accessToken"]}],
             nil,
             opts
           ),
         {:ok, data} <- Remote.json(body),
         %{"roleCredentials" => credentials} when is_map(credentials) <- data,
         expiration when is_integer(expiration) <- credentials["expiration"],
         {:ok, expires} <- DateTime.from_unix(expiration, :millisecond),
         true <- DateTime.compare(expires, opts[:now] || DateTime.utc_now()) == :gt,
         true <- is_binary(credentials["sessionToken"]) and credentials["sessionToken"] != "" do
      Record.new(
        %{
          access_key_id: credentials["accessKeyId"],
          secret_access_key: credentials["secretAccessKey"],
          session_token: credentials["sessionToken"],
          expires_at: expires
        },
        %{provider: :sso}
      )
    else
      {:error, _} = error -> error
      _ -> Remote.failure(:invalid_sso_response)
    end
  end

  defp settings(config, stores, require_role \\ true) do
    session = config["sso_session"]
    session_config = if session, do: stores.sections["sso-session " <> session], else: config

    if is_map(session_config) do
      settings = %{
        start_url: session_config["sso_start_url"],
        region: session_config["sso_region"],
        account: config["sso_account_id"],
        role: config["sso_role_name"],
        session: session,
        scopes: scopes(session_config["sso_registration_scopes"] || "sso:account:access")
      }

      if Enum.all?(
           [settings.start_url, settings.region] ++
             if(require_role, do: [settings.account, settings.role], else: []),
           &(is_binary(&1) and &1 != "")
         ) and Enum.all?(settings.scopes, &(is_binary(&1) and &1 != "")) and settings.scopes != [] and
           Regex.match?(~r/^[a-z0-9]+(?:-[a-z0-9]+)+$/, settings.region) and
           Context.validate_endpoint(settings.start_url) == :ok and
           URI.parse(settings.start_url).scheme == "https",
         do: {:ok, settings},
         else: Remote.failure(:invalid_sso_config)
    else
      Remote.failure(:invalid_sso_config)
    end
  end

  defp scopes(value) when is_binary(value),
    do: value |> String.split(",", trim: true) |> Enum.map(&String.trim/1)

  defp scopes(_value), do: []

  defp cached_token(settings, env, opts) do
    home = opts[:home] || env["HOME"] || System.user_home!()
    key = Base.encode16(:crypto.hash(:sha, settings.session || settings.start_url), case: :lower)
    path = Path.join([home, ".aws", "sso", "cache", key <> ".json"])

    with {:ok, body} <- Remote.file(path),
         {:ok, token} <- Remote.json(body),
         true <- is_binary(token["accessToken"]) and token["accessToken"] != "",
         true <-
           Enum.all?(
             ~w(expiresAt refreshToken clientId clientSecret registrationExpiresAt),
             &(is_nil(token[&1]) or is_binary(token[&1]))
           ) do
      {:ok, token, path}
    else
      _ -> Remote.failure(:sso_login_required)
    end
  end

  defp refresh_if_needed(token, path, settings, opts) do
    now = opts[:now] || DateTime.utc_now()

    with {:ok, expiration, _} <- DateTime.from_iso8601(token["expiresAt"] || "") do
      cond do
        DateTime.diff(expiration, now) > 900 ->
          {:ok, token}

        settings.session == nil or not refreshable?(token, now) ->
          if DateTime.compare(expiration, now) == :gt,
            do: {:ok, token},
            else: Remote.failure(:sso_login_required)

        true ->
          case refresh(token, path, settings, opts) do
            {:error, _} = error ->
              if DateTime.compare(expiration, now) == :gt, do: {:ok, token}, else: error

            result ->
              result
          end
      end
    else
      _ -> Remote.failure(:sso_login_required)
    end
  end

  defp refreshable?(token, now) do
    with true <-
           Enum.all?(
             ~w(clientId clientSecret refreshToken),
             &(is_binary(token[&1]) and token[&1] != "")
           ),
         {:ok, expires, _} <- DateTime.from_iso8601(token["registrationExpiresAt"] || "") do
      DateTime.compare(expires, now) == :gt
    else
      _ -> false
    end
  end

  defp refresh(token, path, settings, opts) do
    now = opts[:now] || DateTime.utc_now()

    with true <-
           Enum.all?(
             ~w(clientId clientSecret refreshToken),
             &(is_binary(token[&1]) and token[&1] != "")
           ),
         {:ok, expires, _} <- DateTime.from_iso8601(token["registrationExpiresAt"] || ""),
         true <- DateTime.compare(expires, now) == :gt,
         {:ok, endpoint} <- endpoint(settings.region, :oidc, opts),
         body =
           JSON.encode!(%{
             "clientId" => token["clientId"],
             "clientSecret" => token["clientSecret"],
             "refreshToken" => token["refreshToken"],
             "grantType" => "refresh_token"
           }),
         {:ok, body} <-
           Remote.request(
             endpoint <> "/token",
             "POST",
             [{"Content-Type", "application/json"}],
             body,
             opts
           ),
         {:ok, response} <- Remote.json(body),
         true <-
           is_binary(response["accessToken"]) and is_integer(response["expiresIn"]) and
             response["expiresIn"] > 0 do
      updated =
        Map.merge(token, %{
          "accessToken" => response["accessToken"],
          "expiresAt" =>
            DateTime.add(now, response["expiresIn"], :second) |> DateTime.to_iso8601(),
          "refreshToken" => response["refreshToken"] || token["refreshToken"]
        })

      with :ok <- cache(path, updated), do: {:ok, updated}
    else
      {:error, _} = error -> error
      _ -> Remote.failure(:sso_login_required)
    end
  end

  defp endpoint(region, service, opts) do
    suffix = if String.starts_with?(region, "cn-"), do: "amazonaws.com.cn", else: "amazonaws.com"
    override = if service == :sso, do: opts[:sso_endpoint], else: opts[:oidc_endpoint]

    endpoint =
      override ||
        if(service == :sso,
          do: "https://portal.sso.#{region}.#{suffix}",
          else: "https://oidc.#{region}.#{suffix}"
        )

    if Context.validate_endpoint(endpoint) == :ok and
         (override != nil or URI.parse(endpoint).scheme == "https"),
       do: {:ok, endpoint},
       else: Remote.failure(:invalid_sso_config)
  end

  defp cache(path, token) do
    temporary = path <> ".ess-" <> Base.url_encode64(:crypto.strong_rand_bytes(9), padding: false)

    result =
      with {:ok, file} <- File.open(temporary, [:write, :binary, :exclusive]) do
        result =
          with :ok <- File.chmod(temporary, 0o600),
               :ok <- IO.binwrite(file, JSON.encode!(token)),
               :ok <- :file.sync(file),
               do: :ok

        File.close(file)
        with :ok <- result, do: File.rename(temporary, path)
      end

    if result != :ok, do: File.rm(temporary)
    if result == :ok, do: :ok, else: Remote.failure(:sso_cache_write_failed)
  end
end
