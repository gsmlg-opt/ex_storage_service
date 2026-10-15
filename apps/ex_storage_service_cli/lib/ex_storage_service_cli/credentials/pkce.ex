defmodule ExStorageServiceCli.Credentials.PKCE do
  @moduledoc "Bounded invocation-owned IAM Identity Center authorization-code login."
  alias ExStorageServiceCli.Credentials.Remote
  @callback_path "/oauth/callback"

  def authorize(settings, oidc, opts) do
    timeout = opts[:login_timeout] || 600_000

    with true <- is_integer(timeout) and timeout > 0 and timeout <= 600_000,
         {:ok, listener} <-
           :gen_tcp.listen(0, [
             :binary,
             packet: :http_bin,
             active: false,
             ip: {127, 0, 0, 1},
             backlog: 8
           ]) do
      try do
        {:ok, {{127, 0, 0, 1}, port}} = :inet.sockname(listener)
        redirect = "http://127.0.0.1:#{port}" <> @callback_path
        state = random(32)
        verifier = random(48)
        challenge = Base.url_encode64(:crypto.hash(:sha256, verifier), padding: false)
        deadline = System.monotonic_time(:millisecond) + timeout
        scopes = settings.scopes

        with {:ok, registration} <-
               post(
                 oidc <> "/client/register",
                 %{
                   "clientName" => "ess-cli",
                   "clientType" => "public",
                   "grantTypes" => ["authorization_code", "refresh_token"],
                   "redirectUris" => ["http://127.0.0.1" <> @callback_path],
                   "issuerUrl" => settings.start_url,
                   "scopes" => scopes
                 },
                 opts
               ),
             true <-
               is_binary(registration["clientId"]) and is_binary(registration["clientSecret"]) and
                 is_integer(registration["clientSecretExpiresAt"]),
             url =
               oidc <>
                 "/authorize?" <>
                 URI.encode_query(%{
                   "response_type" => "code",
                   "client_id" => registration["clientId"],
                   "redirect_uri" => redirect,
                   "state" => state,
                   "code_challenge_method" => "S256",
                   "code_challenge" => challenge,
                   "scopes" => Enum.join(scopes, " ")
                 }),
             :ok <- notify(url, opts),
             {:ok, code} <- accept(listener, state, port, deadline),
             {:ok, token} <-
               post(
                 oidc <> "/token",
                 %{
                   "grantType" => "authorization_code",
                   "clientId" => registration["clientId"],
                   "clientSecret" => registration["clientSecret"],
                   "redirectUri" => redirect,
                   "codeVerifier" => verifier,
                   "code" => code
                 },
                 opts
               ) do
          {:ok, registration, token}
        else
          {:error, _} = error -> error
          _ -> Remote.failure(:invalid_sso_response)
        end
      after
        :gen_tcp.close(listener)
      end
    else
      _ -> Remote.failure(:invalid_sso_login)
    end
  end

  def notify(url, opts) do
    ExStorageServiceCli.Credentials.Browser.notify(
      %{
        verification_uri: url,
        verification_uri_complete: url,
        user_code: nil
      },
      opts
    )
  end

  defp accept(listener, expected_state, port, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      Remote.failure(:sso_login_timeout)
    else
      case :gen_tcp.accept(listener, min(remaining, 1000)) do
        {:ok, socket} ->
          result =
            try do
              callback(socket, expected_state, port, deadline)
            after
              :gen_tcp.close(socket)
            end

          result

        {:error, :timeout} ->
          accept(listener, expected_state, port, deadline)

        _ ->
          Remote.failure(:sso_login_refused)
      end
    end
  end

  defp callback(socket, expected_state, port, deadline) do
    result =
      with {:ok, {:http_request, :GET, {:abs_path, target}, _}} <-
             receive_packet(socket, deadline),
           true <- byte_size(target) <= 8192,
           {:ok, headers} <- headers(socket, deadline, 0, 0, []),
           [host] <-
             for({name, value} <- headers, String.downcase(to_string(name)) == "host", do: value),
           true <- host == "127.0.0.1:#{port}",
           %URI{path: @callback_path, query: query} when is_binary(query) <- URI.parse(target),
           pairs = Enum.to_list(URI.query_decoder(query)),
           [state] <- for({"state", value} <- pairs, do: value),
           true <- state == expected_state,
           [code] <- for({"code", value} <- pairs, do: value),
           true <-
             code != "" and byte_size(code) <= 4096 and not List.keymember?(pairs, "error", 0) do
        {:ok, code}
      else
        _ -> Remote.failure(:sso_login_refused)
      end

    :gen_tcp.send(
      socket,
      "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 31\r\nConnection: close\r\n\r\nYou may return to the terminal.\n"
    )

    result
  rescue
    _ in [ArgumentError, URI.Error] -> Remote.failure(:sso_login_refused)
  end

  defp headers(_socket, _deadline, count, bytes, _acc) when count >= 64 or bytes > 16_384,
    do: Remote.failure(:sso_login_refused)

  defp headers(socket, deadline, count, bytes, acc) do
    case receive_packet(socket, deadline) do
      {:ok, :http_eoh} ->
        {:ok, acc}

      {:ok, {:http_header, _, name, _, value}} ->
        headers(
          socket,
          deadline,
          count + 1,
          bytes + byte_size(to_string(name)) + byte_size(value),
          [{name, value} | acc]
        )

      _ ->
        Remote.failure(:sso_login_refused)
    end
  end

  defp receive_packet(socket, deadline),
    do:
      :gen_tcp.recv(socket, 0, min(max(deadline - System.monotonic_time(:millisecond), 1), 5_000))

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

  defp random(bytes), do: :crypto.strong_rand_bytes(bytes) |> Base.url_encode64(padding: false)
end
