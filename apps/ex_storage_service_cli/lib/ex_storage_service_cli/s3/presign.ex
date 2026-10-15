defmodule ExStorageServiceCli.S3.Presign do
  @moduledoc "Shared native sharing with endpoint-aware signing and explicit effective lifetime."
  alias ExStorageServiceCli.{Context, Error, SigV4}
  alias ExStorageServiceCli.S3.{Encoder, Endpoint, SigV4a}
  alias ExStorageServiceCli.Credentials.DirectorySession

  def url(context, target, method \\ "GET", opts \\ []) do
    expires = opts[:expires] || 3600
    now = opts[:now] || DateTime.utc_now()
    operation = if method == "PUT", do: "PutObject", else: "GetObject"
    input = %{"Bucket" => target.bucket, "Key" => target.key}
    input = if opts[:version_id], do: Map.put(input, "VersionId", opts[:version_id]), else: input

    with :ok <- validate(context, target, method, expires),
         :ok <- version(method, opts[:version_id]),
         {:ok, context} <- Context.refresh(context),
         {:ok, request} <- Encoder.encode(operation, input),
         {:ok, url, endpoint} <- Endpoint.resolve(context, request, input, opts),
         {:ok, context, session_options} <- session(context, target, endpoint, method),
         {:ok, effective} <- lifetime(context, expires, now) do
      signer = if endpoint.auth == :sigv4a, do: SigV4a, else: SigV4

      signed =
        signer.presign_url(
          method,
          url,
          session_options ++
            [
              access_key_id: context.access_key_id,
              secret_access_key: context.secret_access_key,
              session_token: context.session_token,
              region: endpoint.region,
              service: endpoint.service,
              signing_region_set: endpoint.signing_region_set,
              expires: expires,
              now: now
            ]
        )

      {:ok,
       %{
         "url" => signed,
         "method" => method,
         "expires" => expires,
         "effective_expires" => effective,
         "bucket" => target.bucket,
         "key" => target.key
       }}
    end
  end

  def lifetime(context, expires, now) do
    remaining =
      case context.credentials do
        %{expires_at: %DateTime{} = expiry} -> DateTime.diff(expiry, now, :second)
        _ -> expires
      end

    if remaining > 0,
      do: {:ok, min(expires, remaining)},
      else: invalid("Sharing credentials have expired")
  end

  defp validate(context, target, method, expires) do
    cond do
      context.unsigned or context.access_key_id == nil or context.secret_access_key == nil ->
        invalid("Sharing requires signing credentials")

      target.key in [nil, ""] ->
        invalid("Sharing requires an object key")

      method not in ~w(GET PUT HEAD DELETE) ->
        invalid("Sharing method must be GET, PUT, HEAD or DELETE")

      not is_integer(expires) or expires not in 1..604_800 ->
        invalid("Sharing expiry must be between 1 and 604800 seconds")

      true ->
        :ok
    end
  end

  defp version(_, nil), do: :ok
  defp version("GET", id) when is_binary(id) and byte_size(id) in 1..8192, do: :ok

  defp version(_, _),
    do: invalid("Selected versions require GET and a nonempty version identifier")

  defp session(context, _target, %{session_auth: nil}, _method), do: {:ok, context, []}

  defp session(%{directory_sessions: manager} = context, target, endpoint, method)
       when is_pid(manager) do
    with {:ok, credentials} <-
           DirectorySession.resolve(
             manager,
             context,
             target.bucket,
             endpoint,
             if(method in ["GET", "HEAD"], do: "ReadOnly", else: "ReadWrite")
           ) do
      {:ok,
       %{
         context
         | credentials: credentials,
           access_key_id: credentials.access_key_id,
           secret_access_key: credentials.secret_access_key,
           session_token: credentials.session_token
       }, [session_token_query: "X-Amz-S3session-Token"]}
    end
  end

  defp session(_context, _target, _endpoint, _method),
    do: invalid("Directory-bucket sharing requires an invocation-owned session")

  defp invalid(message), do: {:error, Error.new(:client, message, code: :invalid_share)}
end
