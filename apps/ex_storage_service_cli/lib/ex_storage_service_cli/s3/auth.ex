defmodule ExStorageServiceCli.S3.Auth do
  @moduledoc "Signs a fresh request using one complete credential context."
  alias ExStorageServiceCli.{Context, Error, SigV4, S3.Request}
  alias ExStorageServiceCli.S3.SigV4a

  def prepare(%Request{} = request, _url, %Context{unsigned: true}, _opts),
    do: {:ok, request}

  def prepare(%Request{} = request, url, %Context{} = context, opts) do
    if nonempty?(context.access_key_id) and nonempty?(context.secret_access_key) do
      signing_options = [
        access_key_id: context.access_key_id,
        secret_access_key: context.secret_access_key,
        session_token: context.session_token,
        session_token_header: opts[:session_token_header],
        signing_region_set: opts[:signing_region_set],
        region: context.region,
        service: opts[:service] || "s3"
      ]

      signing_options =
        if opts[:now], do: Keyword.put(signing_options, :now, opts[:now]), else: signing_options

      signing_options =
        if request.payload_hash,
          do: Keyword.put(signing_options, :payload_hash, request.payload_hash),
          else: signing_options

      if is_pid(request.body) and request.payload_hash == nil do
        {:error, Error.new(:client, "Replayable streams require a precomputed payload hash")}
      else
        signer = if opts[:auth] == :sigv4a, do: SigV4a, else: SigV4

        auth =
          signer.sign_headers(
            request.method,
            url,
            request.headers,
            request.body || "",
            signing_options
          )

        names = MapSet.new(Enum.map(auth, fn {name, _} -> String.downcase(name) end))

        headers =
          Enum.reject(request.headers, fn {name, _} ->
            MapSet.member?(names, String.downcase(name))
          end) ++ auth

        {:ok, %{request | headers: headers}}
      end
    else
      {:error,
       Error.new(:credentials, "A complete credential pair or explicit unsigned mode is required")}
    end
  end

  defp nonempty?(value), do: is_binary(value) and value != ""
end
