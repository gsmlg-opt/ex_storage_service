defmodule ExStorageServiceCli.S3.PostPolicy do
  @moduledoc "SigV4 browser POST policies with bounded fields and credential-limited expiration."
  alias ExStorageServiceCli.{Context, Error}
  alias ExStorageServiceCli.S3.{Encoder, Endpoint, Presign}
  alias ExStorageServiceCli.Transfer.Limits

  def generate(context, target, opts \\ []) do
    requested = opts[:expires] || 3600
    now = opts[:now] || DateTime.utc_now()

    with :ok <- validate(context, target, requested, opts),
         {:ok, context} <- Context.refresh(context),
         {:ok, expires} <- Presign.lifetime(context, requested, now),
         {:ok, request} <- Encoder.encode("HeadBucket", %{"Bucket" => target.bucket}),
         {:ok, url, %{auth: :sigv4, session_auth: nil} = endpoint} <-
           Endpoint.resolve(context, request, %{"Bucket" => target.bucket}, opts) do
      stamp = Calendar.strftime(now, "%Y%m%d")
      date = Calendar.strftime(now, "%Y%m%dT%H%M%SZ")

      credential =
        context.access_key_id <> "/" <> stamp <> "/" <> endpoint.region <> "/s3/aws4_request"

      fields = %{
        "key" => if(opts[:key_prefix], do: target.key <> "${filename}", else: target.key),
        "x-amz-algorithm" => "AWS4-HMAC-SHA256",
        "x-amz-credential" => credential,
        "x-amz-date" => date
      }

      fields =
        if context.session_token,
          do: Map.put(fields, "x-amz-security-token", context.session_token),
          else: fields

      fields =
        if opts[:content_type],
          do: Map.put(fields, "Content-Type", opts[:content_type]),
          else: fields

      conditions =
        [%{"bucket" => target.bucket}] ++
          Enum.flat_map(Enum.sort(fields), fn
            {"key", value} ->
              if opts[:key_prefix],
                do: [["starts-with", "$key", target.key]],
                else: [%{"key" => value}]

            {key, value} ->
              [%{key => value}]
          end)

      conditions =
        if opts[:max_bytes] != nil,
          do: conditions ++ [["content-length-range", opts[:min_bytes] || 0, opts[:max_bytes]]],
          else: conditions

      expiration =
        now
        |> DateTime.add(expires, :second)
        |> DateTime.truncate(:second)
        |> DateTime.to_iso8601()

      policy =
        JSON.encode!(%{"expiration" => expiration, "conditions" => conditions}) |> Base.encode64()

      key =
        hmac("AWS4" <> context.secret_access_key, stamp)
        |> hmac(endpoint.region)
        |> hmac("s3")
        |> hmac("aws4_request")

      signature = hmac(key, policy) |> Base.encode16(case: :lower)
      fields = fields |> Map.put("policy", policy) |> Map.put("x-amz-signature", signature)
      {:ok, %{"url" => url, "method" => "POST", "fields" => fields, "expires" => expires}}
    else
      {:error, _} = error -> error
      _ -> invalid("POST policies require a general-purpose SigV4 S3 endpoint")
    end
  end

  defp validate(context, target, expires, opts) do
    maximum = Limits.for_context(context).max_single_put_bytes

    cond do
      context.unsigned or context.access_key_id == nil or context.secret_access_key == nil ->
        invalid("POST policies require signing credentials")

      context.capabilities[:provider] == "ess" ->
        invalid("The declared ESS server does not support browser POST uploads")

      target.arn != nil or target.key in [nil, ""] or String.contains?(target.key, "${") ->
        invalid("POST policies require a literal bucket and nonempty object key or prefix")

      not is_integer(expires) or expires not in 1..604_800 ->
        invalid("POST expiry must be between 1 and 604800 seconds")

      opts[:content_type] != nil and
          (not is_binary(opts[:content_type]) or byte_size(opts[:content_type]) > 1024) ->
        invalid("Invalid POST content type")

      opts[:min_bytes] != nil and opts[:max_bytes] == nil ->
        invalid("POST minimum bytes requires maximum bytes")

      opts[:max_bytes] != nil and
          (not is_integer(opts[:max_bytes]) or not is_integer(opts[:min_bytes] || 0) or
             (opts[:min_bytes] || 0) < 0 or opts[:max_bytes] < (opts[:min_bytes] || 0) or
             opts[:max_bytes] > maximum) ->
        invalid("POST content length range exceeds the provider single-upload limits")

      true ->
        :ok
    end
  end

  defp hmac(key, bytes), do: :crypto.mac(:hmac, :sha256, key, bytes)
  defp invalid(message), do: {:error, Error.new(:client, message, code: :invalid_post_policy)}
end
