defmodule ExStorageServiceCli.S3.RegionRedirect do
  @moduledoc "One validated S3 regional correction; never consumes a Location or an old signed request."
  alias ExStorageServiceCli.{Context, Error, S3.EndpointRules}

  @doc "Returns a fresh context for re-encoding/signing, preserving the original custom endpoint policy."
  def correct(%Context{} = context, operation, input, %Error{} = error, headers, opts \\ []) do
    region = region(headers, opts[:error_region] || error.details[:bucket_region])
    corrections = opts[:region_corrections] || context.options[:region_corrections] || 0
    bucket = input["Bucket"]

    if context.frontend == :aws and error.category == :service and corrections == 0 and
         opts[:replayable] != false and is_binary(bucket) and bucket != "" and
         not String.starts_with?(bucket, "arn:") and eligible?(operation, error, region) do
      region = discover(region, opts[:discover_region])
      correction(context, operation, input, region, opts)
    else
      :stop
    end
  end

  defp correction(context, operation, input, region, opts) do
    if valid_region?(region) and
         EndpointRules.partition(region)["name"] ==
           EndpointRules.partition(context.region)["name"] do
      corrected = %{
        context
        | region: region,
          options: Map.put(context.options, :region_corrections, 1)
      }

      # Endpoint rules reapply the original endpoint override, ARN restrictions,
      # FIPS/dualstack settings and modeled authentication contract. No response URL is used.
      case EndpointRules.resolve(corrected, operation, input, opts) do
        {:ok, _contract} -> {:ok, corrected}
        {:error, _} -> :stop
      end
    else
      :stop
    end
  end

  defp eligible?(operation, error, region) do
    error.http_status in [301, 302, 307] or error.code == "PermanentRedirect" or
      (error.code == "AuthorizationHeaderMalformed" and is_binary(region)) or
      (error.code == "IllegalLocationConstraintException" and operation != "CreateBucket") or
      (operation == "HeadObject" and error.http_status in [301, 400]) or
      (operation == "HeadBucket" and error.http_status in [301, 400] and is_binary(region))
  end

  defp discover(region, function) when region in [nil, ""] and is_function(function, 0),
    do: function.()

  defp discover(region, _function), do: region

  defp region(headers, fallback) do
    values =
      for {name, value} <- headers,
          is_binary(name),
          String.downcase(name) == "x-amz-bucket-region",
          entry <- List.wrap(value),
          do: entry

    case values do
      [] -> fallback
      [value] -> value
      _ -> :ambiguous
    end
  end

  defp valid_region?(region),
    do:
      is_binary(region) and byte_size(region) <= 63 and
        Regex.match?(~r/^[a-z]{2,4}(?:-[a-z0-9]+)+-[0-9]+$/, region)
end
