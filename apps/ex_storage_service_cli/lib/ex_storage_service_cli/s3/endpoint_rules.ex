defmodule ExStorageServiceCli.S3.EndpointRules do
  @moduledoc "Pinned S3 endpoint rules, model-bound context parameters, and operation-specific auth contracts."
  alias ExStorageServiceCli.{Context, Error}
  alias ExStorageServiceCli.S3.{EndpointRules.Evaluator, OperationCatalog}

  defdelegate evaluate(parameters), to: Evaluator
  defdelegate partition(region), to: Evaluator

  @doc "Returns a resolved base URL including modeled bucket placement and its signing contract."
  def resolve(%Context{} = context, operation, input, opts \\ []) do
    with {:ok, parameters} <- parameters(context, operation, input, opts),
         {:ok, result} <- Evaluator.evaluate(parameters),
         :ok <- Context.validate_endpoint(result.url),
         {:ok, auth} <- auth(result.properties, context.region) do
      headers = for {name, values} <- result.headers, value <- values, do: {name, value}

      {:ok,
       Map.merge(auth, %{
         endpoint: result.url,
         addressing: :path,
         strip_bucket: is_binary(parameters["Bucket"]),
         headers: headers,
         raw: result
       })}
    end
  end

  @doc "Binds client settings and operation/input context traits to the pinned rule parameters."
  def parameters(%Context{} = context, operation, input, opts \\ []) do
    settings = opts[:s3_settings] || context.options[:s3_settings] || %{}

    endpoint =
      if context.frontend == :native,
        do: context.endpoint,
        else:
          opts[:endpoint] || context.options[:resolved_endpoint] || context.options[:endpoint_url]

    with {:ok, model} <- OperationCatalog.operation(operation),
         {:ok, fips} <-
           boolean(Keyword.get(opts, :use_fips, context.options[:use_fips_endpoint])),
         {:ok, dualstack} <-
           boolean(
             Keyword.get(
               opts,
               :use_dualstack,
               context.options[:use_dualstack_endpoint] || settings["use_dualstack_endpoint"]
             )
           ),
         {:ok, accelerate} <- boolean(settings["use_accelerate_endpoint"]),
         {:ok, arn_region} <- boolean(settings["use_arn_region"], nil),
         {:ok, disable_mrap} <- boolean(settings["disable_multiregion_access_points"]),
         {:ok, disable_express} <- boolean(settings["disable_s3_express_session_auth"], nil) do
      base = %{
        "Region" => context.region,
        "Endpoint" => endpoint,
        "ForcePathStyle" => context.addressing == :path,
        "UseFIPS" => fips,
        "UseDualStack" => dualstack,
        "Accelerate" => accelerate,
        "UseArnRegion" => arn_region,
        "DisableMultiRegionAccessPoints" => disable_mrap,
        "DisableS3ExpressSessionAuth" => disable_express,
        "UseGlobalEndpoint" => settings["us_east_1_regional_endpoint"] == "legacy"
      }

      static =
        Map.new(model["staticContextParams"] || %{}, fn {name, value} ->
          {name, value["value"]}
        end)

      shape = if model["input"], do: OperationCatalog.shape(model["input"]["shape"]), else: %{}

      dynamic =
        Map.new(
          for {member, trait} <- shape["members"] || %{},
              is_map(trait["contextParam"]),
              Map.has_key?(input, member),
              do: {trait["contextParam"]["name"], input[member]}
        )

      {:ok,
       base |> Map.merge(static) |> Map.merge(dynamic) |> Map.merge(opts[:parameters] || %{})}
    end
  end

  defp auth(properties, default_region) do
    case properties["authSchemes"] || [] do
      [] ->
        {:ok,
         %{
           region: default_region,
           service: "s3",
           auth: :sigv4,
           signing_region_set: nil,
           session_auth: nil
         }}

      [scheme | _] ->
        case scheme["name"] do
          "sigv4" ->
            {:ok, contract(scheme, default_region, :sigv4, nil)}

          "sigv4a" ->
            {:ok, contract(scheme, default_region, :sigv4a, nil)}

          "sigv4-s3express" ->
            {:ok, contract(scheme, default_region, :sigv4, :s3express)}

          _ ->
            {:error,
             Error.new(:configuration, "Unsupported modeled endpoint authentication scheme",
               code: :endpoint_auth_scheme
             )}
        end
    end
  end

  defp contract(scheme, default_region, auth, session_auth),
    do: %{
      region: scheme["signingRegion"] || default_region,
      service: scheme["signingName"] || "s3",
      auth: auth,
      signing_region_set: scheme["signingRegionSet"],
      session_auth: session_auth
    }

  defp boolean(value, default \\ false)
  defp boolean(nil, default), do: {:ok, default}
  defp boolean(value, _default) when is_boolean(value), do: {:ok, value}
  defp boolean("true", _default), do: {:ok, true}
  defp boolean("false", _default), do: {:ok, false}

  defp boolean(_value, _default),
    do:
      {:error,
       Error.new(:configuration, "S3 endpoint configuration requires a boolean",
         code: :invalid_endpoint_parameter
       )}
end
