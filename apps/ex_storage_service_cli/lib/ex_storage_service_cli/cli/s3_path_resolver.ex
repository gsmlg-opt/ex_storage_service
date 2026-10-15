defmodule ExStorageServiceCli.CLI.S3PathResolver do
  @moduledoc "Bounded read-only AWS mv validation of underlying access-point buckets."
  alias ExStorageServiceCli.{Context, Error, SigV4, Target}
  alias ExStorageServiceCli.Credentials.SharedConfig
  alias ExStorageServiceCli.S3.{Encoding, EndpointRules.Evaluator, Errors, Request, Retry, XML}
  alias ExStorageServiceCli.Transfer.Coordinator
  alias ExStorageServiceCli.Transport.HTTPFetch

  @control_path Path.expand(
                  "../../../priv/compatibility/aws/s3control-endpoint-rule-set-1.json",
                  __DIR__
                )
  @sts_path Path.expand("../../../priv/compatibility/aws/sts-endpoint-rule-set-1.json", __DIR__)
  @external_resource @control_path
  @external_resource @sts_path
  @control File.read!(@control_path) |> JSON.decode!()
  @sts File.read!(@sts_path) |> JSON.decode!()
  @limit 1_048_576
  @ap ~r/\Aarn:aws[^:]*:s3:[a-z0-9-]+:(?<account>[0-9]{12}):accesspoint[:\/](?<name>[a-z0-9-]{3,50})\z/
  @outpost ~r/\Aarn:aws[^:]*:s3-outposts:[a-z0-9-]+:(?<account>[0-9]{12}):outpost\/op-[a-zA-Z0-9]+\/accesspoint[:\/][a-z0-9-]{3,50}\z/
  @mrap ~r/\Aarn:aws:s3::(?<account>[0-9]{12}):accesspoint[:\/](?<alias>[a-zA-Z0-9]+\.mrap)\z/
  @warning "warning: Provided s3 paths may resolve to same underlying s3 object(s) and result in deletion instead of being moved. To resolve and validate underlying s3 paths are not the same, specify the --validate-same-s3-paths flag or set the AWS_CLI_S3_MV_VALIDATE_SAME_S3_PATHS environment variable to true. To resolve s3 outposts access point path, the arn must be used instead of the alias."

  def validate(invocation, targets, contexts, options, opts \\ [])

  def validate(
        %{frontend: :s3, command: ["mv"]},
        [%Target{kind: :remote} = source, %Target{kind: :remote} = destination],
        [source_context, destination_context],
        options,
        opts
      ) do
    env = Keyword.get(opts, :env, System.get_env())

    enabled =
      options[:validate_same_s3_paths] == true or
        boolean(env["AWS_CLI_S3_MV_VALIDATE_SAME_S3_PATHS"])

    special = special?(source.bucket) or special?(destination.bucket)

    cond do
      not same_key?(source.key, destination.key) or not special or
          source.bucket == destination.bucket ->
        :ok

      not enabled ->
        if opts[:stderr], do: opts[:stderr].(@warning), else: IO.puts(:stderr, @warning)
        :ok

      true ->
        with {:ok, stores} <- SharedConfig.load(Keyword.put(opts, :load_credentials, false)),
             settings = %{
               env: env,
               stores: stores,
               opts: opts,
               session_region: destination_context.region
             },
             {:ok, left} <-
               resolve(
                 source,
                 %{source_context | region: options[:source_region] || source_context.region},
                 settings
               ),
             {:ok, right} <- resolve(destination, destination_context, settings) do
          if Enum.any?(left, &(&1 in right)),
            do:
              syntax("Cannot mv a file onto itself: #{source.original} - #{destination.original}"),
            else: :ok
        end
    end
  end

  def validate(_invocation, _targets, _contexts, _options, _opts), do: :ok

  defp special?(bucket), do: kind(bucket) != :bucket

  defp same_key?(source, destination),
    do:
      source == destination or
        (String.ends_with?(destination, "/") and
           source == destination <> List.last(String.split(source, "/")))

  defp kind(bucket) do
    cond do
      match = Regex.named_captures(@ap, bucket) ->
        {:access_point, match["account"], match["name"]}

      match = Regex.named_captures(@outpost, bucket) ->
        {:access_point, match["account"], bucket}

      match = Regex.named_captures(@mrap, bucket) ->
        {:mrap, match["account"], match["alias"]}

      String.ends_with?(bucket, "-s3alias") ->
        :alias

      String.ends_with?(bucket, "--op-s3") ->
        :outposts_alias

      true ->
        :bucket
    end
  end

  defp resolve(target, context, settings) do
    case kind(target.bucket) do
      {:access_point, account, name} ->
        access_point(account, name, context, settings)

      {:mrap, account, name} ->
        mrap(account, name, nil, MapSet.new(), context, settings)

      :alias ->
        with {:ok, root} <-
               request(
                 :sts,
                 "GetCallerIdentity",
                 %{},
                 %{context | region: settings.session_region},
                 settings
               ),
             [result] <- XML.children(root, "GetCallerIdentityResult"),
             account = XML.child_text(result, "Account"),
             true <- Regex.match?(~r/\A[0-9]{12}\z/, account) do
          access_point(account, target.bucket, context, settings)
        else
          {:error, _} = error -> error
          _ -> protocol()
        end

      :outposts_alias ->
        syntax(
          "Can't resolve underlying bucket name of s3 outposts access point alias. Use arn instead to resolve the bucket name and validate the mv command."
        )

      :bucket ->
        {:ok, [target.bucket]}
    end
  end

  defp access_point(account, name, context, settings) do
    with {:ok, root} <-
           request(
             :control,
             "GetAccessPoint",
             %{"AccountId" => account, "AccessPointName" => name},
             context,
             settings
           ),
         bucket = XML.child_text(root, "Bucket"),
         true <- bucket != "" do
      {:ok, [bucket]}
    else
      {:error, _} = error -> error
      _ -> protocol()
    end
  end

  defp mrap(account, name, token, seen, context, settings) do
    if MapSet.size(seen) >= 1024 or (token != nil and MapSet.member?(seen, token)) do
      {:error, Error.new(:protocol, "Multi-region access point pagination exceeded its bounds")}
    else
      with {:ok, root} <-
             request(
               :control,
               "ListMultiRegionAccessPoints",
               %{"AccountId" => account, "NextToken" => token},
               context,
               settings
             ),
           [points] <- XML.children(root, "AccessPoints") do
        point =
          Enum.find(XML.children(points, "AccessPoint"), &(XML.child_text(&1, "Alias") == name))

        next = XML.child_text(root, "NextToken")

        cond do
          point != nil ->
            with [regions] <- XML.children(point, "Regions"),
                 buckets when buckets != [] <-
                   Enum.map(XML.children(regions, "Region"), &XML.child_text(&1, "Bucket")),
                 true <- Enum.all?(buckets, &(&1 != "")) do
              {:ok, buckets}
            else
              _ -> protocol()
            end

          next != "" ->
            mrap(account, name, next, MapSet.put(seen, token), context, settings)

          true ->
            syntax(
              "Couldn't find multi-region access point with alias #{name} in account #{account}"
            )
        end
      else
        {:error, _} = error -> error
        _ -> protocol()
      end
    end
  end

  defp request(service, operation, input, context, settings) do
    opts = Retry.options(context.options, settings.opts)

    Retry.run(operation, opts, fn _attempt, _remaining ->
      with {:ok, context} <- Context.refresh(context),
           {:ok, endpoint} <- endpoint(service, input, context, settings),
           {:ok, request, url} <- encode(service, operation, input, endpoint),
           {:ok, request} <- sign(request, url, service, context, endpoint, settings.opts),
           {:ok, lease} <- reserve(context) do
        try do
          transport = [
            timeout: context.timeout,
            connect_timeout: context.connect_timeout,
            ssl: context.tls,
            max_control_bytes: @limit,
            signal: settings.opts[:signal],
            rate_limiter: context.options[:rate_limiter],
            limit_upload: context.options[:limit_upload],
            limit_download: context.options[:limit_download],
            rate_endpoint: endpoint.url
          ]

          case HTTPFetch.request(url, %{request | lease: lease}, transport) do
            {:ok, %{status: status} = response} when status in 200..299 ->
              XML.parse(response.body, max_bytes: @limit)

            {:ok, response} ->
              {:error, Errors.from_response(operation, response, context)}

            error ->
              error
          end
        after
          Coordinator.release(lease)
        end
      end
    end)
  end

  defp reserve(%{coordinator: nil}), do: {:ok, nil}
  defp reserve(context), do: Coordinator.acquire(context.coordinator, @limit)

  defp sign(request, _url, _service, %{unsigned: true}, _endpoint, _opts), do: {:ok, request}

  defp sign(request, url, service, context, endpoint, opts) do
    auth =
      List.first(endpoint.properties["authSchemes"] || []) ||
        %{"signingName" => if(service == :sts, do: "sts", else: "s3")}

    signed =
      SigV4.sign_headers(request.method, url, request.headers, request.body || "",
        access_key_id: context.access_key_id,
        secret_access_key: context.secret_access_key,
        session_token: context.session_token,
        region: auth["signingRegion"] || context.region,
        service: auth["signingName"],
        include_content_sha256: service != :sts,
        now: opts[:now] || DateTime.utc_now()
      )

    {:ok, %{request | headers: request.headers ++ signed}}
  end

  defp encode(_service, operation, input, endpoint) do
    {method, path, query, body, headers} =
      case operation do
        "GetCallerIdentity" ->
          {"POST", "/", [], "Action=GetCallerIdentity&Version=2011-06-15",
           [{"content-type", "application/x-www-form-urlencoded; charset=utf-8"}]}

        "GetAccessPoint" ->
          name = input["AccessPointName"]

          {"GET", "/v20180820/accesspoint/" <> Encoding.encode(name), [], nil,
           [{"x-amz-account-id", input["AccountId"]}]}

        "ListMultiRegionAccessPoints" ->
          query = if input["NextToken"], do: [{"nextToken", input["NextToken"]}], else: []

          {"GET", "/v20180820/mrap/instances", query, nil,
           [
             {"x-amz-account-id", input["AccountId"]},
             {"x-amz-checksum-crc64nvme", "AAAAAAAAAAA="}
           ]}
      end

    uri = URI.parse(endpoint.url)

    url =
      URI.to_string(%{
        uri
        | path: String.trim_trailing(uri.path || "", "/") <> path,
          query: if(query == [], do: nil, else: Encoding.query(query))
      })

    extra = for {name, values} <- endpoint.headers, value <- values, do: {name, value}
    names = MapSet.new(extra, &elem(&1, 0))
    headers = Enum.reject(headers, &MapSet.member?(names, elem(&1, 0))) ++ extra
    {:ok, %Request{operation: operation, method: method, headers: headers, body: body}, url}
  end

  @doc false
  def endpoint(service, input, context, settings) do
    profile = settings.stores.config[context.profile] || %{}
    env = settings.env
    name = if service == :sts, do: "sts", else: "s3_control"

    with {:ok, custom} <- configured_endpoint(name, profile, settings),
         :ok <- if(custom, do: Context.validate_endpoint(custom), else: :ok) do
      parameters = %{
        "Region" => context.region,
        "Endpoint" => custom,
        "UseFIPS" =>
          custom == nil and boolean(env["AWS_USE_FIPS_ENDPOINT"] || profile["use_fips_endpoint"]),
        "UseDualStack" =>
          custom == nil and
            boolean(env["AWS_USE_DUALSTACK_ENDPOINT"] || profile["use_dualstack_endpoint"])
      }

      parameters =
        if service == :control,
          do:
            parameters
            |> Map.put("RequiresAccountId", true)
            |> Map.put(
              "UseArnRegion",
              boolean(
                env["AWS_S3_USE_ARN_REGION"] || profile["s3_use_arn_region"] ||
                  get_in(profile, ["s3", "use_arn_region"])
              )
            )
            |> Map.merge(Map.drop(input, ["NextToken"])),
          else: Map.put(parameters, "UseGlobalEndpoint", false)

      case Evaluator.evaluate(parameters, if(service == :sts, do: @sts, else: @control)) do
        {:ok, result} ->
          with :ok <- Context.validate_endpoint(result.url), do: {:ok, result}

        {:error, error} ->
          {:error,
           %{
             error
             | category: :client,
               message: "Unsupported configuration when using S3 Control: " <> error.message
           }}
      end
    end
  end

  defp configured_endpoint(name, profile, settings) do
    env = settings.env

    cond do
      boolean(
        env["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] || profile["ignore_configured_endpoint_urls"]
      ) ->
        {:ok, nil}

      nonempty?(env["AWS_ENDPOINT_URL_" <> String.upcase(name)]) ->
        {:ok, env["AWS_ENDPOINT_URL_" <> String.upcase(name)]}

      nonempty?(env["AWS_ENDPOINT_URL"]) ->
        {:ok, env["AWS_ENDPOINT_URL"]}

      profile["services"] ->
        case settings.stores.sections["services " <> profile["services"]] do
          nil ->
            {:error,
             Error.new(:configuration, "Configured AWS service endpoint section does not exist")}

          section ->
            case section[name] do
              nil ->
                {:ok, profile["endpoint_url"]}

              service when is_map(service) ->
                {:ok, service["endpoint_url"] || profile["endpoint_url"]}

              _ ->
                {:error, Error.new(:configuration, "Invalid AWS service endpoint configuration")}
            end
        end

      true ->
        {:ok, profile["endpoint_url"]}
    end
  end

  defp nonempty?(value), do: is_binary(value) and value != ""
  defp boolean(value) when is_binary(value), do: String.downcase(value) == "true"
  defp boolean(value), do: value == true
  defp syntax(message), do: {:error, Error.new(:syntax, message, code: :same_s3_path)}
  defp protocol, do: {:error, Error.new(:protocol, "Invalid AWS path resolution response")}
end
