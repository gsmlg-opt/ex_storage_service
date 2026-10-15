defmodule ExStorageServiceCli.S3.EndpointRules.Evaluator do
  @moduledoc false
  alias ExStorageServiceCli.{Error, S3.Encoding}
  @rules_path Path.expand("../../../../priv/compatibility/aws/endpoint-rule-set-1.json", __DIR__)
  @partitions_path Path.expand("../../../../priv/compatibility/aws/partitions.json", __DIR__)
  @external_resource @rules_path
  @external_resource @partitions_path
  @rules @rules_path |> File.read!() |> JSON.decode!()
  @partitions @partitions_path |> File.read!() |> JSON.decode!() |> Map.fetch!("partitions")

  def evaluate(parameters), do: evaluate(parameters, @rules)

  @doc false
  def evaluate(parameters, model) when is_map(parameters) and is_map(model) do
    with {:ok, scope} <- parameters(parameters, model["parameters"]) do
      case rules(model["rules"], scope) do
        :no_match -> failure("No endpoint rule matched the supplied parameters")
        result -> result
      end
    end
  end

  def partition(region), do: function("aws.partition", [region])

  defp parameters(input, definitions) do
    Enum.reduce_while(definitions, {:ok, %{}}, fn {name, spec}, {:ok, scope} ->
      value = Map.get(input, name, spec["default"])

      valid =
        is_nil(value) or (spec["type"] == "String" and is_binary(value)) or
          (spec["type"] == "Boolean" and is_boolean(value))

      cond do
        not valid ->
          {:halt, failure("Invalid endpoint parameter type", :invalid_endpoint_parameter)}

        is_nil(value) and spec["required"] == true ->
          {:halt, failure("Required endpoint parameter is missing", :invalid_endpoint_parameter)}

        true ->
          {:cont, {:ok, Map.put(scope, name, value)}}
      end
    end)
  end

  defp rules([], _scope), do: :no_match

  defp rules([rule | rest], scope) do
    result =
      with {:ok, local} <- conditions(rule["conditions"] || [], scope) do
        case rule["type"] do
          "tree" ->
            rules(rule["rules"], local)

          "endpoint" ->
            {:ok,
             %{
               url: value(rule["endpoint"]["url"], local),
               properties: properties(rule["endpoint"]["properties"] || %{}, local),
               headers: properties(rule["endpoint"]["headers"] || %{}, local)
             }}

          "error" ->
            failure(value(rule["error"], local))
        end
      else
        :no_match -> :no_match
      end

    if result == :no_match, do: rules(rest, scope), else: result
  end

  defp conditions([], scope), do: {:ok, scope}

  defp conditions([condition | rest], scope) do
    result = value(condition, scope)

    if result in [nil, false] do
      :no_match
    else
      scope = if condition["assign"], do: Map.put(scope, condition["assign"], result), else: scope
      conditions(rest, scope)
    end
  end

  defp value(%{"fn" => name, "argv" => args}, scope),
    do: function(name, Enum.map(args, &value(&1, scope)))

  defp value(%{"ref" => name}, scope), do: scope[name]

  defp value(template, scope) when is_binary(template) do
    Regex.replace(~r/\{([^{}]+)\}/, template, fn _match, reference ->
      reference |> String.split("#") |> Enum.reduce(scope, &attribute(&2, &1)) |> to_string()
    end)
  end

  defp value(literal, _scope), do: literal

  defp properties(map, scope) when is_map(map),
    do: Map.new(map, fn {key, item} -> {key, properties(item, scope)} end)

  defp properties(list, scope) when is_list(list), do: Enum.map(list, &properties(&1, scope))
  defp properties(item, scope), do: value(item, scope)

  defp function("isSet", [value]), do: value != nil
  defp function("not", [value]), do: value in [nil, false, "", [], %{}]
  defp function("booleanEquals", [left, right]), do: left == right
  defp function("stringEquals", [left, right]), do: left == right
  defp function("getAttr", [value, path]), do: attribute(value, path)
  defp function("uriEncode", [nil]), do: nil
  defp function("uriEncode", [value]), do: Encoding.encode(value)
  defp function("isValidHostLabel", [value, subdomains]), do: valid_host?(value, subdomains)

  defp function("aws.isVirtualHostableS3Bucket", [value, subdomains]),
    do: virtual_bucket?(value, subdomains)

  defp function("aws.partition", [region]) do
    partition =
      Enum.find(@partitions, fn p ->
        is_binary(region) and
          (Map.has_key?(p["regions"], region) or
             Regex.match?(Regex.compile!(p["regionRegex"]), region))
      end) || hd(@partitions)

    Map.put(partition["outputs"], "name", partition["id"])
  end

  defp function("aws.parseArn", ["arn:" <> _ = arn]) do
    case String.split(arn, ":", parts: 6) do
      ["arn", partition, service, region, account, resource]
      when partition != "" and service != "" and resource != "" ->
        %{
          "partition" => partition,
          "service" => service,
          "region" => region,
          "accountId" => account,
          "resourceId" => resource |> String.replace(":", "/") |> String.split("/")
        }

      _ ->
        nil
    end
  end

  defp function("aws.parseArn", [_]), do: nil

  defp function("substring", [value, start, stop, reverse]) when is_binary(value) do
    if start >= stop or byte_size(value) < stop or not ascii?(value),
      do: nil,
      else:
        binary_part(value, if(reverse, do: byte_size(value) - stop, else: start), stop - start)
  end

  defp function("parseURL", [nil]), do: nil

  defp function("parseURL", [url]) do
    case URI.new(url) do
      {:ok, %URI{scheme: scheme, query: query} = uri}
      when scheme in ["http", "https"] and query in [nil, ""] ->
        normalized = Encoding.path(normalize_path(uri.path || "/"))

        normalized =
          if String.ends_with?(normalized, "/"), do: normalized, else: normalized <> "/"

        %{
          "scheme" => uri.scheme,
          "authority" => URI.parse(url).authority,
          "path" => uri.path || "",
          "normalizedPath" => normalized,
          "isIp" => ip?(uri.host)
        }

      _ ->
        nil
    end
  end

  defp attribute(value, path) do
    path
    |> String.split(".")
    |> Enum.reduce(value, fn part, current ->
      case Regex.run(~r/^(\w*)\[(\d+)\]$/, part) do
        [_, name, index] ->
          current = if name == "", do: current, else: if(is_map(current), do: current[name])
          if is_list(current), do: Enum.at(current, String.to_integer(index))

        _ ->
          if is_map(current), do: current[part]
      end
    end)
  end

  defp valid_host?(value, true) when is_binary(value),
    do: value |> String.split(".") |> Enum.all?(&valid_host?(&1, false))

  defp valid_host?(value, false) when is_binary(value),
    do: Regex.match?(~r/^(?!-)[A-Za-z0-9-]{1,63}(?<!-)$/, value)

  defp valid_host?(_, _), do: false

  defp virtual_bucket?(value, subdomains) when is_binary(value) do
    byte_size(value) >= 3 and String.downcase(value) == value and not ipv4?(value) and
      if(subdomains,
        do: value |> String.split(".") |> Enum.all?(&virtual_bucket?(&1, false)),
        else: valid_host?(value, false)
      )
  end

  defp virtual_bucket?(_, _), do: false
  defp ascii?(value), do: Enum.all?(:binary.bin_to_list(value), &(&1 < 128))
  defp ipv4?(value), do: is_binary(value) and Regex.match?(~r/^(\d{1,3}\.){3}\d{1,3}$/, value)

  defp ip?(host) when is_binary(host),
    do:
      ipv4?(host) or
        match?({:ok, {_, _, _, _, _, _, _, _}}, :inet.parse_address(String.to_charlist(host)))

  defp ip?(_host), do: false

  defp normalize_path(path) do
    parts =
      path
      |> String.split("/")
      |> Enum.reduce([], fn
        part, acc when part in ["", "."] -> acc
        "..", [_ | rest] -> rest
        "..", [] -> []
        part, acc -> [part | acc]
      end)
      |> Enum.reverse()
      |> Enum.join("/")

    prefix = if String.starts_with?(path, "/"), do: "/", else: ""
    suffix = if String.ends_with?(path, "/") and parts != "", do: "/", else: ""
    prefix <> parts <> suffix
  end

  defp failure(message, code \\ :endpoint_resolution),
    do: {:error, Error.new(:configuration, message, code: code)}
end
