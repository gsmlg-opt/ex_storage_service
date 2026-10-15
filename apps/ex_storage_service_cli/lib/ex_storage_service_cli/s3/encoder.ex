defmodule ExStorageServiceCli.S3.Encoder do
  @moduledoc "Pure REST-XML serialization of modeled S3 control requests."
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.S3.{Encoding, Input, OperationCatalog, Request}

  @doc "allow_streaming is reserved for dedicated handlers replacing a validated binary placeholder with an owned stream."
  def encode(operation_name, input, opts \\ []) do
    with {:ok, operation} <- OperationCatalog.operation(operation_name),
         {:ok, input} <- Input.validate(operation_name, input) do
      shape = OperationCatalog.shape(operation["input"]["shape"])
      members = shape["members"] || %{}

      try do
        if Enum.any?(members, fn {name, binding} ->
             Map.has_key?(input, name) and special?(binding, opts)
           end),
           do: throw(:streaming_handler_required)

        [template | literal_query] = String.split(operation["http"]["requestUri"], "?", parts: 2)

        path =
          Regex.replace(~r/\{([^}]+)\}/, template, fn _match, label ->
            greedy = String.ends_with?(label, "+")
            label = String.trim_trailing(label, "+")

            {name, binding} =
              Enum.find(members, fn {name, binding} ->
                (binding["locationName"] || name) == label
              end)

            value = scalar(model(binding), Map.fetch!(input, name), "uri")
            if greedy, do: Encoding.path(value), else: Encoding.encode(value)
          end)

        query = Enum.flat_map(literal_query, &Encoding.query_pairs/1)

        {query, headers} =
          Enum.reduce(Enum.sort(input), {query, []}, fn {name, value}, {query, headers} ->
            binding = members[name]
            key = binding["locationName"] || name

            case binding["location"] do
              "querystring" ->
                {query ++ query_values(key, model(binding), value), headers}

              "header" ->
                {query, headers ++ header_value(key, model(binding), value)}

              "headers" ->
                {query,
                 headers ++
                   Enum.map(Enum.sort(value), fn {suffix, value} ->
                     header(key <> suffix, value)
                   end)}

              _ ->
                {query, headers}
            end
          end)

        body = body(shape, operation["input"], input)

        {:ok,
         %Request{
           operation: operation["name"],
           method: operation["http"]["method"],
           path: path,
           query: query,
           headers: headers,
           body: body
         }}
      catch
        code ->
          {:error,
           Error.new(
             :client,
             "Request requires a dedicated handler or contains invalid protocol values",
             operation: operation["name"],
             code: code
           )}
      end
    end
  end

  defp model(binding), do: Map.merge(OperationCatalog.shape(binding["shape"]), binding)

  defp special?(binding, opts),
    do:
      (binding["streaming"] == true and opts[:allow_streaming] != true) or
        model(binding)["eventstream"] == true

  defp query_values(key, %{"type" => "list", "member" => member}, values),
    do: Enum.map(values, &{key, scalar(model(member), &1, "querystring")})

  defp query_values(_key, %{"type" => "map", "value" => member}, values),
    do:
      Enum.map(Enum.sort(values), fn {key, value} ->
        {key, scalar(model(member), value, "querystring")}
      end)

  defp query_values(key, shape, value), do: [{key, scalar(shape, value, "querystring")}]

  defp header_value(_key, %{"type" => "list"}, []), do: []

  defp header_value(key, %{"type" => "list", "member" => member}, values) do
    value =
      Enum.map_join(values, ",", fn value ->
        value = scalar(model(member), value, "header")

        if String.contains?(value, [",", "\""]),
          do: "\"" <> String.replace(value, "\"", "\\\"") <> "\"",
          else: value
      end)

    [header(key, value)]
  end

  defp header_value(key, shape, value), do: [header(key, scalar(shape, value, "header"))]

  defp header(key, value) do
    if String.contains?(key <> value, ["\r", "\n", <<0>>]), do: throw(:invalid_header)
    {key, value}
  end

  defp body(shape, operation_input, input) do
    case shape["payload"] do
      nil ->
        values = Map.filter(input, fn {name, _} -> shape["members"][name]["location"] == nil end)

        if map_size(values) == 0,
          do: nil,
          else:
            xml(
              operation_input["locationName"] || operation_input["shape"],
              Map.merge(shape, operation_input),
              values
            )
            |> IO.iodata_to_binary()

      name ->
        case Map.fetch(input, name) do
          :error ->
            nil

          {:ok, value} ->
            binding = shape["members"][name]

            case model(binding)["type"] do
              type when type in ["string", "blob"] ->
                value

              _ ->
                xml(binding["locationName"] || name, model(binding), value)
                |> IO.iodata_to_binary()
            end
        end
    end
  end

  defp xml(name, %{"type" => "structure"} = shape, values) do
    {attributes, children} =
      Enum.reduce(Enum.sort(values), {namespace(shape), []}, fn {key, value},
                                                                {attributes, children} ->
        binding = shape["members"][key]
        child = binding["locationName"] || key

        if binding["xmlAttribute"],
          do: {attributes ++ [{child, scalar(model(binding), value, "body")}], children},
          else: {attributes, children ++ [xml(child, model(binding), value)]}
      end)

    element(name, attributes, children)
  end

  defp xml(name, %{"type" => "list", "member" => member} = shape, values) do
    if shape["flattened"],
      do: Enum.map(values, &xml(name, model(member), &1)),
      else:
        element(
          name,
          namespace(shape),
          Enum.map(values, &xml(member["locationName"] || "member", model(member), &1))
        )
  end

  defp xml(name, %{"type" => "map", "key" => key, "value" => member} = shape, values) do
    entries =
      Enum.map(Enum.sort(values), fn {key_value, value} ->
        element(if(shape["flattened"], do: name, else: "entry"), [], [
          xml(key["locationName"] || "key", model(key), key_value),
          xml(member["locationName"] || "value", model(member), value)
        ])
      end)

    if shape["flattened"], do: entries, else: element(name, namespace(shape), entries)
  end

  defp xml(name, shape, value),
    do: element(name, namespace(shape), escape(scalar(shape, value, "body")))

  defp namespace(%{"xmlNamespace" => %{"uri" => uri} = namespace}),
    do: [{if(namespace["prefix"], do: "xmlns:" <> namespace["prefix"], else: "xmlns"), uri}]

  defp namespace(_shape), do: []

  defp element(name, attributes, content),
    do: [
      "<",
      name,
      Enum.map(attributes, fn {key, value} -> [" ", key, "=\"", escape(value), "\""] end),
      ">",
      content,
      "</",
      name,
      ">"
    ]

  defp escape(value) do
    if not String.valid?(value) or Regex.match?(~r/[\x00-\x08\x0B\x0C\x0E-\x1F]/u, value),
      do: throw(:invalid_xml_value)

    value
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
    |> String.replace("'", "&apos;")
    |> String.replace("\r", "&#13;")
  end

  defp scalar(%{"type" => "boolean"}, value, _location), do: if(value, do: "true", else: "false")
  defp scalar(%{"type" => "blob"}, value, _location), do: Base.encode64(value)

  defp scalar(%{"type" => "timestamp"} = shape, value, location) do
    format = shape["timestampFormat"] || if(location == "header", do: "rfc822", else: "iso8601")
    datetime = datetime(value)

    datetime =
      if location == "header" or elem(datetime.microsecond, 0) == 0,
        do: DateTime.truncate(datetime, :second),
        else: datetime

    case String.downcase(format) do
      "rfc822" ->
        Calendar.strftime(datetime, "%a, %d %b %Y %H:%M:%S GMT")

      "unixtimestamp" ->
        datetime |> DateTime.to_unix(:microsecond) |> Kernel./(1_000_000) |> Float.to_string()

      "iso8601" ->
        DateTime.to_iso8601(datetime)
    end
  end

  defp scalar(_shape, value, _location) when is_binary(value), do: value
  defp scalar(_shape, value, _location), do: to_string(value)

  defp datetime(%DateTime{} = value),
    do: value |> DateTime.to_unix(:microsecond) |> DateTime.from_unix!(:microsecond)

  defp datetime(value) when is_number(value) do
    case DateTime.from_unix(round(value * 1_000_000), :microsecond) do
      {:ok, value} -> value
      _ -> throw(:invalid_timestamp)
    end
  end

  defp datetime(value) do
    {:ok, value, _offset} = DateTime.from_iso8601(value)
    value
  end
end
