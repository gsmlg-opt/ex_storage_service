defmodule ExStorageServiceCli.S3.Decoder do
  @moduledoc "Pure modeled REST-XML decoding; streaming/event-stream bodies require dedicated handlers."
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.S3.{OperationCatalog, Response, XML}
  alias ExStorageServiceCli.S3.XML.Node

  def decode(operation_name, %Response{} = response) do
    with {:ok, operation} <- OperationCatalog.operation(operation_name) do
      try do
        cond do
          response.status not in 200..299 ->
            throw(:http_error)

          not is_binary(response.body) and response.body != nil ->
            throw(:streaming_handler_required)

          is_binary(response.body) and byte_size(response.body) > 8_388_608 ->
            throw(:control_body_limit)

          true ->
            :ok
        end

        shape = OperationCatalog.shape(operation["output"]["shape"])
        result = decode_output(shape, response) |> customize(operation_name)
        {:ok, result}
      catch
        {:xml_error, error} ->
          {:error, %{error | operation: operation["name"]}}

        code ->
          {:error,
           Error.new(
             if(code in [:http_error, :service_error], do: :service, else: :protocol),
             "Invalid S3 control response or dedicated response handler required",
             operation: operation["name"],
             code: code,
             http_status: response.status
           )}
      end
    end
  end

  @doc "Decodes modeled response metadata without inspecting or consuming an object stream."
  def decode_headers(operation_name, %Response{} = response) do
    with {:ok, operation} <- OperationCatalog.operation(operation_name) do
      try do
        if response.status not in 200..299, do: throw(:http_error)
        {:ok, headers(OperationCatalog.shape(operation["output"]["shape"]), response)}
      catch
        code ->
          {:error,
           Error.new(
             if(code in [:http_error, :service_error], do: :service, else: :protocol),
             "Invalid S3 response metadata",
             operation: operation["name"],
             code: code,
             http_status: response.status
           )}
      end
    end
  end

  defp model(binding), do: Map.merge(OperationCatalog.shape(binding["shape"]), binding)

  defp customize(%{"LocationConstraint" => ""} = result, "GetBucketLocation"),
    do: Map.put(result, "LocationConstraint", nil)

  defp customize(result, _), do: result

  defp decode_output(nil, %Response{body: body}) do
    if body not in [nil, ""], do: check_error(parse(body))
    %{}
  end

  defp decode_output(shape, response) do
    members = shape["members"] || %{}

    if Enum.any?(members, fn {_key, binding} ->
         binding["streaming"] == true or model(binding)["eventstream"] == true
       end),
       do: throw(:streaming_handler_required)

    result = headers(shape, response)

    body = response.body || ""

    case shape["payload"] do
      nil ->
        if body == "",
          do: result,
          else: Map.merge(result, structure(shape, check_error(parse(body))))

      name ->
        binding = members[name]
        payload = model(binding)

        value =
          case payload["type"] do
            "string" -> scalar(payload, body)
            "blob" -> throw(:streaming_handler_required)
            _ -> if(body == "", do: %{}, else: node(payload, check_error(parse(body))))
          end

        Map.put(result, name, value)
    end
  end

  defp headers(nil, _response), do: %{}

  defp headers(shape, response) do
    headers = Map.new(response.headers, fn {key, value} -> {String.downcase(key), value} end)

    Enum.reduce(Enum.sort(shape["members"] || %{}), %{}, fn {name, binding}, result ->
      case binding["location"] do
        "header" ->
          case Map.fetch(headers, String.downcase(binding["locationName"] || name)) do
            {:ok, value} -> header_member(result, name, binding, value)
            :error -> result
          end

        "headers" ->
          prefix = String.downcase(binding["locationName"] || name)

          values =
            Map.new(
              Enum.filter(headers, fn {key, _} -> String.starts_with?(key, prefix) end),
              fn {key, value} ->
                {String.replace_prefix(key, prefix, ""),
                 scalar(model(model(binding)["value"]), value)}
              end
            )

          Map.put(result, name, values)

        "statusCode" ->
          Map.put(result, name, response.status)

        _ ->
          result
      end
    end)
  end

  # The pinned SDK exposes ExpiresString even when a provider's Expires value
  # cannot be parsed. Keep this customization on both control and stream paths.
  defp header_member(result, "Expires", %{"shape" => "Expires"} = binding, value)
       when value != "" do
    result = Map.put(result, "ExpiresString", value)

    try do
      Map.put(result, "Expires", header_scalar(model(binding), value))
    catch
      :invalid_timestamp -> result
    end
  end

  defp header_member(result, name, binding, value),
    do: Map.put(result, name, header_scalar(model(binding), value))

  defp parse(body) do
    case XML.parse(body) do
      {:ok, node} -> node
      {:error, error} -> throw({:xml_error, error})
    end
  end

  defp check_error(%Node{name: name} = node) do
    if XML.local_name(name) == "Error", do: throw(:service_error)
    node
  end

  defp structure(shape, parent) do
    Enum.reduce(Enum.sort(shape["members"] || %{}), %{}, fn {name, binding}, result ->
      shape = model(binding)
      wire_name = binding["locationName"] || name

      cond do
        binding["location"] != nil ->
          result

        binding["xmlAttribute"] ->
          case Enum.find(parent.attributes, fn {key, _} ->
                 XML.local_name(key) == XML.local_name(wire_name)
               end) do
            nil -> result
            {_key, value} -> Map.put(result, name, scalar(shape, value))
          end

        shape["flattened"] == true and shape["type"] == "list" ->
          case XML.children(parent, wire_name) do
            [] ->
              result

            children ->
              Map.put(result, name, Enum.map(children, &node(model(shape["member"]), &1)))
          end

        shape["flattened"] == true and shape["type"] == "map" ->
          case XML.children(parent, wire_name) do
            [] -> result
            children -> Map.put(result, name, map_entries(shape, children))
          end

        true ->
          case XML.children(parent, wire_name) do
            [] ->
              if XML.local_name(parent.name) == wire_name and
                   shape["type"] not in ["list", "map", "structure"],
                 do: Map.put(result, name, node(shape, parent)),
                 else: result

            [child] ->
              Map.put(result, name, node(shape, child))

            _ ->
              throw(:duplicate_xml_member)
          end
      end
    end)
  end

  defp node(%{"type" => "structure"} = shape, node), do: structure(shape, node)

  defp node(%{"type" => "list", "member" => member}, parent),
    do:
      Enum.map(XML.children(parent, member["locationName"] || "member"), &node(model(member), &1))

  defp node(%{"type" => "map"} = shape, parent),
    do: map_entries(shape, XML.children(parent, "entry"))

  defp node(shape, node), do: scalar(shape, XML.text(node))

  defp map_entries(shape, entries),
    do:
      Map.new(entries, fn entry ->
        key = shape["key"]
        value = shape["value"]

        with [key_node] <- XML.children(entry, key["locationName"] || "key"),
             [value_node] <- XML.children(entry, value["locationName"] || "value") do
          {node(model(key), key_node), node(model(value), value_node)}
        else
          _ -> throw(:invalid_xml_map)
        end
      end)

  defp header_scalar(%{"type" => "list", "member" => member}, value),
    do: Enum.map(String.split(value, ","), &scalar(model(member), String.trim(&1)))

  defp header_scalar(shape, value), do: scalar(shape, value)

  defp scalar(%{"type" => "boolean"}, "true"), do: true
  defp scalar(%{"type" => "boolean"}, "false"), do: false
  defp scalar(%{"type" => "boolean"}, _value), do: throw(:invalid_boolean)

  defp scalar(%{"type" => type}, value) when type in ["integer", "long"] do
    case Integer.parse(value) do
      {number, ""} -> number
      _ -> throw(:invalid_number)
    end
  end

  defp scalar(%{"type" => type}, value) when type in ["float", "double"] do
    case Float.parse(value) do
      {number, ""} -> number
      _ -> throw(:invalid_number)
    end
  end

  defp scalar(%{"type" => "timestamp"}, value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _ -> http_date(value)
    end
  end

  defp scalar(%{"type" => "blob"}, value) do
    case Base.decode64(value) do
      {:ok, binary} -> binary
      :error -> throw(:invalid_blob)
    end
  end

  defp scalar(_shape, value) do
    if not String.valid?(value), do: throw(:invalid_utf8)
    value
  end

  @months %{
    "Jan" => 1,
    "Feb" => 2,
    "Mar" => 3,
    "Apr" => 4,
    "May" => 5,
    "Jun" => 6,
    "Jul" => 7,
    "Aug" => 8,
    "Sep" => 9,
    "Oct" => 10,
    "Nov" => 11,
    "Dec" => 12
  }
  defp http_date(value) do
    case Regex.run(
           ~r/^[A-Za-z]{3}, (\d{2}) ([A-Za-z]{3}) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT$/,
           value
         ) do
      [_, day, month, year, hour, minute, second] ->
        with month when is_integer(month) <- @months[month],
             {:ok, naive} <-
               NaiveDateTime.new(
                 String.to_integer(year),
                 month,
                 String.to_integer(day),
                 String.to_integer(hour),
                 String.to_integer(minute),
                 String.to_integer(second)
               ),
             {:ok, datetime} <- DateTime.from_naive(naive, "Etc/UTC") do
          datetime
        else
          _ -> throw(:invalid_timestamp)
        end

      _ ->
        case Float.parse(value) do
          {number, ""} ->
            case DateTime.from_unix(round(number * 1_000_000), :microsecond) do
              {:ok, datetime} -> datetime
              _ -> throw(:invalid_timestamp)
            end

          _ ->
            throw(:invalid_timestamp)
        end
    end
  end
end
