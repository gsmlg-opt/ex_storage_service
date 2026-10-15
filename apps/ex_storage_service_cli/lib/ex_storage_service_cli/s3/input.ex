defmodule ExStorageServiceCli.S3.Input do
  @moduledoc "Strict validation of string-keyed modeled S3 inputs, without coercion or atoms."
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.S3.OperationCatalog

  def validate(operation_name, input) do
    with {:ok, operation} <- OperationCatalog.operation(operation_name),
         :ok <- validate_shape(OperationCatalog.shape(operation["input"]["shape"]), input, [], 0) do
      {:ok, input}
    else
      {:invalid, path, reason} ->
        {:error,
         Error.new(:syntax, "Invalid parameter #{Enum.join(path, ".")}: #{reason}",
           operation: OperationCatalog.operation_name(operation_name),
           code: :invalid_parameter
         )}

      error ->
        error
    end
  end

  defp validate_shape(_shape, _value, path, depth) when depth > 64,
    do: invalid(path, "nesting limit exceeded")

  defp validate_shape(nil, value, path, _depth),
    do: if(value == %{}, do: :ok, else: invalid(path, "operation takes no parameters"))

  defp validate_shape(shape, value, path, depth) do
    with :ok <- type(shape, value, path, depth),
         :ok <- enum(shape, value, path),
         :ok <- bounds(shape, value, path) do
      :ok
    end
  end

  defp type(%{"type" => "structure"} = shape, value, path, depth)
       when is_map(value) and not is_struct(value) do
    members = shape["members"] || %{}
    unknown = Map.keys(value) -- Map.keys(members)
    missing = Enum.filter(shape["required"] || [], &(not Map.has_key?(value, &1)))

    cond do
      unknown != [] ->
        invalid(path, "unknown member")

      missing != [] ->
        invalid(path ++ [hd(missing)], "required member is absent")

      true ->
        each(Enum.sort(value), fn {name, item} ->
          validate_shape(
            OperationCatalog.shape(members[name]["shape"]),
            item,
            path ++ [name],
            depth + 1
          )
        end)
    end
  end

  defp type(%{"type" => "list", "member" => member}, value, path, depth) when is_list(value),
    do:
      each(Enum.with_index(value), fn {item, index} ->
        validate_shape(
          OperationCatalog.shape(member["shape"]),
          item,
          path ++ [Integer.to_string(index)],
          depth + 1
        )
      end)

  defp type(%{"type" => "map", "key" => key, "value" => member}, value, path, depth)
       when is_map(value) and not is_struct(value),
       do:
         each(Enum.sort(value), fn {name, item} ->
           with :ok <-
                  validate_shape(
                    OperationCatalog.shape(key["shape"]),
                    name,
                    path ++ ["[key]"],
                    depth + 1
                  ),
                :ok <-
                  validate_shape(
                    OperationCatalog.shape(member["shape"]),
                    item,
                    path ++ ["[value]"],
                    depth + 1
                  ),
                do: :ok
         end)

  defp type(%{"type" => kind}, value, _path, _depth)
       when kind in ["string", "blob"] and is_binary(value),
       do: :ok

  defp type(%{"type" => "boolean"}, value, _path, _depth) when is_boolean(value), do: :ok

  defp type(%{"type" => "integer"}, value, _path, _depth)
       when is_integer(value) and value in -2_147_483_648..2_147_483_647,
       do: :ok

  defp type(%{"type" => "long"}, value, _path, _depth)
       when is_integer(value) and value in -9_223_372_036_854_775_808..9_223_372_036_854_775_807,
       do: :ok

  defp type(%{"type" => kind}, value, _path, _depth)
       when kind in ["float", "double"] and is_number(value),
       do: :ok

  defp type(%{"type" => "timestamp"}, %DateTime{}, _path, _depth), do: :ok
  defp type(%{"type" => "timestamp"}, value, _path, _depth) when is_number(value), do: :ok

  defp type(%{"type" => "timestamp"}, value, path, _depth) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, _, _} -> :ok
      _ -> invalid(path, "expected an ISO 8601 timestamp or Unix seconds")
    end
  end

  defp type(%{"type" => kind}, _value, path, _depth), do: invalid(path, "expected #{kind}")

  defp enum(%{"enum" => values}, value, path),
    do: if(value in values, do: :ok, else: invalid(path, "value is outside the modeled enum"))

  defp enum(_shape, _value, _path), do: :ok

  defp bounds(shape, value, path) do
    size =
      cond do
        is_number(value) ->
          value

        is_binary(value) ->
          if(shape["type"] == "blob", do: byte_size(value), else: String.length(value))

        is_list(value) ->
          length(value)

        is_map(value) and not is_struct(value) ->
          map_size(value)

        true ->
          nil
      end

    cond do
      size != nil and shape["min"] != nil and size < shape["min"] ->
        invalid(path, "below modeled minimum")

      size != nil and shape["max"] != nil and size > shape["max"] ->
        invalid(path, "above modeled maximum")

      true ->
        :ok
    end
  end

  defp each(values, function),
    do:
      Enum.reduce_while(values, :ok, fn value, :ok ->
        case function.(value) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)

  defp invalid(path, reason), do: {:invalid, path, reason}
end
