defmodule ExStorageServiceCli.S3.XML do
  @moduledoc "Bounded UTF-8 REST-XML decoding with binary names and no DTD or entity expansion."
  alias ExStorageServiceCli.Error

  defmodule Node do
    @moduledoc false
    defstruct [:name, attributes: %{}, content: []]
  end

  @name ~r/^[\p{L}_:][\p{L}\p{N}\p{M}_.:-]*$/u
  @attribute ~r/^([^\s=]+)\s*=\s*(["'])(.*?)\2(.*)$/us

  def parse(xml, opts \\ []) when is_binary(xml) do
    limits = %{
      bytes: opts[:max_bytes] || 8_388_608,
      depth: opts[:max_depth] || 64,
      nodes: opts[:max_nodes] || 65_536
    }

    cond do
      byte_size(xml) > limits.bytes ->
        failure(:xml_limit)

      not String.valid?(xml) ->
        failure(:invalid_xml)

      Regex.match?(~r/[\x00-\x08\x0B\x0C\x0E-\x1F]/u, xml) ->
        failure(:invalid_xml)

      true ->
        xml =
          xml
          |> String.replace_prefix(<<239, 187, 191>>, "")
          |> String.replace("\r\n", "\n")
          |> String.replace("\r", "\n")

        walk(xml, [], nil, 0, limits)
    end
  end

  def children(%Node{content: content}, name),
    do:
      Enum.filter(content, fn child ->
        match?(%Node{}, child) and local_name(child.name) == name
      end)

  def child_text(node, name) do
    case children(node, name) do
      [child | _] -> text(child)
      [] -> ""
    end
  end

  def text(%Node{content: content}),
    do:
      Enum.map_join(content, "", fn
        %Node{} = child -> text(child)
        text -> text
      end)

  def local_name(name), do: name |> String.split(":") |> List.last()

  defp walk("", [], %Node{} = root, _count, _limits), do: {:ok, root}
  defp walk("", _stack, _root, _count, _limits), do: failure(:invalid_xml)

  defp walk("<!--" <> rest, stack, root, count, limits) do
    with {:ok, comment, remaining} <- split(rest, "-->"),
         false <- String.contains?(comment, "--") do
      walk(remaining, stack, root, count, limits)
    else
      _ -> failure(:invalid_xml)
    end
  end

  defp walk("<![CDATA[" <> rest, [_ | _] = stack, root, count, limits) do
    with {:ok, value, remaining} <- split(rest, "]]>") do
      walk(remaining, add_text(stack, value), root, count, limits)
    end
  end

  defp walk("<!" <> _rest, _stack, _root, _count, _limits), do: failure(:invalid_xml)

  defp walk("<?" <> rest, stack, root, count, limits) do
    with {:ok, instruction, remaining} <- split(rest, "?>") do
      if String.starts_with?(String.downcase(instruction), "xml") and
           (stack != [] or root != nil),
         do: failure(:invalid_xml),
         else: walk(remaining, stack, root, count, limits)
    end
  end

  defp walk("</" <> rest, [%Node{} = frame | stack], root, count, limits) do
    with {:ok, name, remaining} <- tag(rest),
         true <- String.trim(name) == frame.name do
      attach(
        %{frame | content: Enum.reverse(frame.content)},
        remaining,
        stack,
        root,
        count,
        limits
      )
    else
      _ -> failure(:invalid_xml)
    end
  end

  defp walk("</" <> _rest, _stack, _root, _count, _limits), do: failure(:invalid_xml)

  defp walk("<" <> rest, stack, root, count, limits) do
    cond do
      count >= limits.nodes or length(stack) >= limits.depth ->
        failure(:xml_limit)

      stack == [] and root != nil ->
        failure(:invalid_xml)

      true ->
        with {:ok, markup, remaining} <- tag(rest),
             {:ok, node, empty?} <- opening(markup) do
          if empty?,
            do: attach(node, remaining, stack, root, count + 1, limits),
            else: walk(remaining, [node | stack], root, count + 1, limits)
        end
    end
  end

  defp walk(data, stack, root, count, limits) do
    {value, remaining} =
      case :binary.match(data, "<") do
        {position, 1} ->
          {binary_part(data, 0, position),
           binary_part(data, position, byte_size(data) - position)}

        :nomatch ->
          {data, ""}
      end

    with false <- String.contains?(value, "]]>"), {:ok, value} <- entities(value) do
      if stack == [] and String.trim(value) != "",
        do: failure(:invalid_xml),
        else: walk(remaining, add_text(stack, value), root, count, limits)
    else
      _ -> failure(:invalid_xml)
    end
  end

  defp attach(node, remaining, [], nil, count, limits),
    do: walk(remaining, [], node, count, limits)

  defp attach(node, remaining, [parent | stack], root, count, limits),
    do:
      walk(remaining, [%{parent | content: [node | parent.content]} | stack], root, count, limits)

  defp add_text([], _value), do: []
  defp add_text(stack, ""), do: stack
  defp add_text([node | stack], value), do: [%{node | content: [value | node.content]} | stack]

  defp opening(markup) do
    empty? = String.ends_with?(markup, "/")
    markup = if empty?, do: binary_part(markup, 0, byte_size(markup) - 1), else: markup

    case String.split(markup, ~r/\s/u, parts: 2) do
      [name | rest] ->
        with :ok <- validate_name(name),
             {:ok, attributes} <- attributes(Enum.join(rest), %{}) do
          {:ok, %Node{name: name, attributes: attributes}, empty?}
        end
    end
  end

  defp attributes(data, result) do
    data = String.trim(data)

    case data do
      "" ->
        {:ok, result}

      _ ->
        case Regex.run(@attribute, data) do
          [_, name, _quote, value, rest] ->
            with :ok <- validate_name(name),
                 false <- Map.has_key?(result, name),
                 false <- String.contains?(value, "<"),
                 {:ok, value} <- entities(value) do
              attributes(rest, Map.put(result, name, value))
            else
              _ -> failure(:invalid_xml)
            end

          _ ->
            failure(:invalid_xml)
        end
    end
  end

  defp validate_name(name) when byte_size(name) > 256, do: failure(:xml_limit)

  defp validate_name(name),
    do: if(Regex.match?(@name, name), do: :ok, else: failure(:invalid_xml))

  defp tag(data), do: tag(data, 0, nil)
  defp tag(data, position, _quote) when position >= byte_size(data), do: failure(:invalid_xml)

  defp tag(data, position, quote) do
    byte = :binary.at(data, position)

    cond do
      byte == ?> and quote == nil ->
        {:ok, binary_part(data, 0, position),
         binary_part(data, position + 1, byte_size(data) - position - 1)}

      byte in [?", ?'] and quote == nil ->
        tag(data, position + 1, byte)

      byte == quote ->
        tag(data, position + 1, nil)

      byte == ?< ->
        failure(:invalid_xml)

      true ->
        tag(data, position + 1, quote)
    end
  end

  defp split(data, delimiter) do
    case :binary.match(data, delimiter) do
      {position, size} ->
        {:ok, binary_part(data, 0, position),
         binary_part(data, position + size, byte_size(data) - position - size)}

      :nomatch ->
        failure(:invalid_xml)
    end
  end

  defp entities(data), do: entities(data, [])

  defp entities(data, result) do
    case :binary.match(data, "&") do
      :nomatch ->
        {:ok, IO.iodata_to_binary(Enum.reverse([data | result]))}

      {position, 1} ->
        prefix = binary_part(data, 0, position)
        remaining = binary_part(data, position + 1, byte_size(data) - position - 1)

        with {:ok, entity, rest} <- split(remaining, ";"),
             {:ok, value} <- entity(entity) do
          entities(rest, [value, prefix | result])
        end
    end
  end

  defp entity("amp"), do: {:ok, "&"}
  defp entity("lt"), do: {:ok, "<"}
  defp entity("gt"), do: {:ok, ">"}
  defp entity("quot"), do: {:ok, "\""}
  defp entity("apos"), do: {:ok, "'"}
  defp entity("#x" <> value), do: codepoint(value, 16)
  defp entity("#" <> value), do: codepoint(value, 10)
  defp entity(_value), do: failure(:invalid_xml)

  defp codepoint(value, base) when byte_size(value) <= 10 do
    case Integer.parse(value, base) do
      {number, ""}
      when number in [9, 10, 13] or number in 0x20..0xD7FF or number in 0xE000..0xFFFD or
             number in 0x10000..0x10FFFF ->
        {:ok, <<number::utf8>>}

      _ ->
        failure(:invalid_xml)
    end
  end

  defp codepoint(_value, _base), do: failure(:invalid_xml)

  defp failure(code),
    do: {:error, Error.new(:protocol, "Invalid or oversized XML response", code: code)}
end
