defmodule ExStorageServiceS3.Handlers.ControlXML do
  @moduledoc false
  alias ExStorageServiceS3.Handlers.Shared
  @namespace "http://s3.amazonaws.com/doc/2006-03-01/"

  def parse(body, root, allowed_names) do
    if String.valid?(body) and not Shared.xml_has_doctype?(body) do
      state = %{stack: [], root: nil, allowed: MapSet.new(allowed_names)}

      case :xmerl_sax_parser.stream(body, event_fun: &event/3, event_state: state) do
        {:ok, %{root: %{name: ^root} = document, stack: []}, rest} ->
          if String.trim(IO.iodata_to_binary(rest)) == "",
            do: {:ok, document},
            else: {:error, :malformed_xml}

        _ ->
          {:error, :malformed_xml}
      end
    else
      {:error, :malformed_xml}
    end
  rescue
    _ -> {:error, :malformed_xml}
  catch
    _, _ -> {:error, :malformed_xml}
  end

  def children(node, name), do: Enum.filter(node.children, &(&1.name == name))

  def text(node, name) do
    case children(node, name) do
      [%{children: [], text: value}] -> {:ok, value}
      _ -> {:error, :malformed_xml}
    end
  end

  defp event({:startElement, uri, local, _qualified, attributes}, _location, state) do
    name = to_string(local)

    unless MapSet.member?(state.allowed, name) and attributes == [] and
             to_string(uri) in ["", @namespace] and length(state.stack) < 8,
           do: throw(:malformed_xml)

    node = %{name: name, children: [], text: []}
    %{state | stack: [node | state.stack]}
  end

  defp event({:endElement, _uri, _local, _qualified}, _location, %{stack: [node | rest]} = state) do
    node = %{
      node
      | children: Enum.reverse(node.children),
        text: node.text |> Enum.reverse() |> IO.iodata_to_binary()
    }

    case rest do
      [] ->
        if state.root != nil, do: throw(:malformed_xml)
        %{state | stack: [], root: node}

      [parent | tail] ->
        %{state | stack: [%{parent | children: [node | parent.children]} | tail]}
    end
  end

  defp event({kind, chars}, _location, %{stack: [node | rest]} = state)
       when kind in [:characters, :ignorableWhitespace] do
    text = :unicode.characters_to_binary(chars)
    if not is_binary(text), do: throw(:malformed_xml)
    %{state | stack: [%{node | text: [text | node.text]} | rest]}
  end

  defp event({:processingInstruction, _, _}, _, _), do: throw(:malformed_xml)
  defp event(_event, _location, state), do: state
end
