defmodule ExStorageServiceS3.Handlers.ControlXMLGeneratedTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceS3.Handlers.{ControlXML, Cors, Tagging}

  @max_bytes 4_096
  @max_cases 20_000

  test "deterministically generated tags and CORS round trip through the real parsers" do
    for seed <- 1..16, {parser, _root, value, document} <- documents(seed) do
      assert byte_size(document) <= @max_bytes
      assert {:ok, ^value} = parser.parse(document)
      assert {:ok, ^value} = parser.parse(parser.document(value))
    end
  end

  test "bounded generated truncation and byte mutations reject without exceptions" do
    count =
      for seed <- [1, 8], entry <- documents(seed), reduce: 0 do
        total ->
          {parser, root, _value, document} = entry

          # Every shorter byte prefix includes cuts inside UTF8 and XML escapes.
          truncated = for size <- 0..(byte_size(document) - 1), do: binary_part(document, 0, size)

          inserted =
            for offset <- 0..byte_size(document), invalid <- [<<0>>, <<255>>] do
              binary_part(document, 0, offset) <>
                invalid <>
                binary_part(document, offset, byte_size(document) - offset)
            end

          structural = [
            document <> document,
            document <> "trailing",
            "<!DOCTYPE #{root} [<!ENTITY denied SYSTEM 'file:///not-readable'>]>" <> document,
            String.replace(document, "<#{root} ", "<#{root} custom='denied' ", global: false),
            String.replace(document, "</#{root}>", "<?denied instruction?></#{root}>"),
            String.replace(
              document,
              "http://s3.amazonaws.com/doc/2006-03-01/",
              "https://foreign.test/"
            )
          ]

          cases = truncated ++ inserted ++ structural

          for {body, index} <- Enum.with_index(cases) do
            assert byte_size(body) <= @max_bytes

            assert {:error, :malformed_xml} = parser.parse(body),
                   "#{inspect(parser)} seed=#{seed} mutation=#{index} bytes=#{byte_size(body)}"
          end

          total + length(cases)
      end

    assert count > 1_000 and count <= @max_cases
    IO.puts("generated XML corpus: #{count} rejected cases, max input #{@max_bytes} bytes")
  end

  test "generated nesting honors the parser depth boundary" do
    for depth <- 1..16 do
      document = String.duplicate("<Root>", depth) <> String.duplicate("</Root>", depth)

      if depth <= 8 do
        assert {:ok, _node} = ControlXML.parse(document, "Root", ["Root"])
      else
        assert {:error, :malformed_xml} = ControlXML.parse(document, "Root", ["Root"])
      end
    end
  end

  defp documents(seed) do
    tags =
      Map.new(1..(rem(seed, 10) + 1), fn index ->
        {"key-#{seed}-#{index}", "研发 + / : @ #{seed}-#{index}"}
      end)

    rules = [
      %{
        id: "browser <#{seed}> & \"quoted\" 雪",
        allowed_origins: ["https://browser-#{seed}.test"],
        allowed_methods: ["GET", "HEAD"],
        allowed_headers: ["x-generated-*"],
        expose_headers: ["etag"],
        max_age_seconds: seed
      }
    ]

    [
      {Tagging, "Tagging", tags, Tagging.document(tags)},
      {Cors, "CORSConfiguration", rules, Cors.document(rules)}
    ]
  end
end
