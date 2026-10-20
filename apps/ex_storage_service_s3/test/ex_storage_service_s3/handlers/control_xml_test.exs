defmodule ExStorageServiceS3.Handlers.ControlXMLTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceS3.Handlers.{ControlXML, Cors, Tagging}

  test "tag XML accepts S3 namespaces and UTF8 without losing literal text" do
    tags = %{"owner" => "研发 + / : @", "empty" => ""}
    assert {:ok, ^tags} = Tagging.parse(Tagging.document(tags))

    assert {:ok, ^tags} =
             Tagging.parse(
               String.replace(
                 Tagging.document(tags),
                 " xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"",
                 ""
               )
             )

    assert {:ok, %{}} =
             Tagging.parse(
               "<s:Tagging xmlns:s='http://s3.amazonaws.com/doc/2006-03-01/'><s:TagSet/></s:Tagging>"
             )
  end

  test "control XML rejects entities, extra roots, attributes and structural ambiguity" do
    for body <- [
          "<!DOCTYPE Tagging [<!ENTITY secret SYSTEM 'file:///not-readable'>]><Tagging><TagSet/></Tagging>",
          "<Tagging xmlns='https://foreign.example/'><TagSet/></Tagging>",
          "<Tagging custom='value'><TagSet/></Tagging>",
          "<Tagging><TagSet/><TagSet/></Tagging>",
          "<Tagging>unexpected<TagSet/></Tagging>",
          "<Tagging><TagSet><Tag><Key>a</Key><Key>b</Key><Value>c</Value></Tag></TagSet></Tagging>",
          "<Tagging><TagSet><Tag><Key>a</Key><Value><Key>b</Key></Value></Tag></TagSet></Tagging>",
          "<Tagging><TagSet/></Tagging><Tagging><TagSet/></Tagging>",
          "<Tagging><TagSet/></Tagging>trailing",
          "<Tagging><?instruction value?><TagSet/></Tagging>",
          "<Tagging><Unexpected/></Tagging>",
          <<255>>
        ] do
      assert {:error, :malformed_xml} = Tagging.parse(body), inspect(body)
    end

    deeply_nested = String.duplicate("<Tagging>", 9) <> String.duplicate("</Tagging>", 9)
    assert {:error, :malformed_xml} = ControlXML.parse(deeply_nested, "Tagging", ["Tagging"])
  end

  test "CORS serialization round trips normalized rules and rejects duplicate singleton fields" do
    rules = [
      %{
        id: "browser",
        allowed_origins: ["https://example.test"],
        allowed_methods: ["GET"],
        allowed_headers: ["x-amz-*"],
        expose_headers: ["etag"],
        max_age_seconds: 0
      }
    ]

    assert {:ok, ^rules} = Cors.parse(Cors.document(rules))

    for repeated <- [
          "<ID>a</ID><ID>b</ID>",
          "<MaxAgeSeconds>1</MaxAgeSeconds><MaxAgeSeconds>2</MaxAgeSeconds>"
        ] do
      body =
        "<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod>#{repeated}</CORSRule></CORSConfiguration>"

      assert {:error, :malformed_xml} = Cors.parse(body)
    end
  end
end
