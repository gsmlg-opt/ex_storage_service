defmodule ExStorageServiceCli.S3.XMLTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Error, S3.XML}

  test "namespaces, mixed text, attributes, CDATA and numeric entities retain bytes" do
    xml =
      ~s(<?xml version="1.0"?><s:Result xmlns:s="urn:s3" note="a&gt;b"><s:Key>a//../%+雪&#xD;<![CDATA[?&#xA;]]>&amp;</s:Key><!-- x --><Empty/></s:Result>)

    assert {:ok, node} = XML.parse(xml)
    assert node.name == "s:Result"
    assert node.attributes == %{"xmlns:s" => "urn:s3", "note" => "a>b"}
    assert XML.child_text(node, "Key") == "a//../%+雪\r?&#xA;&"
    assert [%XML.Node{name: "Empty", content: []}] = XML.children(node, "Empty")
  end

  test "external entities, DTDs, unknown entities and malformed documents fail safely" do
    for xml <- [
          ~s(<!DOCTYPE Error SYSTEM "file:///etc/passwd"><Error/>),
          ~s(<!DOCTYPE R [<!ENTITY x "x">]><R>&x;</R>),
          "<R>&unknown;</R>",
          "<R>&</R>",
          "<R>&#0;</R>",
          "<R>&#xD800;</R>",
          "<R><A></R>",
          "<R/><Other/>",
          "<R",
          "text<R/>",
          "<R a='1' a='2'/>",
          "<R>\0</R>",
          "<R>bad]]>text</R>"
        ] do
      assert {:error, %Error{category: :protocol}} = XML.parse(xml)
    end
  end

  test "size, nesting, element and name budgets are enforced" do
    assert {:error, %Error{code: :xml_limit}} = XML.parse("<R>large</R>", max_bytes: 8)
    assert {:error, %Error{code: :xml_limit}} = XML.parse("<R><A><B/></A></R>", max_depth: 2)
    assert {:error, %Error{code: :xml_limit}} = XML.parse("<R><A/><B/></R>", max_nodes: 2)

    assert {:error, %Error{code: :xml_limit}} =
             XML.parse("<" <> String.duplicate("a", 257) <> "/>")
  end

  test "untrusted element and attribute names never create atoms" do
    assert {:ok, _} = XML.parse("<Root note='warm'><Key>value</Key></Root>")
    count = :erlang.system_info(:atom_count)

    for index <- 1..1000 do
      name = "untrusted_element_#{index}"
      assert {:ok, node} = XML.parse("<#{name} untrusted_attribute_#{index}='value'/>")
      assert node.name == name
    end

    assert :erlang.system_info(:atom_count) == count
  end
end
