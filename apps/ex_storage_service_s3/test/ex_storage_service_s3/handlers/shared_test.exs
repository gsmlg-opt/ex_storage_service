defmodule ExStorageServiceS3.Handlers.SharedTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceS3.Handlers.Shared

  describe "decode_aws_chunked/1" do
    test "decodes well-formed aws-chunked framing" do
      body =
        "5;chunk-signature=abc\r\nhello\r\n" <>
          "6;chunk-signature=def\r\n world\r\n" <>
          "0;chunk-signature=ghi\r\n\r\n"

      assert Shared.decode_aws_chunked(body) == "hello world"
    end

    test "fails closed when the declared chunk size exceeds the data" do
      body = "10;chunk-signature=abc\r\nshort\r\n"
      assert Shared.decode_aws_chunked(body) == {:error, :malformed_chunked}
    end

    test "fails closed on a non-hex chunk header" do
      body = "nothex\r\ndata\r\n"
      assert Shared.decode_aws_chunked(body) == {:error, :malformed_chunked}
    end

    test "fails closed when there is no CRLF terminator" do
      assert Shared.decode_aws_chunked("garbage") == {:error, :malformed_chunked}
    end
  end

  describe "xml_has_doctype?/1" do
    test "flags DOCTYPE declarations regardless of case" do
      assert Shared.xml_has_doctype?(~s(<?xml version="1.0"?><!DOCTYPE foo [ ]><Delete/>))
      assert Shared.xml_has_doctype?(~s(<!doctype x>))
    end

    test "flags custom ENTITY declarations" do
      assert Shared.xml_has_doctype?(~s(<!ENTITY xxe SYSTEM "file:///etc/passwd">))
    end

    test "accepts ordinary S3 XML bodies" do
      refute Shared.xml_has_doctype?(
               ~s(<?xml version="1.0"?><Delete><Object><Key>k</Key></Object></Delete>)
             )
    end
  end
end
