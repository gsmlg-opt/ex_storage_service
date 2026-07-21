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

    test "decodes framing split across every input boundary" do
      body =
        "5;chunk-signature=abc\r\nhello\r\n" <>
          "6;chunk-signature=def\r\n world\r\n" <>
          "0;chunk-signature=ghi\r\n\r\n"

      chunks = for <<byte <- body>>, do: <<byte>>

      assert chunks
             |> Shared.decode_aws_chunked_stream(11)
             |> Enum.to_list()
             |> IO.iodata_to_binary() == "hello world"
    end

    test "enforces the decoded payload limit while streaming" do
      stream = Shared.decode_aws_chunked_stream(["5\r\nhello\r\n0\r\n\r\n"], 4)
      assert catch_throw(Enum.to_list(stream)) == {:error, :entity_too_large}
    end

    test "rejects a stream that ends before the terminal chunk" do
      stream = Shared.decode_aws_chunked_stream(["5\r\nhello\r\n"], 5)
      assert catch_throw(Enum.to_list(stream)) == {:error, :malformed_chunked}
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
