defmodule ExStorageServiceSdk.S3.ListingEncodingTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceSdk.Error
  alias ExStorageServiceSdk.S3.Customizations

  @listing_fields [
    {"ListObjects",
     ["Delimiter", "Marker", "NextMarker", "Contents.Key", "CommonPrefixes.Prefix"]},
    {"ListObjectsV2",
     ["Delimiter", "Prefix", "StartAfter", "Contents.Key", "CommonPrefixes.Prefix"]},
    {"ListObjectVersions",
     [
       "KeyMarker",
       "NextKeyMarker",
       "Prefix",
       "Delimiter",
       "Versions.Key",
       "DeleteMarkers.Key",
       "CommonPrefixes.Prefix"
     ]}
  ]

  for {operation, fields} <- @listing_fields,
      field <- fields do
    test "#{operation} rejects invalid UTF-8 decoded from #{field}" do
      operation = unquote(operation)
      result = listing_field(unquote(field), "bad%FF")

      assert {:error, %Error{category: :protocol, code: :invalid_utf8, operation: ^operation}} =
               Customizations.output(operation, result, %{automatic_encoding: true})
    end
  end

  test "automatic listing decoding rejects malformed UTF-8 sequences without exposing the value" do
    for encoded <- ["%C0%AF", "%E9%9B", "%ED%A0%80", "%F4%90%80%80"] do
      assert {:error, %Error{category: :protocol, code: :invalid_utf8} = error} =
               Customizations.output(
                 "ListObjectsV2",
                 listing_field("Contents.Key", "private-key" <> encoded),
                 %{automatic_encoding: true}
               )

      refute inspect(error) =~ "private-key"
    end
  end

  test "automatic decoding preserves valid UTF-8, literal malformed escapes and single decoding" do
    for {operation, fields} <- @listing_fields,
        field <- fields,
        {encoded, decoded} <- [
          {"%E9%9B%AA%F0%9F%8C%8D+%2B%252F", "雪🌍 +%2F"},
          {"bad%xx%2%", "bad%xx%2%"}
        ] do
      result = listing_field(field, encoded)
      expected = listing_field(field, decoded)

      assert {:ok, ^expected} =
               Customizations.output(operation, result, %{automatic_encoding: true})
    end
  end

  test "caller-selected encoding leaves encoded fields unchanged" do
    for {operation, fields} <- @listing_fields,
        field <- fields do
      result = listing_field(field, "bad%FF")

      assert {:ok, ^result} =
               Customizations.output(operation, result, %{automatic_encoding: false})
    end
  end

  test "only automatic URL listing output is decoded" do
    result = %{
      "EncodingType" => "url",
      "ContinuationToken" => "opaque%FF",
      "NextContinuationToken" => "opaque%FF",
      "VersionIdMarker" => "opaque%FF",
      "NextVersionIdMarker" => "opaque%FF"
    }

    for {operation, _fields} <- @listing_fields do
      assert {:ok, ^result} =
               Customizations.output(operation, result, %{automatic_encoding: true})
    end

    without_encoding = %{"Contents" => [%{"Key" => "bad%FF"}]}

    assert {:ok, ^without_encoding} =
             Customizations.output("ListObjectsV2", without_encoding, %{automatic_encoding: true})

    assert {:ok, ^result} = Customizations.output("GetObject", result, %{})
  end

  defp listing_field(field, value) do
    case String.split(field, ".") do
      [field] -> %{"EncodingType" => "url", field => value}
      [collection, field] -> %{"EncodingType" => "url", collection => [%{field => value}]}
    end
  end
end
