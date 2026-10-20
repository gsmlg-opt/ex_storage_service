defmodule ExStorageService.Storage.BucketCorsTest do
  use ExUnit.Case, async: true
  alias ExStorageService.Storage.BucketCORS

  test "origin, method and every requested header must match the same rule" do
    rules = [
      %{
        allowed_origins: ["https://*.example.test"],
        allowed_methods: ["GET", "PUT"],
        allowed_headers: ["x-amz-*", "content-type"],
        expose_headers: ["etag", "x-amz-version-id"],
        max_age_seconds: 60
      }
    ]

    assert {:ok, [rule]} = BucketCORS.validate(rules)

    assert {:ok, ^rule} =
             BucketCORS.match([rule], "https://app.example.test", "PUT", [
               "X-Amz-Meta-Owner",
               "Content-Type"
             ])

    assert :no_match = BucketCORS.match([rule], "https://app.example.test.evil", "PUT", [])
    assert :no_match = BucketCORS.match([rule], "https://app.example.test", "DELETE", [])
    assert :no_match = BucketCORS.match([rule], "https://app.example.test", "PUT", ["x-private"])
  end

  test "invalid wildcard, headers, count, method and max age fail validation" do
    base = %{allowed_origins: ["https://example.test"], allowed_methods: ["GET"]}

    for rules <- [
          [],
          List.duplicate(base, 101),
          [Map.put(base, :allowed_origins, ["https://*.*.test"])],
          [Map.put(base, :allowed_origins, ["https://example.test\r\nx-evil: injected"])],
          [Map.put(base, :allowed_methods, ["OPTIONS"])],
          [Map.put(base, :allowed_headers, ["x-one,x-two"])],
          [Map.put(base, :expose_headers, ["*"])],
          [Map.put(base, :max_age_seconds, -1)]
        ] do
      assert {:error, :invalid_cors} = BucketCORS.validate(rules)
    end
  end
end
