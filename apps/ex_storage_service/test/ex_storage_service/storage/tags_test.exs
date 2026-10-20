defmodule ExStorageService.Storage.TagsTest do
  use ExUnit.Case, async: true
  alias ExStorageService.Storage.Tags

  test "tag maps and URL encoded headers retain Unicode and literal delimiters" do
    assert {:ok, %{"team" => "研发", "space key" => "a+b /"}} =
             Tags.from_header("team=%E7%A0%94%E5%8F%91&space%20key=a%2Bb%20%2F")

    assert {:ok, %{}} = Tags.validate(%{})
    assert {:ok, %{"owner" => ""}} = Tags.validate([{"owner", ""}])
  end

  test "duplicate, reserved, malformed and excessive tags fail before persistence" do
    for tags <- [
          [{"a", "first"}, {"a", "second"}],
          %{"aws:managed" => "no"},
          %{"" => "value"},
          %{String.duplicate("k", 129) => "value"},
          %{"key" => String.duplicate("v", 257)},
          %{"key" => <<0>>},
          %{"key" => <<255>>},
          Enum.map(1..11, &{"key#{&1}", "value"})
        ] do
      assert {:error, :invalid_tag} = Tags.validate(tags)
    end

    assert {:error, :invalid_tag} = Tags.from_header("same=one&same=two")
    assert {:error, :invalid_tag} = Tags.from_header("broken=%XX")
    assert {:ok, tags} = Tags.validate(Enum.map(1..50, &{"key#{&1}", "value"}), 50)
    assert map_size(tags) == 50
  end
end
