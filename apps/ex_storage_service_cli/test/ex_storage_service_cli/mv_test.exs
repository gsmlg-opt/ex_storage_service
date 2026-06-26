defmodule ExStorageServiceCli.MvTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.Commands.Mv

  describe "relative_key/2" do
    test "returns key as-is if prefix is empty" do
      assert Mv.relative_key("a/b/c.txt", "") == "a/b/c.txt"
    end

    test "removes prefix ending with slash" do
      assert Mv.relative_key("a/b/c.txt", "a/b/") == "c.txt"
      assert Mv.relative_key("a/b/sub/c.txt", "a/b/") == "sub/c.txt"
    end

    test "removes prefix not ending with slash" do
      assert Mv.relative_key("a/b/c.txt", "a/b") == "c.txt"
      assert Mv.relative_key("a/b", "a/b") == "b"
      assert Mv.relative_key("a/b/sub/c.txt", "a/b") == "sub/c.txt"
    end
  end
end
