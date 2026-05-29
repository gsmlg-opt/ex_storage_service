defmodule ExStorageServiceCli.CpTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.Commands.Cp

  describe "relative_local_path/2" do
    test "returns basename if file is identical to base" do
      assert Cp.relative_local_path("./my-folder/a.txt", "./my-folder/a.txt") == "a.txt"
    end

    test "removes base directory prefix" do
      assert Cp.relative_local_path("./my-folder/a.txt", "./my-folder") == "a.txt"
      assert Cp.relative_local_path("./my-folder/sub/b.txt", "./my-folder") == "sub/b.txt"
      assert Cp.relative_local_path("./my-folder/sub/b.txt", "./my-folder/") == "sub/b.txt"
    end
  end

  describe "relative_key/2" do
    test "returns key as-is if prefix is empty" do
      assert Cp.relative_key("a/b/c.txt", "") == "a/b/c.txt"
    end

    test "removes prefix ending with slash" do
      assert Cp.relative_key("a/b/c.txt", "a/b/") == "c.txt"
      assert Cp.relative_key("a/b/sub/c.txt", "a/b/") == "sub/c.txt"
    end

    test "removes prefix not ending with slash" do
      assert Cp.relative_key("a/b/c.txt", "a/b") == "c.txt"
      assert Cp.relative_key("a/b", "a/b") == "b"
      assert Cp.relative_key("a/b/sub/c.txt", "a/b") == "sub/c.txt"
    end
  end
end
