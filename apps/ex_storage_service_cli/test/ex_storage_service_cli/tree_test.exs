defmodule ExStorageServiceCli.TreeTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.Commands.Tree

  describe "format_tree/3" do
    test "renders nested S3 object keys as a directory tree" do
      objects = [
        %{key: "docs/readme.md"},
        %{key: "images/raw/photo.jpg"},
        %{key: "images/hero.png"}
      ]

      assert Tree.format_tree("my-bucket", "", objects) == [
               "s3://my-bucket/",
               "├── docs/",
               "│   └── readme.md",
               "└── images/",
               "    ├── hero.png",
               "    └── raw/",
               "        └── photo.jpg",
               "",
               "3 directories, 3 files"
             ]
    end
  end
end
