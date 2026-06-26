defmodule ExStorageServiceCli.RmTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.Commands.Rm

  describe "parse_s3_path/1" do
    test "correctly parses paths with keys" do
      assert Rm.parse_s3_path("s3://my-bucket/my-key.txt") == {"my-bucket", "my-key.txt"}

      assert Rm.parse_s3_path("s3://my-bucket/nested/folder/file.json") ==
               {"my-bucket", "nested/folder/file.json"}
    end

    test "correctly parses paths with empty keys" do
      assert Rm.parse_s3_path("s3://my-bucket") == {"my-bucket", ""}
      assert Rm.parse_s3_path("s3://my-bucket/") == {"my-bucket", ""}
    end
  end
end
