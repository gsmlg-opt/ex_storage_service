defmodule ExStorageService.S3.Plugs.AuthorizeTest do
  use ExUnit.Case, async: true

  alias ExStorageService.S3.Plugs.Authorize

  describe "map_action/2" do
    test "GET [] maps to s3:ListAllMyBuckets" do
      assert Authorize.map_action("GET", []) == "s3:ListAllMyBuckets"
    end

    test "GET [bucket] maps to s3:ListBucket" do
      assert Authorize.map_action("GET", ["my-bucket"]) == "s3:ListBucket"
    end

    test "GET [bucket, key] maps to s3:GetObject" do
      assert Authorize.map_action("GET", ["my-bucket", "my-key"]) == "s3:GetObject"
    end

    test "HEAD [bucket] maps to s3:HeadBucket" do
      assert Authorize.map_action("HEAD", ["my-bucket"]) == "s3:HeadBucket"
    end

    test "PUT [bucket] maps to s3:CreateBucket" do
      assert Authorize.map_action("PUT", ["my-bucket"]) == "s3:CreateBucket"
    end

    test "PUT [bucket, key] maps to s3:PutObject" do
      assert Authorize.map_action("PUT", ["my-bucket", "my-key"]) == "s3:PutObject"
    end

    test "DELETE [bucket] maps to s3:DeleteBucket" do
      assert Authorize.map_action("DELETE", ["my-bucket"]) == "s3:DeleteBucket"
    end

    test "DELETE [bucket, key] maps to s3:DeleteObject" do
      assert Authorize.map_action("DELETE", ["my-bucket", "my-key"]) == "s3:DeleteObject"
    end

    test "POST [bucket] maps to s3:DeleteObject" do
      assert Authorize.map_action("POST", ["my-bucket"]) == "s3:DeleteObject"
    end

    test "POST [bucket, key] with uploads query maps to s3:PutObject" do
      assert Authorize.map_action("POST", ["my-bucket", "my-key"], %{"uploads" => ""}) ==
               "s3:PutObject"
    end

    test "POST [bucket, key] with uploadId query maps to s3:PutObject" do
      assert Authorize.map_action("POST", ["my-bucket", "my-key"], %{"uploadId" => "abc123"}) ==
               "s3:PutObject"
    end
  end

  describe "build_resource_arn/1" do
    test "empty path returns wildcard ARN" do
      assert Authorize.build_resource_arn([]) == "arn:ess:::*"
    end

    test "bucket-only path returns bucket ARN" do
      assert Authorize.build_resource_arn(["my-bucket"]) == "arn:ess:::my-bucket"
    end

    test "bucket with key path returns full ARN" do
      assert Authorize.build_resource_arn(["my-bucket", "path", "to", "key"]) ==
               "arn:ess:::my-bucket/path/to/key"
    end
  end

  describe "call/2 with auth disabled" do
    test "passes the connection through unchanged" do
      conn =
        Plug.Test.conn(:get, "/")
        |> Plug.Conn.assign(:user_id, "some-user")

      result = Authorize.call(conn, [])

      refute result.halted
      assert result.status != 403
    end
  end

  describe "call/2 with auth enabled" do
    setup do
      previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
      Application.put_env(:ex_storage_service, :s3_auth_enabled, true)

      on_exit(fn ->
        if previous do
          Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)
        else
          Application.delete_env(:ex_storage_service, :s3_auth_enabled)
        end
      end)

      :ok
    end

    test "root user bypasses authorization" do
      conn =
        Plug.Test.conn(:get, "/")
        |> Plug.Conn.assign(:user_id, "root")

      result = Authorize.call(conn, [])

      refute result.halted
    end
  end
end
