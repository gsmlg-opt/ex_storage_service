defmodule ExStorageService.IAM.PolicyTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.Policy
  alias ExStorageService.IAM.User

  setup do
    # Create a test user for policy evaluation
    {:ok, user} = User.create_user("test-policy-user-#{:erlang.unique_integer([:positive])}")

    on_exit(fn ->
      # Clean up: detach policies, delete user
      case Policy.get_user_policies(user.id) do
        {:ok, policies} ->
          Enum.each(policies, fn p ->
            Policy.detach_policy(user.id, p.id)
            Policy.delete_policy(p.id)
          end)

        _ ->
          :ok
      end

      User.delete_user(user.id)
    end)

    %{user: user}
  end

  describe "default deny" do
    test "denies access when no policies are attached", %{user: user} do
      assert Policy.evaluate(user.id, "s3:GetObject", "arn:ess:::mybucket/key.txt") == :deny
    end

    test "denies access for non-existent user" do
      assert Policy.evaluate("usr_nonexistent", "s3:GetObject", "arn:ess:::mybucket/key.txt") ==
               :deny
    end
  end

  describe "explicit allow" do
    test "allows access when policy grants the action", %{user: user} do
      {:ok, policy} =
        Policy.create_policy("allow-get", [
          %{
            effect: :allow,
            actions: ["s3:GetObject"],
            resources: ["arn:ess:::mybucket/key.txt"]
          }
        ])

      Policy.attach_policy(user.id, policy.id)

      assert Policy.evaluate(user.id, "s3:GetObject", "arn:ess:::mybucket/key.txt") == :allow
    end

    test "denies access when action does not match", %{user: user} do
      {:ok, policy} =
        Policy.create_policy("allow-get-only", [
          %{
            effect: :allow,
            actions: ["s3:GetObject"],
            resources: ["*"]
          }
        ])

      Policy.attach_policy(user.id, policy.id)

      assert Policy.evaluate(user.id, "s3:PutObject", "arn:ess:::mybucket/key.txt") == :deny
    end

    test "denies access when resource does not match", %{user: user} do
      {:ok, policy} =
        Policy.create_policy("allow-specific-bucket", [
          %{
            effect: :allow,
            actions: ["s3:GetObject"],
            resources: ["arn:ess:::other-bucket/*"]
          }
        ])

      Policy.attach_policy(user.id, policy.id)

      assert Policy.evaluate(user.id, "s3:GetObject", "arn:ess:::mybucket/key.txt") == :deny
    end
  end

  describe "explicit deny wins over allow" do
    test "deny takes precedence over allow", %{user: user} do
      {:ok, allow_policy} =
        Policy.create_policy("allow-all", [
          %{
            effect: :allow,
            actions: ["s3:*"],
            resources: ["*"]
          }
        ])

      {:ok, deny_policy} =
        Policy.create_policy("deny-deletes", [
          %{
            effect: :deny,
            actions: ["s3:DeleteObject"],
            resources: ["*"]
          }
        ])

      Policy.attach_policy(user.id, allow_policy.id)
      Policy.attach_policy(user.id, deny_policy.id)

      # Allow should still work for non-denied actions
      assert Policy.evaluate(user.id, "s3:GetObject", "arn:ess:::mybucket/key.txt") == :allow

      # Deny should win for DeleteObject
      assert Policy.evaluate(user.id, "s3:DeleteObject", "arn:ess:::mybucket/key.txt") == :deny
    end
  end

  describe "wildcard action matching" do
    test "s3:* matches any s3 action" do
      assert Policy.action_matches?("s3:*", "s3:GetObject") == true
      assert Policy.action_matches?("s3:*", "s3:PutObject") == true
      assert Policy.action_matches?("s3:*", "s3:DeleteBucket") == true
    end

    test "* matches anything" do
      assert Policy.action_matches?("*", "s3:GetObject") == true
      assert Policy.action_matches?("*", "anything") == true
    end

    test "exact match works" do
      assert Policy.action_matches?("s3:GetObject", "s3:GetObject") == true
      assert Policy.action_matches?("s3:GetObject", "s3:PutObject") == false
    end

    test "partial wildcard works" do
      assert Policy.action_matches?("s3:Get*", "s3:GetObject") == true
      assert Policy.action_matches?("s3:Get*", "s3:GetBucketAcl") == true
      assert Policy.action_matches?("s3:Get*", "s3:PutObject") == false
    end
  end

  describe "resource glob matching" do
    test "exact resource match" do
      assert Policy.resource_matches?("arn:ess:::mybucket", "arn:ess:::mybucket") == true
      assert Policy.resource_matches?("arn:ess:::mybucket", "arn:ess:::other") == false
    end

    test "wildcard * matches all resources" do
      assert Policy.resource_matches?("*", "arn:ess:::mybucket/key.txt") == true
    end

    test "glob pattern with trailing wildcard" do
      assert Policy.resource_matches?("arn:ess:::mybucket/*", "arn:ess:::mybucket/key.txt") ==
               true

      assert Policy.resource_matches?("arn:ess:::mybucket/*", "arn:ess:::mybucket/dir/key.txt") ==
               true

      assert Policy.resource_matches?("arn:ess:::other/*", "arn:ess:::mybucket/key.txt") == false
    end

    test "glob pattern matches nested paths" do
      assert Policy.resource_matches?(
               "arn:ess:::mybucket/logs/*",
               "arn:ess:::mybucket/logs/2024/01/file.log"
             ) == true
    end
  end

  describe "predefined policy templates" do
    test "read_only_statements allows read actions", %{user: user} do
      {:ok, policy} = Policy.create_policy("ReadOnly", Policy.read_only_statements())
      Policy.attach_policy(user.id, policy.id)

      assert Policy.evaluate(user.id, "s3:GetObject", "arn:ess:::mybucket/key.txt") == :allow
      assert Policy.evaluate(user.id, "s3:HeadObject", "arn:ess:::mybucket/key.txt") == :allow
      assert Policy.evaluate(user.id, "s3:ListBucket", "arn:ess:::mybucket") == :allow

      assert Policy.evaluate(user.id, "s3:ListAllMyBuckets", "arn:ess:::*") == :allow
      assert Policy.evaluate(user.id, "s3:PutObject", "arn:ess:::mybucket/key.txt") == :deny
      assert Policy.evaluate(user.id, "s3:DeleteObject", "arn:ess:::mybucket/key.txt") == :deny
    end

    test "read_write_statements allows read and write actions", %{user: user} do
      {:ok, policy} = Policy.create_policy("ReadWrite", Policy.read_write_statements())
      Policy.attach_policy(user.id, policy.id)

      assert Policy.evaluate(user.id, "s3:GetObject", "arn:ess:::mybucket/key.txt") == :allow
      assert Policy.evaluate(user.id, "s3:PutObject", "arn:ess:::mybucket/key.txt") == :allow
      assert Policy.evaluate(user.id, "s3:DeleteObject", "arn:ess:::mybucket/key.txt") == :allow
      assert Policy.evaluate(user.id, "s3:CreateBucket", "arn:ess:::mybucket") == :deny
    end

    test "full_access_statements allows everything", %{user: user} do
      {:ok, policy} = Policy.create_policy("FullAccess", Policy.full_access_statements())
      Policy.attach_policy(user.id, policy.id)

      assert Policy.evaluate(user.id, "s3:GetObject", "arn:ess:::mybucket/key.txt") == :allow
      assert Policy.evaluate(user.id, "s3:PutObject", "arn:ess:::mybucket/key.txt") == :allow
      assert Policy.evaluate(user.id, "s3:DeleteBucket", "arn:ess:::mybucket") == :allow
      assert Policy.evaluate(user.id, "s3:CreateBucket", "arn:ess:::newbucket") == :allow
    end

    test "bucket_scoped_statements restricts to a specific bucket", %{user: user} do
      {:ok, policy} =
        Policy.create_policy("BucketScoped", Policy.bucket_scoped_statements("mybucket"))

      Policy.attach_policy(user.id, policy.id)

      # Allowed on mybucket
      assert Policy.evaluate(user.id, "s3:GetObject", "arn:ess:::mybucket/key.txt") == :allow
      assert Policy.evaluate(user.id, "s3:PutObject", "arn:ess:::mybucket/key.txt") == :allow
      assert Policy.evaluate(user.id, "s3:DeleteBucket", "arn:ess:::mybucket") == :allow

      # Denied on other buckets
      assert Policy.evaluate(user.id, "s3:GetObject", "arn:ess:::other/key.txt") == :deny
      assert Policy.evaluate(user.id, "s3:PutObject", "arn:ess:::other/key.txt") == :deny
    end
  end
end
