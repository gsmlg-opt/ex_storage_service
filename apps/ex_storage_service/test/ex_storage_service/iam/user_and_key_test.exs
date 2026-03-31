defmodule ExStorageService.IAM.UserAndKeyTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.User
  alias ExStorageService.IAM.AccessKey
  alias ExStorageService.IAM.Audit

  setup do
    # Ensure master_key is configured for AccessKey encryption
    unless Application.get_env(:ex_storage_service, :master_key) do
      key = :crypto.strong_rand_bytes(32) |> Base.encode64()
      Application.put_env(:ex_storage_service, :master_key, key)
    end

    :ok
  end

  # ── User CRUD ──────────────────────────────────────────────────────

  describe "User.create_user/1" do
    test "creates a user with a generated id and active status" do
      {:ok, user} = User.create_user("alice")

      assert String.starts_with?(user.id, "usr_")
      assert user.name == "alice"
      assert user.status == :active
      assert user.created_at != nil
      assert user.updated_at != nil

      # cleanup
      User.delete_user(user.id)
    end
  end

  describe "User.get_user/1" do
    test "retrieves an existing user" do
      {:ok, created} = User.create_user("bob")
      {:ok, fetched} = User.get_user(created.id)

      assert fetched.id == created.id
      assert fetched.name == "bob"
      assert fetched.status == :active

      User.delete_user(created.id)
    end

    test "returns {:error, :not_found} for unknown id" do
      assert {:error, :not_found} = User.get_user("usr_does_not_exist")
    end
  end

  describe "User.list_users/0" do
    test "includes a newly created user" do
      {:ok, user} = User.create_user("carol-#{:erlang.unique_integer([:positive])}")
      {:ok, users} = User.list_users()

      assert Enum.any?(users, fn u -> u.id == user.id end)

      User.delete_user(user.id)
    end
  end

  describe "User.suspend_user/1 and activate_user/1" do
    test "suspends an active user" do
      {:ok, user} = User.create_user("dave")
      {:ok, suspended} = User.suspend_user(user.id)

      assert suspended.status == :suspended
      assert suspended.id == user.id

      User.delete_user(user.id)
    end

    test "re-activates a suspended user" do
      {:ok, user} = User.create_user("eve")
      {:ok, _} = User.suspend_user(user.id)
      {:ok, activated} = User.activate_user(user.id)

      assert activated.status == :active

      User.delete_user(user.id)
    end

    test "suspend returns error for non-existent user" do
      assert {:error, :not_found} = User.suspend_user("usr_nonexistent")
    end
  end

  describe "User.delete_user/1" do
    test "deletes a user so it can no longer be fetched" do
      {:ok, user} = User.create_user("frank")
      assert :ok = User.delete_user(user.id)
      assert {:error, :not_found} = User.get_user(user.id)
    end

    test "returns error when deleting non-existent user" do
      assert {:error, :not_found} = User.delete_user("usr_ghost")
    end
  end

  # ── AccessKey CRUD ─────────────────────────────────────────────────

  describe "AccessKey.create_access_key/1" do
    test "creates key pair for an existing user" do
      {:ok, user} = User.create_user("key-user")
      {:ok, key} = AccessKey.create_access_key(user.id)

      assert String.starts_with?(key.access_key_id, "AKIA")
      assert byte_size(key.secret_access_key) > 0
      assert key.user_id == user.id
      assert key.status == :active

      AccessKey.delete_key(key.access_key_id)
      User.delete_user(user.id)
    end

    test "fails for non-existent user" do
      assert {:error, :not_found} = AccessKey.create_access_key("usr_nope")
    end
  end

  describe "AccessKey.get_access_key/1" do
    test "returns key with decrypted secret that matches original" do
      {:ok, user} = User.create_user("get-key-user")
      {:ok, created} = AccessKey.create_access_key(user.id)
      {:ok, fetched} = AccessKey.get_access_key(created.access_key_id)

      assert fetched.secret_access_key == created.secret_access_key
      assert fetched.user_id == user.id
      assert fetched.status == :active

      AccessKey.delete_key(created.access_key_id)
      User.delete_user(user.id)
    end

    test "returns error for unknown key id" do
      assert {:error, :not_found} = AccessKey.get_access_key("AKIA_DOESNOTEXIST")
    end
  end

  describe "AccessKey.list_user_keys/1" do
    test "lists keys for a user without exposing secrets" do
      {:ok, user} = User.create_user("list-key-user")
      {:ok, key} = AccessKey.create_access_key(user.id)
      {:ok, keys} = AccessKey.list_user_keys(user.id)

      assert length(keys) >= 1
      listed = Enum.find(keys, fn k -> k.access_key_id == key.access_key_id end)
      assert listed != nil
      assert listed.user_id == user.id
      refute Map.has_key?(listed, :secret_access_key)

      AccessKey.delete_key(key.access_key_id)
      User.delete_user(user.id)
    end
  end

  describe "AccessKey.deactivate_key/1 and activate_key/1" do
    test "deactivates and re-activates a key" do
      {:ok, user} = User.create_user("toggle-key-user")
      {:ok, key} = AccessKey.create_access_key(user.id)

      {:ok, deactivated} = AccessKey.deactivate_key(key.access_key_id)
      assert deactivated.status == :inactive

      {:ok, reactivated} = AccessKey.activate_key(key.access_key_id)
      assert reactivated.status == :active

      AccessKey.delete_key(key.access_key_id)
      User.delete_user(user.id)
    end
  end

  describe "AccessKey.delete_key/1" do
    test "deletes a key so it can no longer be fetched" do
      {:ok, user} = User.create_user("del-key-user")
      {:ok, key} = AccessKey.create_access_key(user.id)

      assert :ok = AccessKey.delete_key(key.access_key_id)
      assert {:error, :not_found} = AccessKey.get_access_key(key.access_key_id)

      User.delete_user(user.id)
    end
  end

  # ── Audit Logging ──────────────────────────────────────────────────

  describe "Audit.log_event/4 and list_events/1" do
    test "logs an event and retrieves it" do
      {:ok, event} = Audit.log_event("admin", :create_user, "usr_test123", %{name: "test"})

      assert event.actor == "admin"
      assert event.action == :create_user
      assert event.target == "usr_test123"
      assert event.details == %{name: "test"}
      assert event.timestamp != nil
      assert event.id != nil

      {:ok, events} = Audit.list_events(actor: "admin")
      assert Enum.any?(events, fn e -> e.id == event.id end)
    end

    test "filters events by action" do
      unique = :erlang.unique_integer([:positive])
      target = "usr_filter_#{unique}"

      {:ok, _} = Audit.log_event("system", :suspend_user, target)
      {:ok, _} = Audit.log_event("system", :delete_user, target)

      {:ok, suspend_events} = Audit.list_events(action: :suspend_user, target: target)
      assert length(suspend_events) >= 1
      assert Enum.all?(suspend_events, fn e -> e.action == :suspend_user end)
    end
  end
end
