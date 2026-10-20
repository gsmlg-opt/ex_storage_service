defmodule ExStorageService.IAM.PolicyHardeningTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.{Policy, User}

  setup do
    {:ok, user} = User.create_user("policy-hardening-#{System.unique_integer([:positive])}")

    on_exit(fn ->
      case Concord.get("user_policies:#{user.id}") do
        {:ok, ids} when is_list(ids) ->
          Enum.each(ids, &Concord.delete("policy:#{&1}"))

        _ ->
          :ok
      end

      Concord.delete("user_policies:#{user.id}")
      User.delete_user(user.id)
    end)

    %{user: user}
  end

  test "canonical and legacy resource prefixes preserve the same exact scope", %{user: user} do
    attach(user.id, :allow, ["s3:GetObject"], ["arn:ess:::private/allowed/*"])
    assert :allow = Policy.evaluate(user.id, "s3:GetObject", "arn:aws:s3:::private/allowed/a")
    assert :deny = Policy.evaluate(user.id, "s3:GetObject", "arn:aws:s3:::private/other/a")
  end

  test "a missing attached deny policy fails the entire policy read closed", %{user: user} do
    policy = attach(user.id, :allow, ["s3:*"], ["*"])
    :ok = Concord.put("user_policies:#{user.id}", [policy.id, "missing-deny-policy"])
    assert {:error, _} = Policy.get_user_policies(user.id)
    assert :deny = Policy.evaluate(user.id, "s3:GetObject", "arn:aws:s3:::private/a")
  end

  test "unsupported policy conditions are rejected rather than ignored" do
    assert {:error, :invalid_policy} =
             Policy.create_policy("conditional", [
               %{effect: :allow, actions: ["s3:*"], resources: ["*"], condition: %{ip: "x"}}
             ])
  end

  test "legacy HEAD-only grants do not become GET grants", %{user: user} do
    attach_legacy(user.id, "s3:HeadObject")

    assert :allow =
             Policy.evaluate(user.id, "s3:GetObject", "arn:aws:s3:::private/a",
               legacy_action: "s3:HeadObject"
             )

    assert :deny =
             Policy.evaluate(user.id, "s3:GetObject", "arn:aws:s3:::private/a",
               legacy_action: "s3:GetObject"
             )
  end

  test "legacy PutObject is not broadened to multipart initiation", %{user: user} do
    attach_legacy(user.id, "s3:PutObject")

    assert :deny =
             Policy.evaluate(user.id, "s3:PutObject", "arn:aws:s3:::private/a",
               legacy_action: "s3:CreateMultipartUpload"
             )
  end

  test "standard actions authorize HEAD and multipart while legacy denies remain effective", %{
    user: user
  } do
    attach(user.id, :allow, ["s3:GetObject", "s3:PutObject"], ["arn:aws:s3:::private/*"])

    assert :allow =
             Policy.evaluate(user.id, "s3:GetObject", "arn:aws:s3:::private/a",
               legacy_action: "s3:HeadObject"
             )

    assert :allow =
             Policy.evaluate(user.id, "s3:PutObject", "arn:aws:s3:::private/a",
               legacy_action: "s3:CreateMultipartUpload"
             )

    attach(user.id, :deny, ["s3:HeadObject"], ["arn:ess:::private/*"])

    assert :deny =
             Policy.evaluate(user.id, "s3:GetObject", "arn:aws:s3:::private/a",
               legacy_action: "s3:HeadObject"
             )
  end

  defp attach(user_id, effect, actions, resources) do
    {:ok, policy} =
      Policy.create_policy("hardening", [
        %{effect: effect, actions: actions, resources: resources}
      ])

    :ok = Policy.attach_policy(user_id, policy.id)
    policy
  end

  defp attach_legacy(user_id, action) do
    id = "legacy-#{System.unique_integer([:positive])}"

    :ok =
      Concord.put("policy:#{id}", %{
        id: id,
        statements: [%{effect: :allow, actions: [action], resources: ["arn:ess:::private/*"]}]
      })

    :ok = Policy.attach_policy(user_id, id)
  end
end
