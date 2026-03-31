defmodule ExStorageService.IAM.Policy do
  @moduledoc """
  IAM Policy engine backed by Concord key-value store.

  Policies are stored with the key pattern: "policy:{policy_id}"
  User-policy attachments are stored as: "user_policies:{user_id}"

  Policy evaluation follows AWS-style logic:
  1. Collect all matching statements from the user's attached policies
  2. If any statement has effect :deny -> DENY
  3. If any statement has effect :allow -> ALLOW
  4. Otherwise -> DENY (default deny)

  Statement format:
    %{effect: :allow | :deny, actions: ["s3:GetObject", "s3:*"], resources: ["arn:ess:::bucket/*"]}
  """

  @type effect :: :allow | :deny
  @type statement :: %{effect: effect(), actions: [String.t()], resources: [String.t()]}
  @type t :: %{
          id: String.t(),
          name: String.t(),
          statements: [statement()],
          created_at: String.t()
        }

  # ── Policy CRUD ──

  @doc """
  Creates a new policy with the given name and list of statements.
  """
  @spec create_policy(String.t(), [statement()]) :: {:ok, t()} | {:error, term()}
  def create_policy(name, statements) do
    policy_id = generate_policy_id()
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    policy = %{
      id: policy_id,
      name: name,
      statements: statements,
      created_at: now
    }

    case Concord.put("policy:#{policy_id}", policy) do
      :ok -> {:ok, policy}
      error -> error
    end
  end

  @doc """
  Retrieves a policy by its ID.
  """
  @spec get_policy(String.t()) :: {:ok, t()} | {:error, :not_found | term()}
  def get_policy(policy_id) do
    case Concord.get("policy:#{policy_id}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, policy} -> {:ok, policy}
      error -> error
    end
  end

  @doc """
  Lists all policies.
  """
  @spec list_policies() :: {:ok, [t()]} | {:error, term()}
  def list_policies do
    case Concord.get_all() do
      {:ok, all} ->
        policies =
          all
          |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "policy:") end)
          |> Enum.map(fn {_k, v} -> v end)

        {:ok, policies}

      error ->
        error
    end
  end

  @doc """
  Updates a policy's fields (e.g., statements).
  """
  @spec update_policy(String.t(), map()) :: {:ok, t()} | {:error, :not_found | term()}
  def update_policy(policy_id, attrs) do
    case get_policy(policy_id) do
      {:ok, policy} ->
        updated = Map.merge(policy, attrs)

        case Concord.put("policy:#{policy_id}", updated) do
          :ok -> {:ok, updated}
          error -> error
        end

      error ->
        error
    end
  end

  @doc """
  Deletes a policy by its ID.
  """
  @spec delete_policy(String.t()) :: :ok | {:error, :not_found | term()}
  def delete_policy(policy_id) do
    case get_policy(policy_id) do
      {:ok, _} -> Concord.delete("policy:#{policy_id}")
      error -> error
    end
  end

  # ── Policy attachment ──

  @doc """
  Attaches a policy to a user.
  """
  @spec attach_policy(String.t(), String.t()) :: :ok | {:error, term()}
  def attach_policy(user_id, policy_id) do
    key = "user_policies:#{user_id}"

    policy_ids =
      case Concord.get(key) do
        {:ok, nil} -> []
        {:ok, ids} -> ids
        _ -> []
      end

    unless policy_id in policy_ids do
      Concord.put(key, policy_ids ++ [policy_id])
    else
      :ok
    end
  end

  @doc """
  Detaches a policy from a user.
  """
  @spec detach_policy(String.t(), String.t()) :: :ok | {:error, term()}
  def detach_policy(user_id, policy_id) do
    key = "user_policies:#{user_id}"

    case Concord.get(key) do
      {:ok, nil} ->
        :ok

      {:ok, ids} ->
        updated = List.delete(ids, policy_id)
        Concord.put(key, updated)

      error ->
        error
    end
  end

  @doc """
  Gets all policies attached to a user.
  """
  @spec get_user_policies(String.t()) :: {:ok, [t()]} | {:error, term()}
  def get_user_policies(user_id) do
    key = "user_policies:#{user_id}"

    case Concord.get(key) do
      {:ok, nil} ->
        {:ok, []}

      {:ok, policy_ids} ->
        policies =
          policy_ids
          |> Enum.map(&get_policy/1)
          |> Enum.filter(fn
            {:ok, _} -> true
            _ -> false
          end)
          |> Enum.map(fn {:ok, p} -> p end)

        {:ok, policies}

      error ->
        error
    end
  end

  # ── Policy evaluation ──

  @doc """
  Evaluates whether a user is allowed to perform an action on a resource.

  Returns :allow or :deny.
  """
  @spec evaluate(String.t(), String.t(), String.t()) :: :allow | :deny
  def evaluate(user_id, action, resource) do
    case get_user_policies(user_id) do
      {:ok, policies} ->
        statements =
          policies
          |> Enum.flat_map(fn p -> p.statements end)
          |> Enum.filter(fn stmt ->
            actions_match?(stmt.actions, action) and resources_match?(stmt.resources, resource)
          end)

        cond do
          Enum.any?(statements, fn s -> s.effect == :deny end) -> :deny
          Enum.any?(statements, fn s -> s.effect == :allow end) -> :allow
          true -> :deny
        end

      {:error, _} ->
        :deny
    end
  end

  # ── Pattern matching ──

  @doc """
  Checks if an action matches any of the given action patterns.
  Supports wildcard patterns like "s3:*" or "*".
  """
  @spec action_matches?(String.t(), String.t()) :: boolean()
  def action_matches?(pattern, action) do
    pattern == "*" or pattern == action or
      (String.ends_with?(pattern, "*") and
         String.starts_with?(action, String.trim_trailing(pattern, "*")))
  end

  @doc """
  Checks if a resource matches any of the given resource patterns.
  Supports glob-style patterns like "arn:ess:::bucket/*" or "*".
  """
  @spec resource_matches?(String.t(), String.t()) :: boolean()
  def resource_matches?(pattern, resource) do
    pattern == "*" or pattern == resource or glob_match?(pattern, resource)
  end

  # ── Predefined policy templates ──

  @doc """
  Returns a ReadOnly policy statement list.
  """
  @spec read_only_statements() :: [statement()]
  def read_only_statements do
    [
      %{
        effect: :allow,
        actions: ["s3:GetObject", "s3:HeadObject", "s3:ListBucket", "s3:ListAllMyBuckets"],
        resources: ["*"]
      }
    ]
  end

  @doc """
  Returns a ReadWrite policy statement list.
  """
  @spec read_write_statements() :: [statement()]
  def read_write_statements do
    [
      %{
        effect: :allow,
        actions: [
          "s3:GetObject",
          "s3:HeadObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:ListAllMyBuckets"
        ],
        resources: ["*"]
      }
    ]
  end

  @doc """
  Returns a FullAccess policy statement list.
  """
  @spec full_access_statements() :: [statement()]
  def full_access_statements do
    [
      %{
        effect: :allow,
        actions: ["s3:*"],
        resources: ["*"]
      }
    ]
  end

  @doc """
  Returns a bucket-scoped policy statement list for the given bucket.
  Allows all S3 actions but only on the specified bucket and its objects.
  """
  @spec bucket_scoped_statements(String.t()) :: [statement()]
  def bucket_scoped_statements(bucket) do
    [
      %{
        effect: :allow,
        actions: ["s3:*"],
        resources: ["arn:ess:::#{bucket}", "arn:ess:::#{bucket}/*"]
      }
    ]
  end

  # ── Private helpers ──

  defp actions_match?(patterns, action) do
    Enum.any?(patterns, fn pattern -> action_matches?(pattern, action) end)
  end

  defp resources_match?(patterns, resource) do
    Enum.any?(patterns, fn pattern -> resource_matches?(pattern, resource) end)
  end

  defp glob_match?(pattern, string) do
    regex_str =
      pattern
      |> Regex.escape()
      |> String.replace("\\*", ".*")

    case Regex.compile("^#{regex_str}$") do
      {:ok, regex} -> Regex.match?(regex, string)
      _ -> false
    end
  end

  defp generate_policy_id do
    suffix =
      :crypto.strong_rand_bytes(8)
      |> Base.encode32(case: :lower, padding: false)
      |> String.slice(0, 12)

    "pol_#{suffix}"
  end
end
