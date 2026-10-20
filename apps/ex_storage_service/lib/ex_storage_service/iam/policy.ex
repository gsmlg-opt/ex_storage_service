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
    %{effect: :allow | :deny, actions: ["s3:GetObject", "s3:*"], resources: ["arn:aws:s3:::bucket/*"]}

  New policies use AWS S3 action semantics. Stored policies without a semantics
  marker retain their original ESS operation-specific allows. Request callers
  supply the original operation action with `:legacy_action`; explicit denies
  are evaluated against both forms. Legacy `arn:ess:::` resources remain aliases
  for the same canonical S3 resource, without changing bucket or key scope.
  Conditions, principals and other unsupported statement semantics are rejected.
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
    with :ok <- validate_statements(statements) do
      persist_policy(name, statements)
    end
  end

  defp persist_policy(name, statements) do
    policy_id = generate_policy_id()
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    policy = %{
      id: policy_id,
      name: name,
      semantics: :aws_s3_v1,
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
    case Concord.get("policy:#{policy_id}", consistency: :linearizable) do
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

        with :ok <- validate_statements(updated.statements),
             true <- Map.get(updated, :semantics) == Map.get(policy, :semantics),
             :ok <- Concord.put("policy:#{policy_id}", updated) do
          {:ok, updated}
        else
          false -> {:error, :invalid_policy}
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

    case Concord.get(key, consistency: :linearizable) do
      {:ok, nil} ->
        Concord.put(key, [policy_id])

      {:error, :not_found} ->
        Concord.put(key, [policy_id])

      {:ok, ids} when is_list(ids) ->
        if policy_id in ids, do: :ok, else: Concord.put(key, ids ++ [policy_id])

      {:ok, _} ->
        {:error, :invalid_policy_attachment}

      error ->
        error
    end
  end

  @doc """
  Detaches a policy from a user.
  """
  @spec detach_policy(String.t(), String.t()) :: :ok | {:error, term()}
  def detach_policy(user_id, policy_id) do
    key = "user_policies:#{user_id}"

    case Concord.get(key, consistency: :linearizable) do
      {:ok, nil} ->
        :ok

      {:error, :not_found} ->
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

    case Concord.get(key, consistency: :linearizable) do
      {:ok, nil} ->
        {:ok, []}

      {:error, :not_found} ->
        {:ok, []}

      {:ok, policy_ids} when is_list(policy_ids) ->
        Enum.reduce_while(policy_ids, {:ok, []}, fn id, {:ok, policies} ->
          case get_policy(id) do
            {:ok, policy} -> {:cont, {:ok, [policy | policies]}}
            {:error, _} = error -> {:halt, error}
          end
        end)

      {:ok, _} ->
        {:error, :invalid_policy_attachment}

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
  def evaluate(user_id, action, resource, opts \\ []) do
    legacy_action = Keyword.get(opts, :legacy_action, action)
    action = canonical_action(action)

    case get_user_policies(user_id) do
      {:ok, policies} ->
        if Enum.all?(policies, &valid_stored_policy?/1) do
          effects =
            Enum.flat_map(policies, fn policy ->
              policy.statements
              |> Enum.filter(fn statement ->
                actions = request_actions(policy, statement.effect, action, legacy_action)

                Enum.any?(actions, &actions_match?(statement.actions, &1)) and
                  resources_match?(statement.resources, resource)
              end)
              |> Enum.map(& &1.effect)
            end)

          cond do
            :deny in effects -> :deny
            :allow in effects -> :allow
            true -> :deny
          end
        else
          :deny
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
    pattern = canonical_resource(pattern)
    resource = canonical_resource(resource)
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
        actions: ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket", "s3:ListAllMyBuckets"],
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
          "s3:GetObjectVersion",
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
        resources: ["arn:aws:s3:::#{bucket}", "arn:aws:s3:::#{bucket}/*"]
      }
    ]
  end

  # ── Private helpers ──

  defp request_actions(%{semantics: :aws_s3_v1}, _effect, action, legacy_action) do
    if legacy_action in [
         "s3:HeadObject",
         "s3:HeadBucket",
         "s3:CreateMultipartUpload",
         "s3:UploadPart",
         "s3:CompleteMultipartUpload"
       ],
       do: [action, legacy_action],
       else: [action]
  end

  defp request_actions(_legacy_policy, :deny, action, legacy_action),
    do: [action, legacy_action]

  defp request_actions(_legacy_policy, :allow, _action, legacy_action), do: [legacy_action]

  defp valid_stored_policy?(%{statements: statements} = policy) do
    Map.get(policy, :semantics) in [nil, :aws_s3_v1] and
      validate_statements(statements) == :ok
  end

  defp valid_stored_policy?(_), do: false

  defp validate_statements(statements) when is_list(statements) do
    if Enum.all?(statements, &valid_statement?/1), do: :ok, else: {:error, :invalid_policy}
  end

  defp validate_statements(_), do: {:error, :invalid_policy}

  defp valid_statement?(%{effect: effect, actions: actions, resources: resources} = statement) do
    map_size(statement) == 3 and effect in [:allow, :deny] and
      string_patterns?(actions) and string_patterns?(resources)
  end

  defp valid_statement?(_), do: false

  defp string_patterns?(patterns) when is_list(patterns) and patterns != [],
    do: Enum.all?(patterns, &(is_binary(&1) and byte_size(&1) > 0))

  defp string_patterns?(_), do: false

  defp canonical_action("s3:HeadObject"), do: "s3:GetObject"
  defp canonical_action("s3:HeadBucket"), do: "s3:ListBucket"
  defp canonical_action("s3:CreateMultipartUpload"), do: "s3:PutObject"
  defp canonical_action("s3:UploadPart"), do: "s3:PutObject"
  defp canonical_action("s3:CompleteMultipartUpload"), do: "s3:PutObject"
  defp canonical_action(action), do: action

  defp canonical_resource("arn:ess:::" <> suffix), do: "arn:aws:s3:::" <> suffix
  defp canonical_resource(resource), do: resource

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
