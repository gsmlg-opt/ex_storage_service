defmodule ExStorageService.Storage.BucketCORS do
  @moduledoc "Persisted S3 CORS rules and bounded origin/method/header matching."
  alias ExStorageService.Metadata.BucketConfig

  @methods ~w(GET PUT POST DELETE HEAD)

  def get(bucket), do: Concord.get("bucket_cors:#{bucket}", consistency: :linearizable)

  def put(bucket, rules, opts \\ []) do
    with {:ok, validated} <- validate(rules),
         do: BucketConfig.put(bucket, "bucket_cors", validated, opts)
  end

  def delete(bucket, opts \\ []), do: BucketConfig.delete(bucket, "bucket_cors", opts)

  def validate(rules) when is_list(rules) and length(rules) in 1..100 do
    Enum.reduce_while(rules, {:ok, []}, fn rule, {:ok, result} ->
      case validate_rule(rule) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | result]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, result} -> {:ok, Enum.reverse(result)}
      error -> error
    end
  end

  def validate(_), do: {:error, :invalid_cors}

  def match(rules, origin, method, headers \\ []) do
    case Enum.find(rules, fn rule ->
           method in rule.allowed_methods and
             Enum.any?(rule.allowed_origins, &glob?(&1, origin)) and
             Enum.all?(headers, fn header ->
               Enum.any?(rule.allowed_headers, &glob?(&1, String.downcase(header)))
             end)
         end) do
      nil -> :no_match
      rule -> {:ok, rule}
    end
  end

  defp validate_rule(rule) when is_map(rule) do
    origins = Map.get(rule, :allowed_origins, [])
    methods = Map.get(rule, :allowed_methods, [])
    headers = Map.get(rule, :allowed_headers, [])
    exposed = Map.get(rule, :expose_headers, [])
    max_age = Map.get(rule, :max_age_seconds)
    id = Map.get(rule, :id, "")

    if valid_list?(origins, &origin?/1) and origins != [] and
         valid_list?(methods, &(&1 in @methods)) and methods != [] and
         valid_list?(headers, &header?(&1, true)) and
         valid_list?(exposed, &header?(&1, false)) and
         (is_nil(max_age) or (is_integer(max_age) and max_age in 0..2_147_483_647)) and
         is_binary(id) and String.valid?(id) and String.length(id) <= 255 do
      {:ok,
       %{
         id: id,
         allowed_origins: origins,
         allowed_methods: methods,
         allowed_headers: Enum.map(headers, &String.downcase/1),
         expose_headers: Enum.map(exposed, &String.downcase/1),
         max_age_seconds: max_age
       }}
    else
      {:error, :invalid_cors}
    end
  end

  defp validate_rule(_), do: {:error, :invalid_cors}
  defp valid_list?(list, predicate), do: is_list(list) and Enum.all?(list, predicate)

  defp origin?(value) when value in ["*", "null"], do: true

  defp origin?(value) when is_binary(value) do
    String.valid?(value) and byte_size(value) <= 2048 and
      length(String.split(value, "*")) <= 2 and
      Regex.match?(~r/\Ahttps?:\/\/[A-Za-z0-9.*\-\[\]:]+\z/, value)
  end

  defp origin?(_), do: false

  defp header?(value, wildcard?) when is_binary(value) do
    String.valid?(value) and byte_size(value) in 1..256 and
      Regex.match?(~r/\A[!#$%&'*+.^_`|~0-9A-Za-z-]+\z/, value) and
      if(wildcard?,
        do: length(String.split(value, "*")) <= 2,
        else: not String.contains?(value, "*")
      )
  end

  defp header?(_, _), do: false

  defp glob?(pattern, value) do
    regex = "\\A" <> String.replace(Regex.escape(pattern), "\\*", ".*") <> "\\z"
    Regex.match?(Regex.compile!(regex), value)
  end
end
