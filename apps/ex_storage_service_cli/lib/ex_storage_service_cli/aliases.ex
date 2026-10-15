defmodule ExStorageServiceCli.Aliases do
  @moduledoc """
  Named endpoint/provider records stored alongside legacy profiles.

  Records use binary keys. Providers are `ess-profile`, `aws-profile`, `static`,
  `anonymous`, or `environment`; profile references default to the ESS default
  profile. Resolution uses `get/2`, while `list/1` redacts secret keys/tokens.

  Export/import exchange JSON documents with `version: 1` and an `aliases` map.
  Export requires `include_secrets: true` to include secrets; import requires
  `allow_secrets: true` to accept them. A redacted static record cannot restore
  usable credentials. Profile-reference records round-trip without secrets.
  Import replaces existing aliases only with `overwrite: true`; `preview_import/2`
  validates and reports proposed actions without writing. All updates are atomic.
  """
  alias ExStorageServiceCli.{Config, Error}

  @fields ~w(endpoint profile provider region access_key_id secret_access_key session_token service service_version bucket_type)
  @secrets ~w(secret_access_key session_token)

  def set(name, record, opts \\ []) do
    with {:ok, record} <- validate(name, record) do
      Config.update(opts, fn data ->
        with {:ok, aliases} <- records(data),
             do: {:ok, Map.put(data, "aliases", Map.put(aliases, name, record))}
      end)
    end
  end

  def get(name, opts \\ []) do
    with {:ok, data} <- Config.read(opts), do: from_config(data, name)
  end

  @doc false
  def from_config(data, name) do
    with {:ok, aliases} <- records(data) do
      case Map.fetch(aliases, name) do
        {:ok, record} -> validate(name, record)
        :error -> failure("Requested alias does not exist", :alias_missing)
      end
    end
  end

  @doc false
  def names(data) do
    with {:ok, aliases} <- records(data), do: {:ok, Map.keys(aliases)}
  end

  def list(opts \\ []) do
    with {:ok, data} <- Config.read(opts),
         {:ok, aliases} <- records(data),
         do: {:ok, Map.new(aliases, fn {name, record} -> {name, redact(record)} end)}
  end

  def remove(name, opts \\ []) do
    Config.update(opts, fn data ->
      with {:ok, aliases} <- records(data) do
        if Map.has_key?(aliases, name),
          do: {:ok, Map.put(data, "aliases", Map.delete(aliases, name))},
          else: failure("Requested alias does not exist", :alias_missing)
      end
    end)
  end

  def export(opts \\ []) do
    with {:ok, data} <- Config.read(opts), {:ok, aliases} <- records(data) do
      aliases =
        if opts[:include_secrets],
          do: aliases,
          else: Map.new(aliases, fn {name, record} -> {name, redact(record)} end)

      {:ok, JSON.encode!(%{"version" => 1, "aliases" => aliases})}
    end
  end

  def preview_import(document, opts \\ []) do
    with {:ok, imported} <- decode_import(document, opts),
         {:ok, data} <- Config.read(opts),
         {:ok, existing} <- records(data),
         :ok <- allow_overwrite(existing, imported, opts) do
      {:ok,
       Enum.map(Enum.sort(Map.keys(imported)), fn name ->
         %{name: name, action: if(Map.has_key?(existing, name), do: :replace, else: :create)}
       end)}
    end
  end

  def import(document, opts \\ []) do
    with {:ok, imported} <- decode_import(document, opts) do
      Config.update(opts, fn data ->
        with {:ok, existing} <- records(data),
             :ok <- allow_overwrite(existing, imported, opts),
             do: {:ok, Map.put(data, "aliases", Map.merge(existing, imported))}
      end)
    end
  end

  defp decode_import(document, opts)
       when is_binary(document) and byte_size(document) <= 1_048_576 do
    case JSON.decode(document) do
      {:ok, %{"version" => 1, "aliases" => aliases}} when is_map(aliases) ->
        Enum.reduce_while(aliases, {:ok, %{}}, fn {name, record}, {:ok, result} ->
          if is_map(record) and opts[:allow_secrets] != true and
               Enum.any?(@secrets, &Map.has_key?(record, &1)) do
            {:halt,
             failure(
               "Import contains secrets; explicit allow-secrets is required",
               :secrets_rejected
             )}
          else
            case validate(name, record) do
              {:ok, valid} -> {:cont, {:ok, Map.put(result, name, valid)}}
              {:error, error} -> {:halt, {:error, error}}
            end
          end
        end)

      _ ->
        failure("Alias import must be a version 1 alias document", :invalid_import)
    end
  end

  defp decode_import(_document, _opts),
    do: failure("Alias import exceeds its size limit or is not text", :invalid_import)

  defp records(data) do
    case Map.get(data, "aliases", %{}) do
      aliases when is_map(aliases) ->
        if Enum.all?(aliases, fn {name, record} -> match?({:ok, _}, validate(name, record)) end),
          do: {:ok, aliases},
          else: failure("Configuration contains an invalid alias", :invalid_alias)

      _ ->
        failure("Configuration aliases must be a table", :invalid_alias)
    end
  end

  defp validate(name, record) when is_binary(name) and is_map(record) do
    endpoint = Map.get(record, "endpoint", "")
    uri = URI.parse(if is_binary(endpoint), do: endpoint, else: "")

    credentials? =
      Enum.any?(~w(access_key_id secret_access_key session_token), &Map.has_key?(record, &1))

    pair? = nonempty?(record["access_key_id"]) and nonempty?(record["secret_access_key"])

    provider =
      record["provider"] ||
        if(record["profile"],
          do: "ess-profile",
          else: if(credentials?, do: "static", else: "ess-profile")
        )

    record = record |> Map.put("provider", provider) |> maybe_default_profile(provider)

    cond do
      not Regex.match?(~r/^[A-Za-z][A-Za-z0-9_.-]*$/, name) ->
        failure("Alias name is invalid", :invalid_alias)

      not Enum.all?(record, fn {key, value} -> key in @fields and is_binary(value) end) ->
        failure("Alias contains invalid fields", :invalid_alias)

      uri.scheme not in ["http", "https"] or not nonempty?(uri.host) or uri.userinfo != nil or
        uri.query != nil or uri.fragment != nil ->
        failure(
          "Alias endpoint must be an HTTP(S) URL without credentials, query, or fragment",
          :invalid_alias
        )

      provider not in ["ess-profile", "aws-profile", "static", "anonymous", "environment"] ->
        failure("Alias credential provider is invalid", :invalid_alias)

      record["service"] not in [nil, "ess", "aws", "s3"] or
          record["bucket_type"] not in [nil, "general", "directory"] ->
        failure("Alias service or bucket type is invalid", :invalid_alias)

      credentials? and (not pair? or provider != "static" or Map.has_key?(record, "profile")) ->
        failure(
          "Alias static credentials must be a complete pair without a profile",
          :invalid_alias
        )

      provider == "static" and not pair? ->
        failure("Alias static credentials are incomplete", :invalid_alias)

      provider in ["ess-profile", "aws-profile"] and not nonempty?(record["profile"]) ->
        failure("Alias credential profile is missing", :invalid_alias)

      true ->
        {:ok, record}
    end
  end

  defp validate(_name, _record), do: failure("Alias record is invalid", :invalid_alias)
  defp maybe_default_profile(record, "ess-profile"), do: Map.put_new(record, "profile", "default")
  defp maybe_default_profile(record, _provider), do: record
  defp nonempty?(value), do: is_binary(value) and value != ""
  defp redact(record), do: Map.drop(record, @secrets)

  defp allow_overwrite(existing, imported, opts) do
    if opts[:overwrite] != true and Enum.any?(Map.keys(imported), &Map.has_key?(existing, &1)),
      do: failure("Alias already exists; explicit overwrite is required", :alias_exists),
      else: :ok
  end

  defp failure(message, code), do: {:error, Error.new(:config, message, code: code)}
end
