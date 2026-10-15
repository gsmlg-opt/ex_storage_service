defmodule ExStorageServiceCli.Config do
  @moduledoc """
  Manages legacy `[default]` and `[profiles.<name>]` profiles and aliases.
  `ESS_CONFIG_DIR` overrides `XDG_CONFIG_HOME/ess` and `~/.config/ess`.
  Updates preserve unrelated TOML data; new callers use `read_profile/2` for errors.
  """
  alias ExStorageServiceCli.{Config.Store, Error}
  @profile_fields [:endpoint, :access_key_id, :secret_access_key, :session_token, :region]

  @doc "Returns the path to the config directory."
  def config_dir do
    case System.get_env("ESS_CONFIG_DIR") do
      directory when is_binary(directory) and directory != "" ->
        Path.expand(directory)

      _ ->
        case System.get_env("XDG_CONFIG_HOME") do
          directory when is_binary(directory) and directory != "" ->
            Path.join(Path.expand(directory), "ess")

          _ ->
            Path.expand("~/.config/ess")
        end
    end
  end

  @doc "Returns the path to the config file."
  def config_path, do: Path.join(config_dir(), "config.toml")

  @doc "Reads complete configuration with binary keys and explicit errors."
  def read(opts \\ []), do: Store.read(path(opts))

  @doc "Loads a profile, returning typed errors for corrupt or missing named profiles."
  def read_profile(name, opts \\ []) do
    with {:ok, data} <- read(opts), do: profile(data, name)
  end

  @doc false
  def profile(data, name) do
    profiles = Map.get(data, "profiles", %{})

    profile =
      if name == "default",
        do: data["default"],
        else: if(is_map(profiles), do: profiles[name], else: :invalid)

    case profile do
      nil when name == "default" ->
        {:ok, %{}}

      nil ->
        {:error,
         Error.new(:config, "Requested configuration profile does not exist",
           code: :profile_missing
         )}

      profile when is_map(profile) ->
        fields = Enum.map(@profile_fields, &Atom.to_string/1)

        if Enum.all?(profile, fn {key, value} -> key not in fields or is_binary(value) end) do
          {:ok,
           Map.new(
             for field <- @profile_fields,
                 value = profile[Atom.to_string(field)],
                 not is_nil(value),
                 do: {field, value}
           )}
        else
          invalid_profile()
        end

      _ ->
        invalid_profile()
    end
  end

  @doc """
  Loads a named legacy profile as a map. Missing/read-error profiles retain the
  legacy empty-map return; new execution paths must use `read_profile/2` instead.
  """
  @spec load_profile(String.t()) :: map()
  def load_profile(name) do
    case read_profile(name) do
      {:ok, profile} -> profile
      {:error, _error} -> %{}
    end
  end

  @doc "Saves a profile atomically with private permissions, retaining unrelated data."
  @spec save_profile(String.t(), map()) :: :ok | {:error, term()}
  def save_profile(name, profile, opts \\ []) when is_binary(name) and is_map(profile) do
    serialized = Map.new(profile, fn {key, value} -> {to_string(key), value} end)

    if name != "" and Enum.all?(serialized, fn {_key, value} -> is_binary(value) end) do
      update(opts, fn data ->
        if name == "default" do
          existing = Map.get(data, "default", %{})

          if is_map(existing),
            do: {:ok, Map.put(data, "default", Map.merge(existing, serialized))},
            else: invalid_profile()
        else
          profiles = Map.get(data, "profiles", %{})

          if is_map(profiles) and is_map(Map.get(profiles, name, %{})) do
            record = Map.merge(Map.get(profiles, name, %{}), serialized)
            {:ok, Map.put(data, "profiles", Map.put(profiles, name, record))}
          else
            invalid_profile()
          end
        end
      end)
    else
      invalid_profile()
    end
  end

  @doc "Lists all available profile names."
  @spec list_profiles() :: [String.t()]
  def list_profiles do
    case read() do
      {:ok, data} ->
        profiles = Map.get(data, "profiles", %{})
        names = if is_map(profiles), do: Enum.sort(Map.keys(profiles)), else: []
        if Map.has_key?(data, "default"), do: ["default" | names], else: names

      {:error, _error} ->
        []
    end
  end

  @doc false
  def update(opts, function), do: Store.update(path(opts), function)

  @doc "Creates an explicit private backup before a caller-requested migration."
  def backup(opts \\ []), do: Store.backup(path(opts))

  defp path(opts), do: Keyword.get(opts, :path, config_path())
  defp invalid_profile, do: {:error, Error.new(:config, "Invalid configuration profile")}
end
