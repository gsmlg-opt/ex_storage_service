defmodule ExStorageServiceCli.Config do
  @moduledoc """
  Manages CLI configuration profiles stored in `~/.config/ess/config.toml`.

  Supports multiple named profiles with credentials and endpoint settings.

  ## Config File Format

      [default]
      endpoint = "http://localhost:9000"
      access_key_id = "AKIA..."
      secret_access_key = "..."
      region = "us-east-1"

      [profiles.production]
      endpoint = "https://s3.example.com"
      access_key_id = "AKIA..."
      secret_access_key = "..."
      region = "us-east-1"
  """

  @config_dir "~/.config/ess"
  @config_file "config.toml"

  @doc """
  Returns the path to the config directory.
  """
  def config_dir do
    Path.expand(@config_dir)
  end

  @doc """
  Returns the path to the config file.
  """
  def config_path do
    Path.join(config_dir(), @config_file)
  end

  @doc """
  Loads a named profile from the config file.

  Returns a map with `:endpoint`, `:access_key_id`, `:secret_access_key`, `:region`.
  Returns an empty map if the profile or config file doesn't exist.
  """
  @spec load_profile(String.t()) :: map()
  def load_profile(profile_name) do
    path = config_path()

    if File.exists?(path) do
      case File.read(path) do
        {:ok, content} ->
          case Toml.decode(content) do
            {:ok, config} ->
              extract_profile(config, profile_name)

            {:error, _reason} ->
              %{}
          end

        {:error, _} ->
          %{}
      end
    else
      %{}
    end
  end

  @doc """
  Saves a profile to the config file.

  Creates the config directory and file if they don't exist.
  Merges with existing config if the file already exists.
  """
  @spec save_profile(String.t(), map()) :: :ok | {:error, term()}
  def save_profile(profile_name, profile_data) do
    dir = config_dir()
    path = config_path()

    File.mkdir_p!(dir)

    existing =
      if File.exists?(path) do
        case File.read(path) do
          {:ok, content} ->
            case Toml.decode(content) do
              {:ok, config} -> config
              _ -> %{}
            end

          _ ->
            %{}
        end
      else
        %{}
      end

    updated = put_profile(existing, profile_name, profile_data)
    content = encode_toml(updated)

    File.write(path, content)
  end

  @doc """
  Lists all available profile names.
  """
  @spec list_profiles() :: [String.t()]
  def list_profiles do
    path = config_path()

    if File.exists?(path) do
      case File.read(path) do
        {:ok, content} ->
          case Toml.decode(content) do
            {:ok, config} ->
              profiles =
                case Map.get(config, "profiles") do
                  nil -> []
                  profiles_map -> Map.keys(profiles_map)
                end

              if Map.has_key?(config, "default"), do: ["default" | profiles], else: profiles

            _ ->
              []
          end

        _ ->
          []
      end
    else
      []
    end
  end

  # Private helpers

  defp extract_profile(config, "default") do
    case Map.get(config, "default") do
      nil -> %{}
      profile -> normalize_profile(profile)
    end
  end

  defp extract_profile(config, profile_name) do
    case get_in(config, ["profiles", profile_name]) do
      nil -> %{}
      profile -> normalize_profile(profile)
    end
  end

  defp normalize_profile(profile) when is_map(profile) do
    %{
      endpoint: Map.get(profile, "endpoint"),
      access_key_id: Map.get(profile, "access_key_id"),
      secret_access_key: Map.get(profile, "secret_access_key"),
      region: Map.get(profile, "region")
    }
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new()
  end

  defp put_profile(config, "default", data) do
    Map.put(config, "default", serialize_profile(data))
  end

  defp put_profile(config, profile_name, data) do
    profiles = Map.get(config, "profiles", %{})
    updated_profiles = Map.put(profiles, profile_name, serialize_profile(data))
    Map.put(config, "profiles", updated_profiles)
  end

  defp serialize_profile(data) do
    data
    |> Enum.map(fn {k, v} -> {to_string(k), v} end)
    |> Map.new()
  end

  defp encode_toml(config) do
    parts = []

    # Encode [default] section
    parts =
      case Map.get(config, "default") do
        nil ->
          parts

        default ->
          section = encode_toml_section("default", default)
          parts ++ [section]
      end

    # Encode [profiles.*] sections
    parts =
      case Map.get(config, "profiles") do
        nil ->
          parts

        profiles ->
          profile_sections =
            profiles
            |> Enum.sort_by(fn {name, _} -> name end)
            |> Enum.map(fn {name, data} ->
              encode_toml_section("profiles.#{name}", data)
            end)

          parts ++ profile_sections
      end

    Enum.join(parts, "\n")
  end

  defp encode_toml_section(header, data) when is_map(data) do
    lines =
      data
      |> Enum.sort_by(fn {k, _} -> k end)
      |> Enum.map(fn {key, value} ->
        "#{key} = #{encode_toml_value(value)}"
      end)

    "[#{header}]\n#{Enum.join(lines, "\n")}\n"
  end

  defp encode_toml_value(value) when is_binary(value), do: ~s("#{value}")
  defp encode_toml_value(value) when is_integer(value), do: to_string(value)
  defp encode_toml_value(value) when is_float(value), do: to_string(value)
  defp encode_toml_value(true), do: "true"
  defp encode_toml_value(false), do: "false"
  defp encode_toml_value(value), do: ~s("#{inspect(value)}")
end
