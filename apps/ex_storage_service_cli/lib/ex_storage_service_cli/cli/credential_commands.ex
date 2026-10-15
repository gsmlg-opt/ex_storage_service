defmodule ExStorageServiceCli.CLI.CredentialCommands do
  @moduledoc "Explicit-profile IAM Identity Center login and scoped local session logout."
  alias ExStorageServiceCli.Credentials.Provider
  alias ExStorageServiceCli.Error

  def run(action, options, opts \\ []) do
    with profile when is_binary(profile) and profile != "" <- options[:profile] do
      execute(action, options, opts)
    else
      _ ->
        {:error,
         Error.new(:syntax, "SSO commands require an explicit --profile",
           code: :sso_profile_required
         )}
    end
  end

  defp execute("login", options, opts) do
    opts = Keyword.merge(opts, Map.to_list(Map.take(options, [:use_device_code, :no_browser])))

    with :ok <- Provider.login(options, opts) do
      success("login", options)
    end
  end

  defp execute("logout", options, opts) do
    with {:ok, _result} <- Provider.logout(options, opts) do
      success("logout", options)
    end
  end

  defp execute(_action, _options, _opts), do: {:error, Error.new(:syntax, "Unknown SSO command")}

  defp success(action, options) do
    cond do
      options[:quiet] == true or options[:output] == "off" ->
        :ok

      options[:json] == true or options[:output] == "json" ->
        IO.puts(JSON.encode!(%{status: "ok", operation: action}))

      action == "login" ->
        IO.puts("SSO login completed")

      true ->
        IO.puts("Selected SSO session cache cleared")
    end

    :ok
  end
end
