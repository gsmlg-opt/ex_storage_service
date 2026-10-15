defmodule ExStorageServiceCli.Credentials.Record do
  @moduledoc "One immutable credential pair with optional temporary-session metadata."
  alias ExStorageServiceCli.Error
  defstruct [:access_key_id, :secret_access_key, :session_token, :expires_at, :provenance]

  def new(fields, provenance) do
    fields = Map.new(fields)
    access = field(fields, :access_key_id)
    secret = field(fields, :secret_access_key)
    token = field(fields, :session_token)

    with true <- nonempty?(access) and nonempty?(secret),
         true <- token == nil or nonempty?(token),
         {:ok, expires} <- expiration(field(fields, :expires_at)) do
      {:ok,
       %__MODULE__{
         access_key_id: access,
         secret_access_key: secret,
         session_token: token,
         expires_at: expires,
         provenance: provenance
       }}
    else
      _ ->
        {:error,
         Error.new(:credentials, "Credential provider returned incomplete or invalid credentials",
           code: :invalid_credentials
         )}
    end
  end

  defp field(fields, name), do: Map.get(fields, name, Map.get(fields, Atom.to_string(name)))

  defp nonempty?(value),
    do: is_binary(value) and String.valid?(value) and String.trim(value) != ""

  defp expiration(nil), do: {:ok, nil}
  defp expiration(%DateTime{} = value), do: {:ok, value}

  defp expiration(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _ -> :error
    end
  end

  defp expiration(_value), do: :error
end

defimpl Inspect, for: ExStorageServiceCli.Credentials.Record do
  def inspect(record, opts) do
    Inspect.Algebra.concat([
      "#Credentials.Record<redacted, expires_at: ",
      Inspect.Algebra.to_doc(record.expires_at, opts),
      ">"
    ])
  end
end
