defmodule ExStorageServiceCli.Credentials.RecordTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Credentials.Record
  alias ExStorageServiceCli.Error

  test "one complete pair and optional token/expiry form a redacted record" do
    assert {:ok,
            %Record{
              access_key_id: "access",
              secret_access_key: "secret",
              session_token: "token",
              expires_at: ~U[2027-01-01 00:00:00Z]
            } = record} =
             Record.new(
               %{
                 access_key_id: "access",
                 secret_access_key: "secret",
                 session_token: "token",
                 expires_at: "2027-01-01T00:00:00Z"
               },
               %{provider: :static, sensitive: "private provenance"}
             )

    output = inspect(record)
    for value <- ["access", "secret", "token", "private provenance"], do: refute(output =~ value)
    assert output =~ "redacted"
  end

  test "partial, blank and malformed credential metadata never succeed" do
    for fields <- [
          %{access_key_id: "access"},
          %{secret_access_key: "secret"},
          %{access_key_id: "", secret_access_key: "secret"},
          %{access_key_id: "access", secret_access_key: "secret", session_token: ""},
          %{access_key_id: "access", secret_access_key: "secret", expires_at: "tomorrow"}
        ] do
      assert {:error, %Error{category: :credentials, code: :invalid_credentials, details: %{}}} =
               Record.new(fields, :static)
    end
  end
end
