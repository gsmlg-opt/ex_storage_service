defmodule ExStorageServiceCli.Credentials.LegacyTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Credentials.{Provider, Record}
  alias ExStorageServiceCli.Error
  @moduletag :tmp_dir

  test "legacy EC2 credentials precede Boto and container without token mixing", %{tmp_dir: root} do
    ec2 = Path.join(root, "ec2")
    boto = Path.join(root, "boto")
    File.write!(ec2, "AWSAccessKeyId=ec2-a\nAWSSecretKey=ec2-s\n")

    File.write!(
      boto,
      "[Credentials]\naws_access_key_id=boto-a\naws_secret_access_key=boto-s\naws_session_token=ignored\n"
    )

    env = %{
      "AWS_CREDENTIAL_FILE" => ec2,
      "BOTO_CONFIG" => boto,
      "AWS_CONTAINER_CREDENTIALS_FULL_URI" => "http://example.test/not-allowed"
    }

    assert {:ok,
            %Record{
              access_key_id: "ec2-a",
              session_token: nil,
              provenance: %{provider: :ec2_credentials_file}
            }, _} = Provider.resolve(%{}, env: env, home: root)

    assert {:ok,
            %Record{access_key_id: "boto-a", session_token: nil, provenance: %{provider: :boto}},
            _} = Provider.resolve(%{}, env: Map.delete(env, "AWS_CREDENTIAL_FILE"), home: root)
  end

  test "partial selected legacy pair fails instead of falling through", %{tmp_dir: root} do
    ec2 = Path.join(root, "ec2")
    File.write!(ec2, "AWSAccessKeyId=partial-marker\n")

    assert {:error, %Error{code: :invalid_credentials} = error} =
             Provider.resolve(%{}, env: %{"AWS_CREDENTIAL_FILE" => ec2}, home: root)

    refute inspect(error) =~ "partial-marker"
  end
end
