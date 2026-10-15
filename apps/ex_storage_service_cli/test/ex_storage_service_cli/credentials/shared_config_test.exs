defmodule ExStorageServiceCli.Credentials.SharedConfigTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Credentials.SharedConfig
  alias ExStorageServiceCli.Error
  @moduletag :tmp_dir

  test "profiles, quoted names and nested S3 service settings load without modifying files", %{
    tmp_dir: root
  } do
    config = Path.join(root, "config")
    credentials = Path.join(root, "credentials")

    content =
      "# preserved\n[default]\nregion = us-west-2\n[profile \"named profile\"]\ns3 =\n  addressing_style = virtual\n  signature_version = s3v4\n[services local]\ns3 =\n  endpoint_url = http://localhost:9100\n"

    File.write!(config, content)

    File.write!(
      credentials,
      "[named profile]\naws_access_key_id = access\naws_secret_access_key = secret\n"
    )

    assert {:ok, %{config: profiles, credentials: stored, sections: sections}} =
             SharedConfig.load(
               env: %{},
               home: root,
               config_file: config,
               credentials_file: credentials
             )

    assert profiles["named profile"]["s3"] == %{
             "addressing_style" => "virtual",
             "signature_version" => "s3v4"
           }

    assert stored["named profile"]["aws_access_key_id"] == "access"
    assert sections["services local"]["s3"]["endpoint_url"] == "http://localhost:9100"
    assert File.read!(config) == content
  end

  test "malformed and duplicate data fail without credential values in errors" do
    for input <- [
          "key=secret",
          "[broken",
          "[p]\nx=secret\nx=value",
          "[p]\nx=secret\n[p]\nx=value",
          "[p]\ns3=\n x=secret\n x=value",
          "[p]\ninvalid secret",
          <<0>>
        ] do
      assert {:error, %Error{code: :invalid_shared_config} = error} = SharedConfig.parse(input)
      refute inspect(error) =~ "secret"
    end
  end

  test "missing default paths are empty read-only stores", %{tmp_dir: root} do
    assert {:ok, %{config: %{}, credentials: %{}}} = SharedConfig.load(env: %{}, home: root)
    refute File.exists?(Path.join(root, ".aws"))
  end
end
