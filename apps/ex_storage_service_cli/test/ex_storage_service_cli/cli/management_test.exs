defmodule ExStorageServiceCli.CLI.ManagementTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.CLI.{Invocation, Management, Parser}
  alias ExStorageServiceCli.S3.{Encoder, OperationCatalog}
  @moduletag :tmp_dir

  test "all native document families bind to catalog operations and preserve complete input" do
    for {family, document, field, suffix, target} <- [
          {"cors", %{"CORSRules" => [%{"AllowedMethods" => ["GET"], "AllowedOrigins" => ["*"]}]},
           "CORSConfiguration", "BucketCors", "s3://bucket"},
          {"ilm",
           %{
             "Rules" => [
               %{
                 "ID" => "archive",
                 "Status" => "Enabled",
                 "Filter" => %{"Prefix" => "docs/"},
                 "Expiration" => %{"Days" => 30}
               }
             ]
           }, "LifecycleConfiguration", "BucketLifecycleConfiguration", "s3://bucket"},
          {"replicate",
           %{
             "Role" => "arn:aws:iam::123456789012:role/replicate",
             "Rules" => [
               %{
                 "ID" => "all",
                 "Status" => "Enabled",
                 "Prefix" => "",
                 "Destination" => %{"Bucket" => "arn:aws:s3:::backup"}
               }
             ]
           }, "ReplicationConfiguration", "BucketReplication", "s3://bucket"},
          {"event",
           %{
             "TopicConfigurations" => [
               %{
                 "Id" => "created",
                 "TopicArn" => "arn:aws:sns:us-east-1:123456789012:objects",
                 "Events" => ["s3:ObjectCreated:*"]
               }
             ]
           }, "NotificationConfiguration", "BucketNotificationConfiguration", "s3://bucket"},
          {"encrypt",
           %{
             "Rules" => [
               %{
                 "ApplyServerSideEncryptionByDefault" => %{"SSEAlgorithm" => "AES256"},
                 "BucketKeyEnabled" => true
               }
             ]
           }, "ServerSideEncryptionConfiguration", "BucketEncryption", "s3://bucket"},
          {"ownership", %{"Rules" => [%{"ObjectOwnership" => "BucketOwnerEnforced"}]},
           "OwnershipControls", "BucketOwnershipControls", "s3://bucket"},
          {"public-access",
           %{
             "BlockPublicAcls" => true,
             "IgnorePublicAcls" => true,
             "BlockPublicPolicy" => true,
             "RestrictPublicBuckets" => true
           }, "PublicAccessBlockConfiguration", "PublicAccessBlock", "s3://bucket"},
          {"lock",
           %{
             "ObjectLockEnabled" => "Enabled",
             "Rule" => %{"DefaultRetention" => %{"Mode" => "GOVERNANCE", "Days" => 1}}
           }, "ObjectLockConfiguration", "ObjectLockConfiguration", "s3://bucket"},
          {"retention", %{"Mode" => "GOVERNANCE", "RetainUntilDate" => "2030-01-01T00:00:00Z"},
           "Retention", "ObjectRetention", "s3://bucket/key"},
          {"legalhold", %{"Status" => "ON"}, "LegalHold", "ObjectLegalHold", "s3://bucket/key"},
          {"tag", %{"TagSet" => [%{"Key" => "owner", "Value" => "ops"}]}, "Tagging",
           "BucketTagging", "s3://bucket"},
          {"tag", %{"TagSet" => [%{"Key" => "owner", "Value" => "ops"}]}, "Tagging",
           "ObjectTagging", "s3://bucket/a//../b%+"},
          {"acl",
           %{
             "Owner" => %{"ID" => "owner"},
             "Grants" => [
               %{
                 "Grantee" => %{"Type" => "CanonicalUser", "ID" => "owner"},
                 "Permission" => "FULL_CONTROL"
               }
             ]
           }, "AccessControlPolicy", "ObjectAcl", "s3://bucket/key"}
        ] do
      owner = self()

      call = fn operation, input ->
        assert {:ok, _request} = Encoder.encode(operation, input)
        send(owner, {:operation, operation, input})
        {:ok, %{"ProviderExtra" => %{"kept" => true}}}
      end

      invocation = invocation([family, "set"], [target, JSON.encode!(document)])
      assert {:ok, %{"ProviderExtra" => %{"kept" => true}}} = run(invocation, target, call)
      operation = "Put" <> suffix
      assert_receive {:operation, ^operation, input}
      assert input[field] == document
      assert {:ok, _} = OperationCatalog.operation(operation)
    end
  end

  test "policy replacement is deliberate and preserves raw caller document", %{tmp_dir: directory} do
    document =
      "{\n  \"Version\": \"2012-10-17\", \"Statement\": [], \"ProviderExtension\": {\"x\": true}\n}"

    path = Path.join(directory, "policy.json")
    File.write!(path, document)
    owner = self()

    call = fn
      "GetBucketPolicy", _ ->
        {:ok, %{"Policy" => "existing"}}

      "PutBucketPolicy", input ->
        send(owner, {:written, input})
        {:ok, %{}}
    end

    invocation = invocation(["policy", "set"], ["s3://bucket", "file://" <> path])
    assert {:error, %Error{message: message}} = run(invocation, "s3://bucket", call)
    assert message =~ "--replace"
    refute_received {:written, _}
    assert {:ok, %{}} = run(%{invocation | options: %{replace: true}}, "s3://bucket", call)
    assert_received {:written, %{"Policy" => ^document}}
  end

  test "denied policy reads are not treated as absence" do
    denied = Error.new(:service, "Denied", code: "AccessDenied", http_status: 403)
    call = fn "GetBucketPolicy", _ -> {:error, denied} end
    invocation = invocation(["anonymous", "set"], ["download", "s3://bucket"])
    assert {:error, ^denied} = run(invocation, "s3://bucket", call)
  end

  test "anonymous dry run creates a complete partition-aware policy without writing" do
    call = fn "GetBucketPolicy", _ ->
      {:error, Error.new(:service, "Missing", code: "NoSuchBucketPolicy")}
    end

    invocation = invocation(["anonymous", "set"], ["download", "s3://bucket"], %{dry_run: true})
    {:ok, target} = Target.parse("s3://bucket")

    assert {:ok, %{"DryRun" => true, "Input" => %{"Policy" => policy}}} =
             Management.run(invocation, target, %Context{region: "cn-north-1"}, call: call)

    assert policy =~ "arn:aws-cn:s3:::bucket/*"
    refute policy =~ "s3:PutObject"
  end

  test "versioning preserves the CLI version command and uses explicit subcommands" do
    assert {:ok, %Invocation{command: ["version"]}} = Parser.parse(["version"])

    for {action, status} <- [{"enable", "Enabled"}, {"suspend", "Suspended"}] do
      call = fn "PutBucketVersioning", input ->
        assert input == %{
                 "Bucket" => "bucket",
                 "VersioningConfiguration" => %{"Status" => status}
               }

        {:ok, %{}}
      end

      assert {:ok, %{}} =
               run(invocation(["version", action], ["s3://bucket"]), "s3://bucket", call)
    end

    assert {:error, %Error{category: :syntax}} =
             Parser.parse(["version", "enable", "s3://bucket", "--version-id", "ignored"])
  end

  test "undo copies only the selected confirmed data version with exact copy-source encoding" do
    call = fn
      "HeadObject", input ->
        assert input["VersionId"] == "v/+%"
        {:ok, %{"ETag" => "\"selected\""}}

      "CopyObject", input ->
        assert input["CopySource"] == %{
                 "Bucket" => "bucket",
                 "Key" => "a//../b%+",
                 "VersionId" => "v/+%"
               }

        assert input["CopySourceIfMatch"] == "\"selected\""
        {:ok, %{"CopyObjectResult" => %{"ETag" => "\"restored\""}}}
    end

    invocation = invocation(["undo"], ["s3://bucket/a//../b%+"], %{version_id: "v/+%"})
    assert {:ok, _} = run(invocation, hd(invocation.args), call)
  end

  test "undo never deletes a data version passed as a delete-marker ID" do
    call = fn "ListObjectVersions", _ ->
      {:ok, %{"IsTruncated" => false, "Versions" => [%{"Key" => "key", "VersionId" => "data"}]}}
    end

    invocation = invocation(["undo"], ["s3://bucket/key"], %{delete_marker_id: "data"})
    assert {:error, %Error{category: :syntax}} = run(invocation, hd(invocation.args), call)
  end

  test "undo proves the selected delete marker across pages before deleting it" do
    call = fn
      "ListObjectVersions", %{"KeyMarker" => "key", "VersionIdMarker" => "v1"} ->
        {:ok,
         %{
           "IsTruncated" => false,
           "DeleteMarkers" => [%{"Key" => "key", "VersionId" => "marker"}]
         }}

      "ListObjectVersions", _ ->
        {:ok, %{"IsTruncated" => true, "NextKeyMarker" => "key", "NextVersionIdMarker" => "v1"}}

      "DeleteObject", input ->
        assert input == %{"Bucket" => "bucket", "Key" => "key", "VersionId" => "marker"}
        {:ok, %{"DeleteMarker" => true}}
    end

    invocation = invocation(["undo"], ["s3://bucket/key"], %{delete_marker_id: "marker"})
    assert {:ok, %{"DeleteMarker" => true}} = run(invocation, hd(invocation.args), call)
  end

  test "nonportable removals fail explicitly instead of inventing API operations" do
    for family <- ~w(retention lock acl) do
      target = if family == "lock", do: "s3://bucket", else: "s3://bucket/key"
      call = fn _, _ -> flunk("Unexpected network request") end

      assert {:error, %Error{category: :syntax}} =
               run(invocation([family, "remove"], [target]), target, call)
    end
  end

  test "every supported family removal selects an existing API operation" do
    for {family, expected} <- [
          {"policy", "DeleteBucketPolicy"},
          {"cors", "DeleteBucketCors"},
          {"ilm", "DeleteBucketLifecycle"},
          {"replicate", "DeleteBucketReplication"},
          {"event", "PutBucketNotificationConfiguration"},
          {"encrypt", "DeleteBucketEncryption"},
          {"ownership", "DeleteBucketOwnershipControls"},
          {"public-access", "DeletePublicAccessBlock"},
          {"tag", "DeleteBucketTagging"},
          {"legalhold", "PutObjectLegalHold"}
        ] do
      target = if family == "legalhold", do: "s3://bucket/key", else: "s3://bucket"

      call = fn operation, input ->
        assert operation == expected
        assert {:ok, _} = Encoder.encode(operation, input)
        {:ok, %{}}
      end

      options = if family == "replicate", do: %{all: true, force: true}, else: %{}
      assert {:ok, %{}} = run(invocation([family, "remove"], [target], options), target, call)
    end
  end

  test "dry run validates the full document and rejects oversized or malformed JSON" do
    call = fn _, _ -> flunk("Unexpected network request") end

    for document <- ["[]", "not-json", JSON.encode!(%{"Unknown" => true})] do
      assert {:error, %Error{}} =
               run(
                 invocation(["cors", "set"], ["s3://bucket", document], %{dry_run: true}),
                 "s3://bucket",
                 call
               )
    end
  end

  defp invocation(command, args, options \\ %{}),
    do: %Invocation{command: command, args: args, options: options}

  defp run(invocation, target, call) do
    {:ok, target} = Target.parse(target)
    Management.run(invocation, target, %Context{}, call: call)
  end
end
