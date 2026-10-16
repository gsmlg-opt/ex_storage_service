Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeRmBypassTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, Parser}
  @moduletag :tmp_dir
  @header "x-amz-bypass-governance-retention"

  defp target, do: %Target{kind: :remote, bucket: "bucket", key: "key"}
  defp invocation(options), do: %Invocation{frontend: :native, command: ["rm"], options: options}
  defp opts(root), do: [tmp_dir: root, max_attempts: 1, emit: fn _ -> :ok end]

  defp context(endpoint, conditional \\ true),
    do: %Context{
      endpoint: endpoint,
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret",
      capabilities: %{conditional_delete: conditional}
    }

  defp head(extra \\ []),
    do: {200, [{"ETag", "\"e\""}, {"Content-Length", "1"}] ++ extra, ""}

  defp listing(false) do
    "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>key</Key>" <>
      "<Size>1</Size><ETag>&quot;e&quot;</ETag></Contents></ListBucketResult>"
  end

  defp listing(true) do
    "<ListVersionsResult><IsTruncated>false</IsTruncated>" <>
      "<Version><Key>key</Key><VersionId>v +%</VersionId><Size>1</Size>" <>
      "<ETag>&quot;e&quot;</ETag></Version>" <>
      "<DeleteMarker><Key>key</Key><VersionId>marker</VersionId></DeleteMarker>" <>
      "</ListVersionsResult>"
  end

  defp deleted(false), do: "<DeleteResult><Deleted><Key>key</Key></Deleted></DeleteResult>"

  defp deleted(true),
    do:
      "<DeleteResult><Deleted><Key>key</Key><VersionId>v +%</VersionId></Deleted>" <>
        "<Deleted><Key>key</Key><VersionId>marker</VersionId></Deleted></DeleteResult>"

  defp bypass(headers) do
    assert headers[@header] == "true"
    assert headers["authorization"] =~ @header
  end

  test "bypass is an explicit native rm boolean", %{tmp_dir: root} do
    for {args, expected} <- [{["--bypass"], true}, {["--bypass=false"], false}] do
      assert {:ok, parsed} = Parser.parse(["rm", "s3://bucket/key"] ++ args)
      assert parsed.options[:bypass_governance_retention] == expected
    end

    for args <- [
          ["s3", "rm", "s3://bucket/key", "--bypass"],
          ["cp", "local", "s3://bucket/key", "--bypass"],
          ["rm", "s3://bucket/key", "--bypass=invalid"]
        ] do
      assert {:error, %Error{category: :syntax}} = Parser.parse(args)
    end

    refute_receive {:wire_request, _}
    assert File.ls!(root) == []
  end

  test "single current deletion signs the explicit bypass", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{204, [], ""}])
    assert {:ok, parsed} = Parser.parse(["rm", "s3://bucket/key", "--bypass"])
    assert :ok = HighLevel.run(parsed, [target()], [context(endpoint)], opts(root))
    assert_receive {:wire_request, %{method: "DELETE", target: "/bucket/key", headers: headers}}
    bypass(headers)
    refute_receive {:wire_request, _}
  end

  test "single selected version retains its exact identity with bypass", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{204, [], ""}])

    assert :ok =
             HighLevel.run(
               invocation(%{bypass_governance_retention: true, version_id: "v +%"}),
               [target()],
               [context(endpoint)],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "DELETE", target: path, headers: headers}}
    assert path == "/bucket/key?versionId=v%20%2B%25"
    bypass(headers)
    refute_receive {:wire_request, _}
  end

  test "false and default do not send a single-delete bypass header", %{tmp_dir: root} do
    for options <- [%{}, %{bypass_governance_retention: false}] do
      {endpoint, _} = WireServer.fixture([{204, [], ""}])
      assert :ok = HighLevel.run(invocation(options), [target()], [context(endpoint)], opts(root))
      assert_receive {:wire_request, %{method: "DELETE", headers: headers}}
      refute Map.has_key?(headers, @header)
      refute headers["authorization"] =~ @header
    end
  end

  test "age-selected deletion retains current conditions and selected versions with bypass", %{
    tmp_dir: root
  } do
    for version <- [nil, "v1"] do
      extra = [{"Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT"}]
      extra = if version, do: [{"x-amz-version-id", version} | extra], else: extra
      {endpoint, _} = WireServer.fixture([head(extra), {204, [], ""}])
      options = %{older_than: "1d", bypass_governance_retention: true}
      options = if version, do: Map.put(options, :version_id, version), else: options

      assert :ok =
               HighLevel.run(
                 invocation(options),
                 [target()],
                 [context(endpoint)],
                 Keyword.put(opts(root), :now, ~U[2026-01-03 00:00:00Z])
               )

      assert_receive {:wire_request, %{method: "HEAD", headers: head_headers}}
      refute Map.has_key?(head_headers, @header)
      assert_receive {:wire_request, %{method: "DELETE", target: path, headers: headers}}
      bypass(headers)

      if version do
        assert path == "/bucket/key?versionId=v1"
        refute Map.has_key?(headers, "if-match")
      else
        assert path == "/bucket/key"
        assert headers["if-match"] == "\"e\""
        assert headers["authorization"] =~ "if-match"
      end
    end
  end

  test "version batch signs bypass and preserves versions and delete markers", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], listing(true)}, {200, [], deleted(true)}])

    assert :ok =
             HighLevel.run(
               invocation(%{all_versions: true, bypass_governance_retention: true}),
               [target()],
               [context(endpoint, false)],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET", headers: listing_headers}}
    refute Map.has_key?(listing_headers, @header)
    assert_receive {:wire_request, %{method: "POST", body: body, headers: headers}}
    assert body =~ "<VersionId>v +%</VersionId>"
    assert body =~ "<VersionId>marker</VersionId>"
    bypass(headers)
    refute_receive {:wire_request, _}
  end

  test "recursive current batch retains conditional preflight with bypass", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([{200, [], listing(false)}, head(), {200, [], deleted(false)}])

    assert :ok =
             HighLevel.run(
               invocation(%{recursive: true, bypass_governance_retention: true}),
               [%{target() | key: ""}],
               [context(endpoint)],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET", headers: listing_headers}}
    refute Map.has_key?(listing_headers, @header)
    assert_receive {:wire_request, %{method: "HEAD", headers: head_headers}}
    refute Map.has_key?(head_headers, @header)
    assert_receive {:wire_request, %{method: "POST", body: body, headers: headers}}
    assert body =~ "<ETag>&quot;e&quot;</ETag>"
    refute body =~ "<VersionId>"
    bypass(headers)
    refute_receive {:wire_request, _}
  end

  test "false and default do not send a batch bypass header", %{tmp_dir: root} do
    for bypass <- [%{}, %{bypass_governance_retention: false}] do
      {endpoint, _} = WireServer.fixture([{200, [], listing(true)}, {200, [], deleted(true)}])
      options = Map.put(bypass, :all_versions, true)
      assert :ok = HighLevel.run(invocation(options), [target()], [context(endpoint)], opts(root))
      assert_receive {:wire_request, %{method: "GET"}}
      assert_receive {:wire_request, %{method: "POST", headers: headers}}
      refute Map.has_key?(headers, @header)
      refute headers["authorization"] =~ @header
    end
  end

  test "bypass does not authorize an unsafe current batch", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], listing(false)}])

    assert {:error, %Error{code: :conditional_delete_unavailable}} =
             HighLevel.run(
               invocation(%{recursive: true, bypass_governance_retention: true}),
               [%{target() | key: ""}],
               [context(endpoint, false)],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, _}
  end

  test "bypass does not skip selected-version identity checks", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([head([{"x-amz-version-id", "other"}])])

    assert {:error, %Error{code: :source_version_mismatch}} =
             HighLevel.run(
               invocation(%{
                 version_id: "v1",
                 older_than: "1d",
                 bypass_governance_retention: true
               }),
               [target()],
               [context(endpoint)],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "HEAD", headers: headers}}
    refute Map.has_key?(headers, @header)
    refute_receive {:wire_request, _}
  end

  test "a bypass batch preserves per-object failure reporting", %{tmp_dir: root} do
    failure =
      "<DeleteResult><Error><Key>key</Key><VersionId>v +%</VersionId>" <>
        "<Code>AccessDenied</Code><Message>Compliance retention</Message></Error></DeleteResult>"

    {endpoint, _} = WireServer.fixture([{200, [], listing(true)}, {200, [], failure}])

    assert {:error,
            %Error{code: :batch_delete_failed, details: %{failed: 1, codes: ["AccessDenied"]}}} =
             HighLevel.run(
               invocation(%{all_versions: true, bypass_governance_retention: true}),
               [target()],
               [context(endpoint)],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "POST", headers: headers}}
    bypass(headers)
    refute_receive {:wire_request, _}
  end

  test "bypass retains dry-run and conflicting-version no-I/O fences", %{tmp_dir: root} do
    assert :ok =
             HighLevel.run(
               invocation(%{dry_run: true, bypass_governance_retention: true}),
               [target()],
               [%Context{}],
               opts(root)
             )

    assert {:error, %Error{code: :version_selection}} =
             HighLevel.run(
               invocation(%{
                 all_versions: true,
                 version_id: "v1",
                 bypass_governance_retention: true
               }),
               [target()],
               [%Context{}],
               opts(root)
             )

    refute_receive {:wire_request, _}
    assert File.ls!(root) == []
  end
end
