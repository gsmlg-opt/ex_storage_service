defmodule ExStorageServiceCli.CLI.S3PathResolverTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.CLI.S3PathResolver
  @invocation %{frontend: :s3, command: ["mv"]}
  @account %{"AccountId" => "123456789012"}

  defp target(bucket, key \\ "key"),
    do: %Target{kind: :remote, bucket: bucket, key: key, original: "s3://#{bucket}/#{key}"}

  defp settings(env \\ %{}, profile \\ %{}),
    do: %{env: env, stores: %{config: %{"default" => profile}, sections: %{}}}

  test "control and STS endpoint rules resolve regional, FIPS and dualstack endpoints" do
    context = %Context{region: "us-west-2"}

    for {env, host} <- [
          {%{}, "123456789012.s3-control.us-west-2.amazonaws.com"},
          {%{"AWS_USE_FIPS_ENDPOINT" => "true"},
           "123456789012.s3-control-fips.us-west-2.amazonaws.com"},
          {%{"AWS_USE_DUALSTACK_ENDPOINT" => "true"},
           "123456789012.s3-control.dualstack.us-west-2.amazonaws.com"}
        ] do
      assert {:ok, endpoint} = S3PathResolver.endpoint(:control, @account, context, settings(env))
      assert URI.parse(endpoint.url).host == host
    end

    assert {:ok, endpoint} = S3PathResolver.endpoint(:sts, %{}, context, settings())
    assert endpoint.url == "https://sts.us-west-2.amazonaws.com"
  end

  test "control endpoint is independent of data endpoint and custom endpoints suppress FIPS" do
    context = %Context{endpoint: "http://data.invalid", region: "us-east-1"}

    env = %{
      "AWS_ENDPOINT_URL_S3_CONTROL" => "http://control.invalid/base",
      "AWS_USE_FIPS_ENDPOINT" => "true",
      "AWS_USE_DUALSTACK_ENDPOINT" => "true"
    }

    assert {:ok, endpoint} = S3PathResolver.endpoint(:control, @account, context, settings(env))
    assert endpoint.url == "http://123456789012.control.invalid/base"
    env = Map.put(env, "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "true")
    assert {:ok, endpoint} = S3PathResolver.endpoint(:control, @account, context, settings(env))

    assert URI.parse(endpoint.url).host ==
             "123456789012.s3-control-fips.dualstack.us-east-1.amazonaws.com"
  end

  test "Outposts ARN region mismatch is rejected unless UseArnRegion is enabled" do
    input =
      Map.put(
        @account,
        "AccessPointName",
        "arn:aws:s3-outposts:us-west-2:123456789012:outpost/op-123/accesspoint/example-ap"
      )

    assert {:error, %Error{category: :client}} =
             S3PathResolver.endpoint(:control, input, %Context{}, settings())

    assert {:ok, endpoint} =
             S3PathResolver.endpoint(
               :control,
               input,
               %Context{},
               settings(%{"AWS_S3_USE_ARN_REGION" => "true"})
             )

    assert URI.parse(endpoint.url).host == "s3-outposts.us-west-2.amazonaws.com"
    assert hd(endpoint.properties["authSchemes"])["signingName"] == "s3-outposts"
  end

  test "native commands, different keys and ordinary equal buckets do not resolve" do
    targets = [target("bucket"), target("example-ap-s3alias")]

    assert :ok =
             S3PathResolver.validate(%{@invocation | frontend: :native}, targets, [], %{
               validate_same_s3_paths: true
             })

    assert :ok =
             S3PathResolver.validate(
               @invocation,
               [target("bucket"), target("example-ap-s3alias", "different")],
               [%Context{}, %Context{}],
               %{validate_same_s3_paths: true}
             )

    assert :ok =
             S3PathResolver.validate(
               @invocation,
               [target("bucket"), target("bucket")],
               [%Context{}, %Context{}],
               %{validate_same_s3_paths: true}
             )
  end

  test "disabled resolution warns only for effective matching keys" do
    owner = self()

    opts = [
      env: %{"AWS_CLI_S3_MV_VALIDATE_SAME_S3_PATHS" => " true "},
      stderr: &send(owner, {:warning, &1})
    ]

    assert :ok =
             S3PathResolver.validate(
               @invocation,
               [target("bucket", "dir/key"), target("example-ap-s3alias", "dir/")],
               [%Context{}, %Context{}],
               %{},
               opts
             )

    assert_receive {:warning, warning}
    assert warning =~ "--validate-same-s3-paths"
  end
end
