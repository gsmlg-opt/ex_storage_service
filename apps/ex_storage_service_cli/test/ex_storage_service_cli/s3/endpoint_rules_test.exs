defmodule ExStorageServiceCli.S3.EndpointRulesTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.S3.EndpointRules
  alias ExStorageServiceCli.S3.EndpointRules.Evaluator
  alias ExStorageServiceCli.Context
  @path Path.expand("../../fixtures/aws_s3_endpoint_tests_2_31_18.json", __DIR__)
  @external_resource @path
  @cases @path |> File.read!() |> JSON.decode!() |> Map.fetch!("testCases")

  test "model-bound directory operations select session and control auth independently" do
    context = %Context{frontend: :aws, region: "us-west-2", addressing: :virtual}
    input = %{"Bucket" => "mybucket--usw2-az1--x-s3", "Key" => "key"}

    assert {:ok,
            %{
              endpoint:
                "https://mybucket--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com",
              service: "s3express",
              session_auth: :s3express,
              strip_bucket: true
            }} = EndpointRules.resolve(context, "GetObject", input)

    assert {:ok, %{session_auth: nil, service: "s3express"}} =
             EndpointRules.resolve(
               context,
               "CopyObject",
               Map.put(input, "CopySource", "/source/key")
             )

    assert {:ok, %{session_auth: nil, service: "s3express"}} =
             EndpointRules.resolve(context, "CreateSession", input)

    assert {:ok,
            %{
              endpoint:
                "https://s3express-control.us-west-2.amazonaws.com/mybucket--usw2-az1--x-s3",
              session_auth: nil
            }} = EndpointRules.resolve(context, "GetBucketPolicy", input)
  end

  test "access point and MRAP contracts preserve modeled signing service region and scheme" do
    context = %Context{frontend: :aws, region: "us-east-1", addressing: :virtual}

    assert {:ok,
            %{
              endpoint: "https://myendpoint-123456789012.s3-accesspoint.us-west-2.amazonaws.com",
              service: "s3",
              region: "us-west-2",
              strip_bucket: true
            }} =
             EndpointRules.resolve(
               context,
               "GetObject",
               %{"Bucket" => "arn:aws:s3:us-west-2:123456789012:accesspoint:myendpoint"},
               s3_settings: %{"use_arn_region" => "true"}
             )

    assert {:ok,
            %{
              auth: :sigv4a,
              signing_region_set: ["*"],
              endpoint: "https://mrap.mrap.accesspoint.s3-global.amazonaws.com"
            }} =
             EndpointRules.resolve(context, "GetObject", %{
               "Bucket" => "arn:aws:s3::123456789012:accesspoint:mrap.mrap"
             })
  end

  test "native custom endpoint and base path include bucket only once" do
    context = %Context{endpoint: "http://localhost:9000/base", addressing: :path}

    assert {:ok,
            %{
              endpoint: "http://localhost:9000/base/bucket",
              addressing: :path,
              strip_bucket: true
            }} =
             EndpointRules.resolve(context, "GetObject", %{
               "Bucket" => "bucket",
               "Key" => "a//../key"
             })

    assert {:error, %Error{code: :invalid_endpoint_parameter}} =
             EndpointRules.resolve(context, "GetObject", %{},
               s3_settings: %{"use_accelerate_endpoint" => "maybe"}
             )
  end

  for {reference, index} <- Enum.with_index(@cases) do
    @reference reference
    test "pinned AWS endpoint #{index}: #{reference["documentation"]}" do
      case @reference["expect"] do
        %{"error" => message} ->
          assert {:error, %Error{message: ^message}} = Evaluator.evaluate(@reference["params"])

        %{"endpoint" => expected} ->
          assert {:ok, result} = Evaluator.evaluate(@reference["params"])
          assert result.url == expected["url"]
          assert result.properties == expected["properties"] || %{}
          assert result.headers == expected["headers"] || %{}
      end
    end
  end

  for {reference, index} <- Enum.with_index(@cases),
      {operation, operation_index} <- Enum.with_index(reference["operationInputs"] || []) do
    @operation operation
    @reference reference
    test "pinned operation binding #{index}.#{operation_index}: #{operation["operationName"]}" do
      client =
        Map.drop(
          @reference["params"],
          ~w(Bucket Key Prefix CopySource UseObjectLambdaEndpoint UseS3ExpressControlEndpoint DisableAccessPoints)
        )

      context = %Context{
        frontend: :aws,
        region: @reference["params"]["Region"] || "us-east-1",
        addressing: :virtual
      }

      result =
        EndpointRules.resolve(
          context,
          @operation["operationName"],
          @operation["operationParams"] || %{},
          parameters: client
        )

      case @reference["expect"] do
        %{"error" => message} ->
          assert {:error, %Error{message: ^message}} = result

        %{"endpoint" => expected} ->
          assert {:ok, endpoint} = result
          assert endpoint.endpoint == expected["url"]
          assert endpoint.raw.properties == (expected["properties"] || %{})
          assert endpoint.raw.headers == (expected["headers"] || %{})
      end
    end
  end
end
