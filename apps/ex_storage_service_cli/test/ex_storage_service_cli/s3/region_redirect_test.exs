defmodule ExStorageServiceCli.S3.RegionRedirectTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, S3.EndpointRules, S3.RegionRedirect}

  defp context(options \\ %{}),
    do: %Context{frontend: :aws, region: "us-east-1", addressing: :virtual, options: options}

  defp error(status \\ 301, code \\ "PermanentRedirect"),
    do: Error.new(:service, "wrong region", http_status: status, code: code)

  test "re-resolves regional endpoint and signing context without using Location" do
    headers = [
      {"X-Amz-Bucket-Region", "eu-west-1"},
      {"location", "https://attacker.example/steal"}
    ]

    original = context()

    assert {:ok, corrected} =
             RegionRedirect.correct(
               original,
               "GetObject",
               %{"Bucket" => "my-bucket"},
               error(),
               headers
             )

    assert corrected.region == "eu-west-1"
    assert original.region == "us-east-1"

    assert {:ok, contract} =
             EndpointRules.resolve(corrected, "GetObject", %{"Bucket" => "my-bucket"})

    assert contract.endpoint == "https://my-bucket.s3.eu-west-1.amazonaws.com"
    assert contract.region == "eu-west-1"

    assert :stop =
             RegionRedirect.correct(corrected, "GetObject", %{"Bucket" => "my-bucket"}, error(), [
               {"x-amz-bucket-region", "us-west-2"}
             ])
  end

  test "custom endpoints retain their original host, path, and TLS policy" do
    original = %{
      context(%{resolved_endpoint: "https://storage.example/base"})
      | addressing: :path,
        tls: [verify: :verify_peer, cacertfile: "/safe/ca.pem"]
    }

    assert {:ok, corrected} =
             RegionRedirect.correct(
               original,
               "HeadBucket",
               %{"Bucket" => "my-bucket"},
               error(400, "HTTP400"),
               %{"x-amz-bucket-region" => ["us-west-2"]}
             )

    assert corrected.tls == original.tls

    assert {:ok, contract} =
             EndpointRules.resolve(corrected, "HeadBucket", %{"Bucket" => "my-bucket"})

    assert contract.endpoint == "https://storage.example/base/my-bucket"
    assert contract.region == "us-west-2"
  end

  test "region correction rejects ARN, cross-partition, ambiguous, malformed and nonreplayable inputs" do
    for region <- [
          "cn-north-1",
          "us-gov-west-1",
          "eu-west-1\r\nHost:attacker",
          "https://attacker.example"
        ] do
      assert :stop =
               RegionRedirect.correct(
                 context(),
                 "GetObject",
                 %{"Bucket" => "my-bucket"},
                 error(),
                 [{"x-amz-bucket-region", region}]
               )
    end

    headers = [{"x-amz-bucket-region", "us-west-2"}]

    assert :stop =
             RegionRedirect.correct(
               context(),
               "GetObject",
               %{"Bucket" => "arn:aws:s3:us-west-2:123456789012:accesspoint/test"},
               error(),
               headers
             )

    assert :stop =
             RegionRedirect.correct(
               context(),
               "GetObject",
               %{"Bucket" => "my-bucket"},
               error(),
               headers ++ headers
             )

    assert :stop =
             RegionRedirect.correct(
               context(),
               "GetObject",
               %{"Bucket" => "my-bucket"},
               error(),
               headers,
               replayable: false
             )

    assert :stop = RegionRedirect.correct(context(), "ListBuckets", %{}, error(), headers)

    assert :stop =
             RegionRedirect.correct(context(), "GetObject", %{"Bucket" => "my-bucket"}, error(), [
               {"location", "https://attacker.example"}
             ])
  end

  test "same-region redirects re-resolve once, including a custom endpoint" do
    original = %{
      context(%{resolved_endpoint: "https://storage.example/base"})
      | addressing: :path
    }

    input = %{"Bucket" => "my-bucket"}
    headers = [{"x-amz-bucket-region", "us-east-1"}]

    assert {:ok, corrected} =
             RegionRedirect.correct(original, "HeadBucket", input, error(), headers)

    assert corrected.region == original.region
    assert {:ok, contract} = EndpointRules.resolve(corrected, "HeadBucket", input)
    assert contract.endpoint == "https://storage.example/base/my-bucket"
    assert :stop = RegionRedirect.correct(corrected, "HeadBucket", input, error(), headers)
  end

  test "body-region fallback and modeled correction triggers are bounded" do
    input = %{"Bucket" => "my-bucket"}

    assert {:ok, _} =
             RegionRedirect.correct(
               context(),
               "GetObject",
               input,
               error(400, "AuthorizationHeaderMalformed"),
               [],
               error_region: "us-west-2"
             )

    assert :stop =
             RegionRedirect.correct(
               context(),
               "CreateBucket",
               input,
               error(400, "IllegalLocationConstraintException"),
               [],
               error_region: "us-west-2"
             )

    assert :stop =
             RegionRedirect.correct(context(), "GetObject", input, error(403, "AccessDenied"), [],
               error_region: "us-west-2"
             )

    assert :stop =
             RegionRedirect.correct(context(), "GetObject", input, error(), [],
               error_region: "us-west-2",
               region_corrections: 1
             )
  end
end
