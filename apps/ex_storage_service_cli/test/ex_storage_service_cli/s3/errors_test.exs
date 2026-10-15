defmodule ExStorageServiceCli.S3.ErrorsTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, S3.Errors, S3.Response}
  @fixture Path.expand("../../fixtures/differential/same-paths-wire.json", __DIR__)

  test "captured STS error envelopes preserve code, message, HTTP status and XML request ID" do
    oracle = JSON.decode!(File.read!(@fixture))

    for {scenario, code, message} <- [
          {"sts-access-denied", "AccessDenied", "STS denied caller"},
          {"sts-invalid-token", "InvalidClientTokenId",
           "The security token included in the request is invalid"}
        ] do
      golden = Enum.find(oracle["cases"], &(&1["name"] == scenario))
      [response] = golden["responses"]

      error =
        Errors.from_response(
          "GetCallerIdentity",
          %Response{status: response["status"], body: Base.decode64!(response["body_base64"])},
          %Context{}
        )

      assert error.category == :service
      assert error.code == code
      assert error.http_status == 403
      assert error.details.service_message == message
      assert error.request_id == "fixture-sts-request"
      assert error.operation == "GetCallerIdentity"
    end
  end

  test "nested error fields retain redaction and header request ID precedence" do
    body =
      "<ErrorResponse><Error><Code>AccessDenied</Code><Message>fixture-secret and fixture-token&#xA;denied</Message></Error><RequestId>xml-id</RequestId></ErrorResponse>"

    context = %Context{secret_access_key: "fixture-secret", session_token: "fixture-token"}

    error =
      Errors.from_response(
        "GetCallerIdentity",
        %Response{status: 403, request_id: "header-fixture-secret", body: body},
        context
      )

    assert error.details.service_message == "[redacted] and [redacted]?denied"
    assert error.request_id == "header-[redacted]"
    refute error.message =~ "fixture-secret"
    refute error.message =~ "fixture-token"
  end

  test "ambiguous or unrelated nested errors retain the HTTP fallback" do
    for body <- [
          "<ErrorResponse><Error><Code>First</Code></Error><Error><Code>Second</Code></Error></ErrorResponse>",
          "<OtherResponse><Error><Code>Ignored</Code></Error></OtherResponse>",
          "<ErrorResponse/>"
        ] do
      error =
        Errors.from_response("GetCallerIdentity", %Response{status: 403, body: body}, %Context{})

      assert error.code == "HTTP403"
      assert error.details.service_message == "Forbidden"
    end
  end

  test "ordinary S3 root errors retain their existing fields" do
    body =
      "<Error><Code>NoSuchBucket</Code><Message>Missing bucket</Message><Region>eu-west-1</Region></Error>"

    error =
      Errors.from_response(
        "HeadBucket",
        %Response{status: 404, body: body, request_id: "s3-id"},
        %Context{}
      )

    assert error.code == "NoSuchBucket"
    assert error.details.service_message == "Missing bucket"
    assert error.details.bucket_region == "eu-west-1"
    assert error.request_id == "s3-id"
  end
end
