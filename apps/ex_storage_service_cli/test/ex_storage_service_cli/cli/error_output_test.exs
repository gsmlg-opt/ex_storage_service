defmodule ExStorageServiceCli.CLI.ErrorOutputTest do
  # stderr capture replaces a VM-wide named IO device.
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Context, Error, ExitStatus}
  alias ExStorageServiceCli.CLI.ErrorOutput
  alias ExStorageServiceCli.S3.{Errors, Response, Retry}

  test "AWS HEAD denial uses the reference's plain fatal stderr and transfer status" do
    error = Errors.from_response("HeadObject", %Response{status: 403, body: ""}, %Context{})

    assert {:error, error} =
             Retry.run("HeadObject", [max_attempts: 1], fn _, _ -> {:error, error} end)

    error = %{error | details: Map.put(error.details, :high_level_prefix, "fatal error: ")}

    expected =
      "fatal error: An error occurred (403) when calling the HeadObject operation " <>
        "(reached max retries: 0): Forbidden\n"

    assert capture_io(:stderr, fn -> ErrorOutput.write(error, :s3) end) == expected
    assert ExitStatus.code(error, :s3) == 1
    assert ExitStatus.code(error, :s3api) == 254
  end

  test "modeled AWS errors retain the safe service message and operation" do
    error =
      Errors.from_response(
        "GetObject",
        %Response{
          status: 403,
          body: "<Error><Code>AccessDenied</Code><Message>denied secret</Message></Error>"
        },
        %Context{secret_access_key: "secret"}
      )

    assert ErrorOutput.message(error, :s3api) ==
             "An error occurred (AccessDenied) when calling the GetObject operation: denied [redacted]"
  end

  test "AWS parse errors have no native prefix or escape sequences" do
    error = Error.new(:syntax, "Unknown option: --bogus")

    assert capture_io(:stderr, fn -> ErrorOutput.write(error, :s3api) end) ==
             error.message <> "\n"

    assert ExitStatus.code(error, :s3api) == 252
  end
end
