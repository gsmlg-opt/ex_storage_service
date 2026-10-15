defmodule ExStorageServiceCli.S3.Capabilities do
  @moduledoc "Explicit provider boundaries. Permission failures never establish feature support."
  alias ExStorageServiceCli.Error

  # Source-reviewed ESS 0.6.7 router. Unknown subresources may fall through to a
  # different destructive operation, so never probe them against a declared ESS.
  @ess_operations ~w(ListBuckets CreateBucket DeleteBucket HeadBucket ListObjects ListObjectsV2
    PutObject GetObject HeadObject CopyObject DeleteObject DeleteObjects
    CreateMultipartUpload UploadPart ListParts CompleteMultipartUpload AbortMultipartUpload
    GetBucketVersioning PutBucketVersioning GetBucketLifecycleConfiguration
    PutBucketLifecycleConfiguration DeleteBucketLifecycle
    GetBucketNotificationConfiguration PutBucketNotificationConfiguration)

  def record(provider, version \\ nil, bucket_type \\ "general") do
    %{provider: provider, version: version, bucket_type: bucket_type, evidence: :declared}
  end

  def check(context, operation, input \\ %{}) do
    capabilities = context.capabilities

    cond do
      capabilities[:provider] == "ess" and operation not in @ess_operations ->
        unavailable(operation, "The declared ESS server does not safely route this S3 operation")

      capabilities[:provider] == "ess" and
          Enum.any?(
            Map.keys(input),
            &(is_binary(&1) and String.starts_with?(&1, ["SSE", "ObjectLock"]))
          ) ->
        unavailable(
          operation,
          "The declared ESS server does not implement the requested encryption or Object Lock fields"
        )

      true ->
        :ok
    end
  end

  def describe(context) do
    provider = context.capabilities[:provider] || "unknown"

    %{
      "Provider" => provider,
      "Version" => context.capabilities[:version],
      "BucketType" => context.capabilities[:bucket_type] || "general",
      "Evidence" => "declaration and source-reviewed safety exclusions; no permission probes",
      "KnownOperations" => if(provider == "ess", do: @ess_operations, else: []),
      "UnknownCapabilities" => provider != "ess"
    }
  end

  defp unavailable(operation, message),
    do: {:error, Error.new(:client, message, code: :provider_unsupported, operation: operation)}
end
