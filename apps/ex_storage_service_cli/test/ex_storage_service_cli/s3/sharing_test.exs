defmodule ExStorageServiceCli.S3.SharingTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.S3.{PostPolicy, Presign}
  alias ExStorageServiceCli.Credentials.Record
  @now ~U[2025-01-01 00:00:00Z]
  @target %Target{kind: :remote, bucket: "bucket", key: "folder/a +%雪"}
  @oracle JSON.decode!(File.read!(Path.expand("../../fixtures/s3/post-policy.json", __DIR__)))

  defp context,
    do: %Context{
      frontend: :aws,
      endpoint: "https://s3.eu-west-1.amazonaws.com",
      region: "eu-west-1",
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret",
      session_token: "fixture-token"
    }

  test "POST policy signature matches independent stdlib HMAC oracle and binds exact key" do
    assert {:ok, %{"url" => url, "method" => "POST", "fields" => fields}} =
             PostPolicy.generate(context(), @target,
               now: @now,
               content_type: "text/plain",
               min_bytes: 1,
               max_bytes: 100
             )

    assert url == "https://s3.eu-west-1.amazonaws.com/bucket"
    assert fields["policy"] == @oracle["policy"]
    assert fields["x-amz-signature"] == @oracle["signature"]
    assert fields["key"] == @target.key
  end

  test "POST rejects unsupported providers, substitution keys, and impossible content ranges" do
    for {context, target, opts} <- [
          {%{context() | capabilities: %{provider: "ess"}}, @target, []},
          {context(), %{@target | key: "${filename}"}, []},
          {context(), @target, [min_bytes: 10, max_bytes: 9]},
          {context(), @target, [max_bytes: 100_000_000_000_000]}
        ] do
      assert {:error, %Error{code: :invalid_post_policy}} =
               PostPolicy.generate(context, target, opts)
    end
  end

  test "temporary credentials bound effective sharing lifetime and remain in signed query" do
    {:ok, credentials} =
      Record.new(
        %{
          access_key_id: "fixture-access",
          secret_access_key: "fixture-secret",
          session_token: "fixture-token",
          expires_at: DateTime.add(DateTime.utc_now(), 600)
        },
        %{provider: :explicit}
      )

    now = DateTime.utc_now()
    context = %{context() | credentials: credentials}

    assert {:ok, %{"url" => url, "expires" => 3600, "effective_expires" => effective}} =
             Presign.url(context, @target, "PUT", now: now)

    assert effective in 598..600
    assert URI.decode_query(URI.parse(url).query)["X-Amz-Security-Token"] == "fixture-token"
    assert URI.parse(url).path == "/bucket/folder/a%20%2B%25%E9%9B%AA"
    refute url =~ "fixture-secret"
  end
end
