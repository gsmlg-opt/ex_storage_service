# ExStorageServiceSdk

[![Hex.pm](https://img.shields.io/hexpm/v/ex_storage_service_sdk.svg)](https://hex.pm/packages/ex_storage_service_sdk)

An Elixir S3 client for [ExStorageService](https://github.com/gsmlg-opt/ex_storage_service)
and other S3-compatible storage such as MinIO, Cloudflare R2 and AWS S3.

It is the protocol layer of the `ess` CLI, published on its own: SigV4/SigV4a
signing, the pinned S3 service model (AWS CLI 2.31.18) for request encoding and
response decoding, pagination, retries, checksums and multipart uploads. It does
not start any server, storage engine or terminal machinery, and it does not depend
on the AWS SDK.

## Installation

```elixir
def deps do
  [
    {:ex_storage_service_sdk, "~> 0.7.0"}
  ]
end
```

Requires Elixir 1.18+ and OTP 27+. HTTP goes through
[`http_fetch`](https://hex.pm/packages/http_fetch) `~> 1.0.0-rc.10`.

## Usage

```elixir
{:ok, s3} =
  ExStorageServiceSdk.new(
    endpoint: "http://localhost:9000",   # any HTTP(S) endpoint and port
    region: "us-east-1",
    access_key_id: System.fetch_env!("S3_ACCESS_KEY_ID"),
    secret_access_key: System.fetch_env!("S3_SECRET_ACCESS_KEY")
    # path_style: true is the default; set false for virtual-hosted buckets
  )

{:ok, %{etag: etag}} =
  ExStorageServiceSdk.put_object(s3, "notes", "attachments/a b+c%.json", ~s({"x": 1}),
    %{"ContentType" => "application/json", "IfNoneMatch" => "*"})

{:ok, %{status: 206, body: "{\"x"}} =
  ExStorageServiceSdk.get_object(s3, "notes", "attachments/a b+c%.json",
    %{"Range" => "bytes=0-3"})

keys =
  s3
  |> ExStorageServiceSdk.pages("ListObjectsV2", %{"Bucket" => "notes", "Prefix" => "attachments/"})
  |> Stream.flat_map(fn {:ok, page} -> page["Contents"] || [] end)
  |> Enum.map(& &1["Key"])
```

Keys round-trip exactly, including Unicode, spaces, `+`, `%` and repeated `/`.

### Any operation

Every operation in the S3 model is available through `call/4`, using the
model's member names:

```elixir
ExStorageServiceSdk.call(s3, "PutBucketVersioning", %{
  "Bucket" => "notes",
  "VersioningConfiguration" => %{"Status" => "Enabled"}
})
```

### Multipart uploads

```elixir
{:ok, %{"UploadId" => id}} = ExStorageServiceSdk.create_multipart_upload(s3, "b", "big.bin")

parts =
  chunks
  |> Enum.with_index(1)
  |> Enum.map(fn {chunk, n} ->
    {:ok, %{etag: etag}} = ExStorageServiceSdk.upload_part(s3, "b", "big.bin", id, n, chunk)
    {n, etag}
  end)

case ExStorageServiceSdk.complete_multipart_upload(s3, "b", "big.bin", id, parts) do
  {:ok, result} -> {:ok, result}
  {:error, _} = error ->
    ExStorageServiceSdk.abort_multipart_upload(s3, "b", "big.bin", id)
    error
end
```

Parts other than the last must be at least 5 MiB on AWS and MinIO.

### Large downloads

`get_object/5` keeps the body in memory and refuses bodies over
`:max_body_bytes` (default 8 MiB). For large objects, stream the body:

```elixir
{:ok, response} = ExStorageServiceSdk.get_object_stream(s3, "b", "big.bin")
{:ok, file} = File.open("big.bin", [:write, :binary])

{:ok, _bytes, _metadata} =
  s3.transport.consume(response, 0, fn chunk, size ->
    :ok = IO.binwrite(file, chunk)
    {:ok, size + byte_size(chunk)}
  end, [])
```

## Errors

Every call returns `{:ok, ...}` or `{:error, %ExStorageServiceSdk.Error{}}`:

| `category`   | Meaning                                    | Useful fields                                              |
|--------------|--------------------------------------------|------------------------------------------------------------|
| `:service`   | The server answered with an error          | `http_status` (404, 412, 416, ...), `code` (`"NoSuchKey"`) |
| `:transport` | Connection, TLS or timeout failure         | `code` (`:econnrefused`, `:timeout`, ...)                  |
| `:protocol`  | Response is not valid S3                   | includes HTTP 200 responses carrying an S3 `<Error>`       |

A HEAD request has no error body, so a missing object is
`%Error{http_status: 404, code: "HTTP404"}`.

## Retries and conditional writes

Reads and idempotent multipart steps retry transient failures (3 attempts by
default; pass `max_attempts:` in the per-call options). `PutObject`,
`CopyObject`, `DeleteObject` and `CompleteMultipartUpload` are never replayed. If
one may have reached the server but its result is unknown, the call fails with
`code: :indeterminate`. For conditional writes, reconcile by reading the object
(for example its ETag) before trying again: a replayed `If-None-Match: *` write
fails with 412 even when the first attempt succeeded.

## Configuration per client

A config captures its endpoint, region, credentials and transport. Long listings
and multipart uploads keep using the config they started with, even if your
application's settings change mid-operation. Other extension points:

- `credential_provider:` - a function returning fresh
  `%ExStorageServiceSdk.Credentials{}`, called before each signed request.
- `transport:` - a module implementing `ExStorageServiceSdk.Transport`.
- `options: %{request_headers: [{"x-header", "value"}]}` - extra request headers.

## License

MIT. The bundled S3 service model under `priv/compatibility/aws` is from Botocore,
licensed under Apache 2.0; see the license and notice files in that directory.
