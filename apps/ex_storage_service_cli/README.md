# ExStorageServiceCli

[![Hex.pm](https://img.shields.io/hexpm/v/ex_storage_service_cli.svg)](https://hex.pm/packages/ex_storage_service_cli)

A CLI tool (`ess`) for managing [ExStorageService](https://github.com/gsmlg-dev/ex_storage_service) S3-compatible object storage.

## Installation

```bash
mix escript.install hex ex_storage_service_cli
```

The `ess` binary will be installed to `~/.mix/escripts/ess`. Make sure `~/.mix/escripts` is in your `PATH`.

## Quick Start

```bash
# Configure credentials
ess configure

# Create a bucket
ess mb my-bucket

# Upload a file
ess cp ./file.txt s3://my-bucket/file.txt

# List objects
ess ls my-bucket

# Display objects as a tree
ess tree my-bucket

# Download a file
ess cp s3://my-bucket/file.txt ./downloaded.txt

# Delete an object
ess rm s3://my-bucket/file.txt

# Delete a bucket
ess rb my-bucket
```

## Commands

| Command | Description |
|---------|-------------|
| `ess configure` | Set up access credentials and endpoint |
| `ess mb <bucket>` | Make (create) a bucket |
| `ess rb <bucket>` | Remove (delete) a bucket |
| `ess ls [bucket[/prefix]]` | List buckets or objects |
| `ess tree <bucket[/prefix]>` | Display objects as a directory tree |
| `ess cp <src> <dst>` | Copy files (upload/download/S3-to-S3) |
| `ess rm s3://bucket/key` | Remove an object |
| `ess mv <src> <dst>` | Move an object (copy + delete) |
| `ess presign s3://bucket/key` | Generate a presigned URL |
| `ess info` | Show server health info |
| `ess version` | Print CLI version |

## Global Options

```
--endpoint <url>      S3 endpoint (default: http://localhost:9000)
--profile <name>      Use a named profile
--access-key <id>     Override access key ID
--secret-key <key>    Override secret access key
--region <region>     AWS region (default: us-east-1)
--json                Output in JSON format
--no-color            Disable colored output
-h, --help            Show help
```

## Configuration

Credentials are stored in `~/.config/ess/config.toml`:

```toml
[default]
endpoint = "http://localhost:9000"
access_key_id = "AKIA..."
secret_access_key = "..."
region = "us-east-1"

[profiles.production]
endpoint = "https://s3.example.com"
access_key_id = "AKIA..."
secret_access_key = "..."
region = "us-east-1"
```

Use `--profile <name>` to switch between profiles.

## License

MIT — see [LICENSE](LICENSE).
