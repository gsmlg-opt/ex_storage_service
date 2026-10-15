#!/usr/bin/env python3
"""Disposable loopback-only CLI/provider acceptance; never logs credential values."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
from urllib.parse import urlparse
import zlib


def execute(command, *, env=None, timeout=120):
    return subprocess.run(command, env=env, capture_output=True, timeout=timeout)


def require_loopback(endpoint):
    parsed = urlparse(endpoint)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port:
        raise ValueError("Provider probes require explicit http://127.0.0.1:<port> endpoints")


def inspect_disposable(runtime, container, endpoint):
    """Complete read-only admission before credential seeding or object writes."""
    require_loopback(endpoint)
    if not container.startswith("ess-cli-"):
        raise ValueError("Probe containers must have the disposable ess-cli- name prefix")
    result = execute([runtime, "inspect", container])
    if result.returncode:
        raise RuntimeError("Cannot inspect the disposable provider container")
    info = json.loads(result.stdout)[0]
    if info["Name"].lstrip("/") != container:
        raise RuntimeError("Disposable container identity mismatch")
    identity = info.get("Id", "")
    if len(identity) != 64 or any(character not in "0123456789abcdef" for character in identity):
        raise RuntimeError("Disposable container requires an immutable full container ID")
    if any(mount.get("Type") == "bind" for mount in info.get("Mounts", [])):
        raise RuntimeError("Provider probes refuse containers with host bind mounts")
    ports = info.get("NetworkSettings", {}).get("Ports", {})
    if any(mapping.get("HostIp") != "127.0.0.1" for mappings in ports.values() for mapping in (mappings or [])):
        raise RuntimeError("Provider probes refuse any non-loopback published port")
    port = str(urlparse(endpoint).port)
    if not any(mapping.get("HostPort") == port for mapping in ports.get("9000/tcp") or []):
        raise RuntimeError("Endpoint does not match the disposable loopback container mapping")
    return info


def digest(path):
    result = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            result.update(chunk)
    return result.hexdigest()


class Probe:
    def __init__(self, args, root):
        self.args, self.root = args, Path(root)
        self.report = {"schema_version": 1, "artifact_sha256": digest(args.ess), "independent_aws": not args.reference_is_ess, "providers": []}
        if not args.reference_is_ess:
            version = execute([args.aws, "--version"])
            self.report["aws_reference"] = {"artifact_sha256": digest(args.aws), "version": version.stdout.decode().strip()}
        self.credentials = []

    def redact(self, value):
        value = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
        for credential in self.credentials:
            for name in ("access_key", "secret_key"):
                value = value.replace(credential[name], "[REDACTED]")
        return value

    def seed(self, container):
        suffix = secrets.token_hex(6)
        location = "/tmp/ess-cli-probe-" + suffix + ".json"
        expression = f'''
        alias ExStorageService.IAM.{{User, Policy, AccessKey}}
        {{:ok, user}} = User.create_user("cli-probe-{suffix}")
        {{:ok, policy}} = Policy.create_policy("cli-probe-{suffix}", Policy.full_access_statements())
        :ok = Policy.attach_policy(user.id, policy.id)
        {{:ok, key}} = AccessKey.create_access_key(user.id)
        File.open!("{location}", [:write, :exclusive], fn file ->
          File.chmod!("{location}", 0o600)
          IO.binwrite(file, JSON.encode!(%{{"access_key" => key.access_key_id,
            "secret_key" => key.secret_access_key, "user_id" => user.id, "policy_id" => policy.id}}))
        end)
        :ok
        '''
        seeded = execute([self.args.runtime, "exec", container, "/app/bin/ess", "rpc", expression])
        if seeded.returncode:
            raise RuntimeError("Disposable ESS IAM seed failed; credentials were not printed")
        data = execute([self.args.runtime, "exec", container, "cat", location])
        if data.returncode:
            raise RuntimeError("Cannot retrieve the private disposable ESS credential file")
        path = self.root / "ess-credentials.json"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data.stdout)
        execute([self.args.runtime, "exec", container, "rm", "--", location])
        record = json.loads(data.stdout)
        self.credentials.append(record)
        return record

    def environment(self, credential, directory):
        env = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "ESS_", "MC_"))}
        env.update(AWS_CONFIG_FILE="/dev/null", AWS_SHARED_CREDENTIALS_FILE="/dev/null",
                   AWS_ACCESS_KEY_ID=credential["access_key"], AWS_SECRET_ACCESS_KEY=credential["secret_key"],
                   AWS_EC2_METADATA_DISABLED="true", AWS_DEFAULT_REGION="us-east-1", AWS_MAX_ATTEMPTS="1",
                   AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off", PAGER="cat", ERL_FLAGS="+S 2:2",
                   ESS_CONFIG_DIR=str(directory / "config"), ESS_STATE_DIR=str(directory / "state"),
                   MC_CONFIG_DIR=str(directory / "mc"))
        return env

    def provider(self, name, endpoint, container, credential):
        info = self.args.provider_containers[container]
        image = json.loads(execute([self.args.runtime, "image", "inspect", info["Image"]]).stdout)[0]
        report = {"provider": name, "endpoint": endpoint, "container_id": info["Id"],
                  "image_reference": info.get("Config", {}).get("Image"),
                  "image_id": info["Image"], "image_digests": image.get("RepoDigests", []),
                  "checks": [], "failures": [], "limitations": [], "cleanup": []}
        self.report["providers"].append(report)
        directory = self.root / name
        directory.mkdir(mode=0o700)
        env = self.environment(credential, directory)
        prefix = "ess-cli-probe-" + name + "-" + secrets.token_hex(6)
        bucket, configuration_bucket = prefix, prefix + "-config"
        owned = []
        uploaded_raw_keys = []

        def call(arguments, *, reference=False, native=False, check=True):
            if reference:
                command = (["escript", self.args.aws] if self.args.reference_is_ess else [self.args.aws]) + ["--endpoint-url", endpoint, "--region", "us-east-1", "--no-cli-pager"]
            elif native:
                command = ["escript", self.args.ess, "--service", "ess" if name == "ess" else "s3"]
                if arguments[0] != "cat":
                    command += ["--output", "json"]
            else:
                command = ["escript", self.args.ess, "--endpoint-url", endpoint, "--region", "us-east-1", "--no-cli-pager"]
            result = execute(command + arguments, env=env)
            if check and result.returncode:
                raise RuntimeError(self.redact(result.stderr)[:2000])
            return result

        def api(operation, *arguments, **options):
            return call(["s3api", operation, *arguments], **options)

        def record(label, function):
            if self.args.reference_is_ess:
                label = label.replace("independent AWS", "same-artifact")
            try:
                function()
                report["checks"].append(label)
                print(name + ": PASS " + label, flush=True)
            except Exception as error:
                report["failures"].append({"check": label, "error": self.redact(error)})
                print(name + ": FAIL " + label + " " + self.redact(error), flush=True)

        def download_verify(key, source):
            destination = directory / "verified"
            api("get-object", "--bucket", bucket, "--key", key, str(destination), reference=True)
            if digest(source) != digest(destination):
                raise AssertionError("Independent AWS download bytes differ")

        try:
            api("create-bucket", "--bucket", bucket)
            owned.append(bucket)
            api("create-bucket", "--bucket", configuration_bucket)
            owned.append(configuration_bucket)
            call(["alias", "set", "probe", endpoint, "--provider", "environment"], native=True)
            report["checks"].append("authenticated create-bucket and environment alias")
            raw = bytes(range(256)) * 257
            for encoding, body in [("binary", raw), ("gzip", gzip.compress(raw, mtime=0)), ("deflate", zlib.compress(raw))]:
                source = directory / (encoding + ".bin")
                source.write_bytes(body)
                key = "unusual/" + encoding + "% +?#/雪/bytes.bin"

                def raw_roundtrip(source=source, key=key, encoding=encoding):
                    arguments = ["--bucket", bucket, "--key", key, "--body", str(source)]
                    if encoding != "binary":
                        arguments += ["--content-encoding", encoding]
                    api("put-object", *arguments)
                    uploaded_raw_keys.append(key)
                    download_verify(key, source)
                    metadata = json.loads(api("head-object", "--bucket", bucket, "--key", key, reference=True).stdout)
                    report.setdefault("content_encoding", {})[encoding] = {
                        "requested": None if encoding == "binary" else encoding,
                        "aws_head_content_encoding": metadata.get("ContentEncoding"),
                        "raw_bytes_verified": True,
                    }
                    native = call(["cat", "probe/" + bucket + "/" + key], native=True)
                    if native.stdout != source.read_bytes():
                        raise AssertionError("Native cat changed raw object bytes")
                    output = directory / "highlevel-download"
                    call(["s3", "cp", "s3://" + bucket + "/" + key, str(output), "--no-progress"])
                    if digest(source) != digest(output):
                        raise AssertionError("High-level download changed raw object bytes")

                record("raw " + encoding + " unusual-key API/AWS/native/highlevel roundtrip", raw_roundtrip)

            def listed_keys():
                # TODO(upstream): gsmlg-opt/ex_storage_service#22
                result = json.loads(api("list-objects-v2", "--bucket", bucket, "--prefix", "unusual/", reference=True).stdout)
                observed = [item["Key"] for item in result.get("Contents", [])]
                report["listed_raw_keys"] = {"uploaded": uploaded_raw_keys, "listed_by_aws": observed}
                if sorted(observed) != sorted(uploaded_raw_keys):
                    raise AssertionError("Provider inventory changed literal object keys; independent AWS results retained")

            record("independent AWS unusual-key inventory identity", listed_keys)

            source = directory / "small"
            source.write_bytes(b"source\x00\xff")
            api("put-object", "--bucket", bucket, "--key", "move/source", "--body", str(source))

            def copy_move():
                call(["cp", "probe/" + bucket + "/move/source", "probe/" + bucket + "/move/copy"], native=True)
                download_verify("move/copy", source)
                moved = call(["mv", "probe/" + bucket + "/move/copy", "probe/" + bucket + "/move/moved"], native=True, check=False)
                download_verify("move/moved", source)
                if moved.returncode == 0 or b"no verified conditional deletion contract" not in moved.stderr:
                    raise AssertionError("Unverified native move must fail closed")
                download_verify("move/copy", source)
                report["limitations"].append("Native unversioned move preserves the source because conditional deletion is unverified")

            record("native copy verification and unverified move source-preservation", copy_move)

            def multipart():
                source = directory / "multipart"
                with source.open("wb") as stream:
                    for _ in range(144):
                        stream.write(raw[:65536])
                call(["s3", "cp", str(source), "s3://" + bucket + "/multipart", "--no-progress"])
                download_verify("multipart", source)
                if name == "rustfs":
                    uploads = json.loads(api("list-multipart-uploads", "--bucket", bucket, reference=True).stdout)
                    if uploads.get("Uploads"):
                        raise AssertionError("Multipart upload remained incomplete")

            record("9MiB multipart upload and independent AWS byte verification", multipart)

            def pagination():
                for index in range(5):
                    api("put-object", "--bucket", bucket, "--key", "pages/" + str(index), "--body", str(source))
                result = json.loads(api("list-objects-v2", "--bucket", bucket, "--prefix", "pages/", "--page-size", "2").stdout)
                keys = [entry["Key"] for entry in result.get("Contents", [])]
                if keys != ["pages/" + str(index) for index in range(5)]:
                    raise AssertionError("Paginated inventory omitted or duplicated keys")

            record("five keys through two-item API pagination", pagination)

            def sync():
                local = directory / "sync"
                local.mkdir()
                (local / "a.txt").write_bytes(b"new")
                (local / "ignored.bin").write_bytes(b"excluded")
                api("put-object", "--bucket", bucket, "--key", "sync/old.txt", "--body", str(source))
                call(["s3", "sync", str(local) + "/", "s3://" + bucket + "/sync/", "--delete", "--exclude", "*.bin", "--no-progress"])
                listed = json.loads(api("list-objects-v2", "--bucket", bucket, "--prefix", "sync/", reference=True).stdout)
                if [entry["Key"] for entry in listed.get("Contents", [])] != ["sync/a.txt"]:
                    raise AssertionError("Sync failed selected copy/delete/exclusion proof")
                download_verify("sync/a.txt", local / "a.txt")

            record("sync upload/delete/exclude independently listed and downloaded", sync)

            def provider_key_boundaries():
                key = "dot//./../key"
                if name == "rustfs":
                    arguments = ["--bucket", bucket, "--key", key, "--body", str(source)]
                    current = api("put-object", *arguments, check=False)
                    reference = api("put-object", *arguments, reference=True, check=False)
                    report["key_boundary"] = {"key": key, "ess_status": current.returncode,
                        "aws_status": reference.returncode, "ess_error": self.redact(current.stderr),
                        "aws_error": self.redact(reference.stderr)}
                    if current.returncode or reference.returncode:
                        raise AssertionError("RustFS rejects dot-segment key; both executable results retained")
                else:
                    results = {}
                    # TODO(upstream): gsmlg-opt/ex_storage_service#20
                    for key in ["delete-ascii", "delete-雪"]:
                        api("put-object", "--bucket", bucket, "--key", key, "--body", str(source))
                        document = json.dumps({"Objects": [{"Key": key}]}, ensure_ascii=False)
                        current = api("delete-objects", "--bucket", bucket, "--delete", document, check=False)
                        reference = api("delete-objects", "--bucket", bucket, "--delete", document, reference=True, check=False)
                        results[key] = {"ess_status": current.returncode, "aws_status": reference.returncode,
                            "ess_error": self.redact(current.stderr), "aws_error": self.redact(reference.stderr)}
                    report["delete_boundary"] = results
                    if any(result["ess_status"] or result["aws_status"] for result in results.values()):
                        raise AssertionError("Published ESS multi-delete boundary failure; independent results retained")

            record("independent provider unusual-key and multi-delete boundaries", provider_key_boundaries)

            def encoding_metadata():
                # TODO(upstream): gsmlg-opt/ex_storage_service#21
                for encoding, observed in report.get("content_encoding", {}).items():
                    if observed["requested"] != observed["aws_head_content_encoding"]:
                        raise AssertionError("Provider did not retain Content-Encoding=" + encoding + "; raw byte result recorded separately")

            record("independent AWS Content-Encoding metadata retention", encoding_metadata)

            def versioning():
                api("put-bucket-versioning", "--bucket", configuration_bucket, "--versioning-configuration", '{"Status":"Enabled"}')
                result = json.loads(api("get-bucket-versioning", "--bucket", configuration_bucket, reference=True).stdout)
                if result.get("Status") != "Enabled":
                    raise AssertionError("Versioning configuration was not retained")
                if name == "rustfs":
                    api("put-object", "--bucket", configuration_bucket, "--key", "versioned-source", "--body", str(source))
                    before = json.loads(api("head-object", "--bucket", configuration_bucket, "--key", "versioned-source", reference=True).stdout)
                    if not before.get("VersionId") or before["VersionId"] == "null":
                        raise AssertionError("Versioned source lacks an immutable version ID")
                    call(["mv", "probe/" + configuration_bucket + "/versioned-source", "probe/" + configuration_bucket + "/versioned-moved"], native=True)
                    output = directory / "versioned-verified"
                    api("get-object", "--bucket", configuration_bucket, "--key", "versioned-moved", str(output), reference=True)
                    if digest(output) != digest(source):
                        raise AssertionError("Versioned move changed bytes")
                    if api("head-object", "--bucket", configuration_bucket, "--key", "versioned-source", "--version-id", before["VersionId"], reference=True, check=False).returncode == 0:
                        raise AssertionError("Move did not delete the selected immutable version")
                    report["checks"].append("native versioned move deletes only selected immutable source and preserves destination bytes")
                api("put-bucket-versioning", "--bucket", configuration_bucket, "--versioning-configuration", '{"Status":"Suspended"}')

            record("empty configuration-bucket versioning roundtrip", versioning)

            def lifecycle():
                configuration = {"Rules": [{"ID": "expiry", "Status": "Enabled", "Filter": {"Prefix": "expire/"}, "Expiration": {"Days": 7}}]}
                api("put-bucket-lifecycle-configuration", "--bucket", configuration_bucket, "--lifecycle-configuration", json.dumps(configuration))
                result = json.loads(api("get-bucket-lifecycle-configuration", "--bucket", configuration_bucket, reference=True).stdout)
                if result.get("Rules") != configuration["Rules"]:
                    raise AssertionError("Lifecycle configuration changed during roundtrip")
                api("delete-bucket-lifecycle", "--bucket", configuration_bucket)

            record("lifecycle document roundtrip independently read by AWS", lifecycle)
        finally:
            for target in reversed(owned):
                try:
                    if name == "ess" and target == bucket:
                        # WORKAROUND(upstream): gsmlg-opt/ex_storage_service#22
                        # Fixture cleanup uses known original keys, never rewritten inventory keys.
                        for key in uploaded_raw_keys:
                            api("delete-object", "--bucket", target, "--key", key, reference=True)
                        report["cleanup"].append({"bucket": target, "status": "fixture-only known-key cleanup", "upstream_issue": 22})
                    if name == "rustfs" and target == configuration_bucket:
                        versions = json.loads(api("list-object-versions", "--bucket", target, reference=True).stdout)
                        selected = [{"Key": row["Key"], "VersionId": row["VersionId"]} for kind in ["Versions", "DeleteMarkers"] for row in versions.get(kind, [])]
                        if selected:
                            api("delete-objects", "--bucket", target, "--delete", json.dumps({"Objects": selected}), reference=True)
                    listing = json.loads(api("list-objects-v2", "--bucket", target, reference=True).stdout)
                    objects = [{"Key": row["Key"]} for row in listing.get("Contents", [])]
                    if objects:
                        deleted = api("delete-objects", "--bucket", target, "--delete", json.dumps({"Objects": objects}), reference=True, check=False)
                        if deleted.returncode:
                            if name != "ess":
                                raise RuntimeError(self.redact(deleted.stderr))
                            # WORKAROUND(upstream): gsmlg-opt/ex_storage_service#20
                            # Only remove this probe's owned fixtures after recording the server failure.
                            report["cleanup"].append({"bucket": target, "status": "fixture-only individual cleanup", "upstream_issue": 20})
                            for obj in objects:
                                api("delete-object", "--bucket", target, "--key", obj["Key"], reference=True)
                    api("delete-bucket", "--bucket", target, reference=True)
                    report["cleanup"].append({"bucket": target, "status": "removed"})
                except Exception as error:
                    report["cleanup"].append({"bucket": target, "status": "failed", "error": self.redact(error)})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--escript", "--ess", dest="ess", required=True, help="private standalone escript snapshot")
    parser.add_argument("--aws-executable", "--aws", dest="aws", help="pinned independent AWS executable; optional but required for independent proof")
    parser.add_argument("--ess-endpoint", required=True)
    parser.add_argument("--rustfs-endpoint", required=True)
    parser.add_argument("--ess-container", required=True)
    parser.add_argument("--rustfs-container", required=True)
    parser.add_argument("--report", help="exact report filename (or use --output-dir)")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--runtime", choices=["docker", "podman"], default="podman")
    parser.add_argument("--keep-containers", action="store_true")
    parser.add_argument("--rustfs-access-key", default=os.environ.get("RUSTFS_ACCESS_KEY", "fixture-access"))
    parser.add_argument("--rustfs-secret-key-file", type=Path)
    args = parser.parse_args()
    if not args.report:
        if not args.output_dir:
            parser.error("--output-dir or --report is required")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        args.report = str(args.output_dir / "provider-report.json")
    if not args.aws:
        args.aws = args.ess
        args.reference_is_ess = True
    else:
        args.reference_is_ess = False
    for container in [args.ess_container, args.rustfs_container]:
        if not container.startswith("ess-cli-"):
            parser.error("Probe containers must have the disposable ess-cli- name prefix")
    require_loopback(args.ess_endpoint)
    require_loopback(args.rustfs_endpoint)
    # Admit both providers before the first write; keep immutable identities for
    # exec and teardown even if another process later reuses either display name.
    args.provider_containers = {
        args.ess_container: inspect_disposable(args.runtime, args.ess_container, args.ess_endpoint),
        args.rustfs_container: inspect_disposable(args.runtime, args.rustfs_container, args.rustfs_endpoint),
    }
    with tempfile.TemporaryDirectory(prefix="ess-provider-probe-") as root:
        probe = Probe(args, root)
        ess = probe.seed(args.provider_containers[args.ess_container]["Id"])
        rustfs = {"access_key": args.rustfs_access_key, "secret_key": args.rustfs_secret_key_file.read_text().strip() if args.rustfs_secret_key_file else os.environ.get("RUSTFS_SECRET_KEY", "fixture-secret-long-enough")}
        probe.credentials.append(rustfs)
        for provider in [("ess", args.ess_endpoint, args.ess_container, ess), ("rustfs", args.rustfs_endpoint, args.rustfs_container, rustfs)]:
            try:
                probe.provider(*provider)
            except Exception as error:
                if probe.report["providers"] and probe.report["providers"][-1]["provider"] == provider[0]:
                    probe.report["providers"][-1]["failures"].append({"check": "provider setup", "error": probe.redact(error)})
                else:
                    raise
        if not args.keep_containers:
            for info in args.provider_containers.values():
                removed = execute([args.runtime, "rm", "--force", "--volumes", info["Id"]])
                if removed.returncode:
                    raise RuntimeError("Cannot remove the owned disposable acceptance container")
        Path(args.report).write_text(json.dumps(probe.report, indent=2) + "\n")
        print("REPORT " + args.report)
        return int(any(provider["failures"] or any(row["status"] == "failed" for row in provider["cleanup"]) for provider in probe.report["providers"]))


if __name__ == "__main__":
    raise SystemExit(main())
