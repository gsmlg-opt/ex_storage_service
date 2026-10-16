#!/usr/bin/env python3
"""Version preparation and image acceptance for the root release workflow only."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
import urllib.error
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[2]
APPS = ("ex_storage_service", "ex_storage_service_cluster", "ex_storage_service_cli",
        "ex_storage_service_s3", "ex_storage_service_web")
VERSION_FILES = ("mix.exs",) + tuple(f"apps/{app}/mix.exs" for app in APPS)


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def version_pattern(path):
    return r'(@version\s+")([^"\n]+)(")' if "_cli/" in path else r'(version:\s*")([^"\n]+)(")'


def versions(root):
    result = {}
    for name in VERSION_FILES:
        matches = list(re.finditer(version_pattern(name), (root / name).read_text()))
        if len(matches) != 1:
            raise RuntimeError(f"Expected one project version in {name}")
        result[name] = matches[0].group(2)
    return result


def set_versions(root, version):
    versions(root)  # Validate every file before changing any file.
    for name in VERSION_FILES:
        path = root / name
        path.write_text(re.sub(version_pattern(name),
                               lambda match: match[1] + version + match[3], path.read_text()))


def output(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
            for key in ("source_sha", "release_sha", "reused"):
                if key in value:
                    stream.write(f"{key}={str(value[key]).lower()}\n")


def prepare(version, path):
    if git("status", "--porcelain"):
        raise RuntimeError("Release preparation requires a clean checkout")
    source = git("rev-parse", "HEAD")
    git("fetch", "origin", "main", "--tags")
    tag = f"refs/tags/v{version}"
    remote = git("ls-remote", "origin", tag, tag + "^{}")
    reused = bool(remote)
    if reused:
        release = git("rev-parse", tag + "^{commit}")
        message = git("show", "-s", "--format=%B", release)
        match = re.search(r"^Release-Source: ([0-9a-f]{40})$", message, re.M)
        original = match[1] if match else release
        if match:
            if git("rev-parse", release + "^") != original:
                raise RuntimeError("Release source must be the version commit's parent")
            changed = set(git("diff", "--name-only", original, release).splitlines())
            if not changed or not changed.issubset(VERSION_FILES):
                raise RuntimeError("Release commit contains changes outside component versions")
            for name in VERSION_FILES:
                before = subprocess.check_output(["git", "show", f"{original}:{name}"], cwd=ROOT, text=True)
                after = subprocess.check_output(["git", "show", f"{release}:{name}"], cwd=ROOT, text=True)
                expected = re.sub(version_pattern(name),
                                  lambda item: item[1] + version + item[3], before)
                if expected != after:
                    raise RuntimeError("Release commit must contain only version replacements")
        if source not in (original, release):
            raise RuntimeError("Requested source differs from the existing canonical release")
        source = original
        git("checkout", "--detach", release)
        if set(versions(ROOT).values()) != {version}:
            raise RuntimeError("Existing release tag has inconsistent component versions")
    else:
        if git("rev-parse", "refs/remotes/origin/main") != source:
            raise RuntimeError("A new release must start at the current origin/main commit")
        set_versions(ROOT, version)
        if git("diff", "--name-only", "HEAD"):
            git("config", "user.name", "github-actions[bot]")
            git("config", "user.email", "github-actions[bot]@users.noreply.github.com")
            git("add", "--", *VERSION_FILES)
            git("commit", "-m", f"chore(release): v{version}", "-m",
                f"Release-Source: {source}")
    output(path, {"version": version, "source_sha": source, "release_sha": git("rev-parse", "HEAD"), "reused": reused,
                  "component_versions": versions(ROOT)})


def commit(version, prepared, path):
    state = json.loads(prepared.read_text())
    tag = f"v{version}"
    if state["version"] != version or git("rev-parse", "HEAD") != state["release_sha"]:
        raise RuntimeError("Prepared release identity changed during validation")
    if git("status", "--porcelain"):
        raise RuntimeError("Validation changed the prepared source checkout")
    if set(versions(ROOT).values()) != {version}:
        raise RuntimeError("All components must have the requested committed version")
    if state["reused"]:
        release = git("rev-parse", "HEAD")
        remote = git("ls-remote", "origin", f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}")
        remote_sha = remote.splitlines()[-1].split()[0] if remote else None
        if release != remote_sha:
            raise RuntimeError("Existing release tag moved during validation")
    else:
        remote = git("ls-remote", "origin", "refs/heads/main").split()[0]
        if remote != state["source_sha"]:
            raise RuntimeError("origin/main advanced during validation; refusing to publish")
        if git("ls-remote", "origin", f"refs/tags/{tag}"):
            raise RuntimeError("Release tag appeared during validation; rerun to verify it")
        release = git("rev-parse", "HEAD")
        git("tag", tag, release)
        git("push", "--atomic", "origin", "HEAD:refs/heads/main", f"refs/tags/{tag}")
    output(path, {**state, "release_sha": release})


def validate(version, path):
    if git("status", "--porcelain") or set(versions(ROOT).values()) != {version}:
        raise RuntimeError("Publishers require clean, synchronized committed versions")
    release = git("rev-parse", "HEAD")
    if release != git("rev-parse", f"refs/tags/v{version}^{{commit}}"):
        raise RuntimeError("Publishers require the canonical release tag")
    output(path, {"version": version, "release_sha": release})


def image_status(image, version, path):
    if not image.startswith("ghcr.io/"):
        raise RuntimeError("Only the configured GHCR image is supported")
    repository = image.removeprefix("ghcr.io/")
    with urllib.request.urlopen(
        f"https://ghcr.io/token?service=ghcr.io&scope=repository:{repository}:pull", timeout=30
    ) as response:
        token = json.load(response)["token"]
    request = urllib.request.Request(
        f"https://ghcr.io/v2/{repository}/manifests/{version}",
        headers={"Authorization": f"Bearer {token}", "Accept":
                 "application/vnd.oci.image.index.v1+json, "
                 "application/vnd.docker.distribution.manifest.list.v2+json, "
                 "application/vnd.oci.image.manifest.v1+json, "
                 "application/vnd.docker.distribution.manifest.v2+json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read(1024 * 1024)
            digest = response.headers["Docker-Content-Digest"]
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise RuntimeError("Registry returned an invalid image digest")
        result = {"status": "reused", "digest": digest}
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        result = {"status": "absent"}
    path.write_text(json.dumps(result, indent=2) + "\n")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
            stream.write(f"status={result['status']}\n")
            stream.write(f"digest={result.get('digest', '')}\n")


def smoke(image, version, source, path):
    name = "ess-release-" + uuid.uuid4().hex
    env = dict(os.environ, ESS_MASTER_KEY=base64.b64encode(secrets.token_bytes(32)).decode(),
               SECRET_KEY_BASE=base64.b64encode(secrets.token_bytes(64)).decode(),
               ESS_ADMIN_PASSWORD_HASH=hashlib.sha256(secrets.token_bytes(32)).hexdigest())
    args = ["docker", "run", "--detach", "--name", name, "--network", "none",
            "--env", "ESS_MASTER_KEY", "--env", "SECRET_KEY_BASE",
            "--env", "ESS_ADMIN_PASSWORD_HASH", "--env", "ESS_S3_AUTH_ENABLED=true",
            "--env", "ESS_DATA_ROOT=/tmp/ess-release-data", image]
    subprocess.run(args, env=env, check=True)
    try:
        for _attempt in range(45):
            results = [subprocess.run(["docker", "exec", name, "wget", "-qO-",
                                        "http://127.0.0.1:9000" + route], capture_output=True)
                       for route in ("/health", "/health/ready")]
            if all(result.returncode == 0 for result in results):
                break
            time.sleep(2)
        else:
            subprocess.run(["docker", "logs", name], check=False)
            raise RuntimeError("Built image did not pass liveness and readiness")
        expression = (
            'versions = Map.new([:ex_storage_service, :ex_storage_service_cluster, '
            ':ex_storage_service_s3, :ex_storage_service_web], '
            'fn app -> {app, to_string(Application.spec(app, :vsn))} end); '
            'IO.puts("ESS_RELEASE_METADATA:" <> JSON.encode!(%{versions: versions, '
            'build: ExStorageServiceWeb.Layouts.version_info()}))')
        result = subprocess.check_output(["docker", "exec", name, "/app/bin/ess", "rpc",
                                          expression], text=True)
        line = next(line for line in result.splitlines() if line.startswith("ESS_RELEASE_METADATA:"))
        metadata = json.loads(line.removeprefix("ESS_RELEASE_METADATA:"))
        if set(metadata["versions"].values()) != {version}:
            raise RuntimeError("Image contains inconsistent component versions")
        build = metadata["build"]
        if (build["version"] != version or build["git_ref"] != f"v{version}"
                or build["git_sha"] != source):
            raise RuntimeError("Image build identity differs from the validated source")
        if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", build["build_time"]):
            raise RuntimeError("Image has no UTC build-start timestamp")
        path.write_text(json.dumps(metadata, indent=2) + "\n")
    finally:
        subprocess.run(["docker", "rm", "--force", "--volumes", name], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "commit", "validate", "image-status", "smoke"))
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prepared", type=Path)
    parser.add_argument("--image")
    parser.add_argument("--source")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?", args.version):
        parser.error("Invalid release version")
    if args.command == "prepare":
        prepare(args.version, args.output)
    elif args.command == "commit":
        commit(args.version, args.prepared, args.output)
    elif args.command == "validate":
        validate(args.version, args.output)
    elif args.command == "image-status":
        image_status(args.image, args.version, args.output)
    else:
        smoke(args.image, args.version, args.source, args.output)


if __name__ == "__main__":
    main()
