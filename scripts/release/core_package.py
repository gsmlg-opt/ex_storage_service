#!/usr/bin/env python3
"""Verify a core or SDK source tar and compile it as an isolated standalone consumer."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/cli_compatibility"))
from hex_package import PACKAGE_LIMIT, read_package


def entries(contents):
    files = {}
    total = 0
    with tarfile.open(fileobj=io.BytesIO(contents), mode="r:gz") as archive:
        for member in archive:
            if member.isdir():
                continue
            name = Path(member.name)
            total += member.size
            if (not member.isfile() or name.is_absolute() or ".." in name.parts
                    or str(name) != member.name or member.name in files or total > PACKAGE_LIMIT):
                raise RuntimeError("Invalid or oversized core package source entry")
            files[member.name] = archive.extractfile(member).read()
    return files


# Package contents per app; the SDK also ships the S3 model it compiles from.
PACKAGE_PATHS = {
    "ex_storage_service": ("lib", "mix.exs", "README.md", "LICENSE"),
    "ex_storage_service_sdk": ("lib", "mix.exs", "README.md", "LICENSE", "priv/compatibility"),
}


def manifest(package, version, output, app="ex_storage_service"):
    checksum, contents = read_package(package)
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    files = entries(contents)
    expected_files = subprocess.check_output(
        ["git", "ls-tree", "-r", "--name-only", source, "--",
         *(f"apps/{app}/{path}" for path in PACKAGE_PATHS[app])], cwd=ROOT, text=True).splitlines()
    expected_files = {name.removeprefix(f"apps/{app}/") for name in expected_files}
    if set(files) != expected_files or not files:
        raise RuntimeError(f"{app} package omits committed files or contains unexpected files")
    for name, data in files.items():
        expected = subprocess.check_output(
            ["git", "show", f"{source}:apps/{app}/{name}"], cwd=ROOT)
        if data != expected:
            raise RuntimeError(f"{app} package differs from committed source: {name}")
    match = re.search(rb'(?:@version\s+|version:\s*)"([^"]+)"', files["mix.exs"])
    if not match or match[1].decode() != version:
        raise RuntimeError("Core package version differs from requested version")
    output.write_text(json.dumps({"version": version, "source_sha": source,
                                  "hex_inner_checksum": checksum,
                                  "package_sha256": hashlib.sha256(package.read_bytes()).hexdigest(),
                                  "source_files_compared": len(files)}, indent=2) + "\n")


SDK_CONSUMER_MIX = """defmodule SdkConsumer.MixProject do
  use Mix.Project

  def project,
    do: [app: :sdk_consumer, version: "0.1.0", elixir: ">= 1.18.0",
         deps: [{:ex_storage_service_sdk, path: "../source"}]]

  def application, do: [extra_applications: [:logger]]
end
"""


def sdk_smoke(version):
    # Starts only the SDK and its dependencies, builds a client and resolves a
    # request against a closed local port: no server, storage engine or CLI.
    return ('{:ok, _} = Application.ensure_all_started(:ex_storage_service_sdk); '
            f'"{version}" = to_string(Application.spec(:ex_storage_service_sdk, :vsn)); '
            'for m <- [ExStorageService.Storage.Engine, ExStorageServiceCli, ExStorageServiceS3.Router], '
            'do: if(Code.ensure_loaded?(m), do: raise("unexpected #{inspect(m)}")); '
            '{:ok, s3} = ExStorageServiceSdk.new(endpoint: "http://127.0.0.1:9", '
            'access_key_id: "AKIDEXAMPLE", secret_access_key: "secret"); '
            '{:error, %ExStorageServiceSdk.Error{category: :transport}} = '
            'ExStorageServiceSdk.head_object(s3, "bucket", "a b+c%", %{}, max_attempts: 1); '
            'IO.puts("standalone-sdk-consumer-ok")')


def consumer(package, version, output, app="ex_storage_service"):
    output.mkdir()
    checksum, contents = read_package(package)
    source = output / "source"
    source.mkdir()
    for name, data in entries(contents).items():
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    env = dict(os.environ, MIX_ENV="prod", MIX_BUILD_PATH=str(output / "build"),
               MIX_DEPS_PATH=str(output / "deps"))
    if app == "ex_storage_service_sdk":
        # A separate application depends on the package, as a Hex consumer would.
        workdir = output / "app"
        workdir.mkdir()
        (workdir / "mix.exs").write_text(SDK_CONSUMER_MIX)
        commands = [["mix", "deps.get", "--only", "prod"],
                    ["mix", "compile", "--warnings-as-errors"],
                    ["mix", "run", "--no-start", "-e", sdk_smoke(version)]]
    else:
        workdir = source
        commands = [["mix", "deps.get", "--only", "prod"],
                    ["mix", "compile", "--warnings-as-errors"],
                    ["mix", "run", "--no-start", "-e",
                     'Application.load(:ex_storage_service); '
                     f'"{version}" = to_string(Application.spec(:ex_storage_service, :vsn)); '
                     'Code.ensure_loaded!(ExStorageService.Storage.Engine); '
                     'if Code.ensure_loaded?(ExStorageServiceWeb.Endpoint), do: raise("server dependency"); '
                     'IO.puts("standalone-core-consumer-ok")']]
    results = []

    def save_results():
        (output / "commands.json").write_text(json.dumps(results, indent=2) + "\n")

    with (output / "consumer.log").open("w") as log:
        try:
            for command in commands:
                result = {"argv": command, "status": None, "output": ""}
                results.append(result)
                save_results()
                header = f"Running {json.dumps(command)} (cwd={workdir})\n"
                log.write(header)
                log.flush()
                print(header, end="", flush=True)
                chunks = []
                with subprocess.Popen(command, cwd=workdir, env=env, text=True, bufsize=1,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
                    try:
                        for line in process.stdout:
                            chunks.append(line)
                            log.write(line)
                            log.flush()
                            print(line, end="", flush=True)
                        result["status"] = process.wait()
                    except BaseException:
                        process.kill()
                        result["status"] = process.wait()
                        raise
                    finally:
                        result["output"] = "".join(chunks)
                        save_results()
                if result["status"]:
                    raise RuntimeError("Fresh core consumer failed" if app == "ex_storage_service" else "Fresh SDK consumer failed")
        finally:
            save_results()
    (output / "manifest.json").write_text(json.dumps(
        {"version": version, "hex_inner_checksum": checksum, "commands": len(results)}, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("manifest", "consumer"))
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--app", choices=tuple(PACKAGE_PATHS), default="ex_storage_service")
    args = parser.parse_args()
    (manifest if args.command == "manifest" else consumer)(
        args.package, args.version, args.output, args.app)


if __name__ == "__main__":
    main()
