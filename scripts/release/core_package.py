#!/usr/bin/env python3
"""Verify a core source tar and compile it as an isolated standalone consumer."""

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


def manifest(package, version, output):
    checksum, contents = read_package(package)
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    files = entries(contents)
    expected_files = subprocess.check_output(
        ["git", "ls-tree", "-r", "--name-only", source, "--", "apps/ex_storage_service/lib",
         "apps/ex_storage_service/mix.exs", "apps/ex_storage_service/README.md",
         "apps/ex_storage_service/LICENSE"], cwd=ROOT, text=True).splitlines()
    expected_files = {name.removeprefix("apps/ex_storage_service/") for name in expected_files}
    if set(files) != expected_files or not files:
        raise RuntimeError("Core package omits committed files or contains unexpected files")
    for name, data in files.items():
        expected = subprocess.check_output(
            ["git", "show", f"{source}:apps/ex_storage_service/{name}"], cwd=ROOT)
        if data != expected:
            raise RuntimeError(f"Core package differs from committed source: {name}")
    match = re.search(rb'version:\s*"([^"]+)"', files["mix.exs"])
    if not match or match[1].decode() != version:
        raise RuntimeError("Core package version differs from requested version")
    output.write_text(json.dumps({"version": version, "source_sha": source,
                                  "hex_inner_checksum": checksum,
                                  "package_sha256": hashlib.sha256(package.read_bytes()).hexdigest(),
                                  "source_files_compared": len(files)}, indent=2) + "\n")


def consumer(package, version, output):
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
                header = f"Running {json.dumps(command)} (cwd={source})\n"
                log.write(header)
                log.flush()
                print(header, end="", flush=True)
                chunks = []
                with subprocess.Popen(command, cwd=source, env=env, text=True, bufsize=1,
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
                    raise RuntimeError("Fresh core consumer failed")
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
    args = parser.parse_args()
    (manifest if args.command == "manifest" else consumer)(args.package, args.version, args.output)


if __name__ == "__main__":
    main()
