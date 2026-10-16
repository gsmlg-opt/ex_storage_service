#!/usr/bin/env python3
"""Capture the pinned AWS executable against loopback peers; never contacts AWS."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import struct
import subprocess
import tempfile
import threading
from urllib.parse import quote
from xml.sax.saxutils import escape, quoteattr
import xml.etree.ElementTree as ElementTree
import zlib

ROOT = Path(__file__).resolve().parents[3]
AWS_SHA256 = "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"
NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"
VOLATILE_HEADERS = {"authorization", "x-amz-date", "host", "user-agent", "amz-sdk-invocation-id", "amz-sdk-request", "connection", "accept-encoding"}


def canonical_xml(body):
    def node(element):
        children = {}
        for child in element:
            children.setdefault(child.tag.split("}")[-1], []).append(node(child))
        return {"name": element.tag.split("}")[-1],
                "attributes": {key.split("}")[-1]: value for key, value in element.attrib.items()},
                "text": element.text or "", "children": children}

    return node(ElementTree.fromstring(body))


class Peer:
    def __init__(self, response):
        self.response = response
        self.requests, self.errors = [], []
        self.socket = socket.socket()
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen()
        self.socket.settimeout(0.05)
        self.endpoint = "http://127.0.0.1:" + str(self.socket.getsockname()[1])
        self.done = False
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self):
        while not self.done:
            try:
                connection, _ = self.socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                connection.settimeout(5)
                data = b""
                while b"\r\n\r\n" not in data:
                    chunk = connection.recv(65536)
                    if not chunk:
                        raise ValueError("connection closed before headers")
                    data += chunk
                head, body = data.split(b"\r\n\r\n", 1)
                line, *fields = head.decode().split("\r\n")
                method, target, _ = line.split(" ")
                headers = {key.lower(): value.strip() for key, value in (field.split(":", 1) for field in fields)}
                if headers.get("expect", "").lower() == "100-continue":
                    connection.sendall(b"HTTP/1.1 100 Continue\r\n\r\n")
                length = int(headers.get("content-length", "0"))
                while len(body) < length:
                    chunk = connection.recv(min(65536, length - len(body)))
                    if not chunk:
                        raise ValueError("connection closed before body")
                    body += chunk
                if "transfer-encoding" in headers:
                    raise ValueError("chunked requests require a separately reviewed capture")
                self.requests.append({"method": method, "target": target,
                    "headers": {key: value for key, value in headers.items() if key not in VOLATILE_HEADERS},
                    "body_base64": base64.b64encode(body).decode()})
                if body.startswith(b"<"):
                    self.requests[-1]["xml_semantics"] = canonical_xml(body)
                response = self.response(self.requests[-1], len(self.requests)) if callable(self.response) else self.response
                body = base64.b64decode(response["body_base64"])
                fields = dict(response["headers"])
                fields.setdefault("content-length", str(len(body)))
                fields["connection"] = "close"
                head = "HTTP/1.1 " + str(response["status"]) + " Fixture\r\n"
                head += "".join(key + ": " + value + "\r\n" for key, value in fields.items()) + "\r\n"
                connection.sendall(head.encode() + (b"" if method == "HEAD" else body))
            except Exception as error:
                self.errors.append(str(error))
            finally:
                connection.close()

    def close(self):
        self.done = True
        self.socket.close()
        self.thread.join(2)


def sample(shapes, name, field="", depth=0, output=False):
    shape = shapes[name]
    kind = shape["type"]
    if kind == "structure":
        members = shape.get("members", {})
        selected = list(members) if output and depth < 3 else shape.get("required", [])
        if not selected and depth < 3:
            selected = list(members)[:2]
        return {key: sample(shapes, members[key]["shape"], key, depth + 1, output) for key in selected}
    if kind == "list":
        return [sample(shapes, shape["member"]["shape"], field, depth + 1, output)]
    if kind == "map":
        return {"fixture": sample(shapes, shape["value"]["shape"], field, depth + 1, output)}
    if kind == "boolean":
        return False if field in ("IsTruncated", "DeleteMarker") else True
    if kind in ("integer", "long", "float", "double"):
        return max(1, shape.get("min", 1))
    if kind == "timestamp":
        return "2026-01-02T03:04:05Z"
    if kind == "blob":
        return "YWJj"
    if shape.get("enum"):
        return shape["enum"][0]
    overrides = {"Bucket": "bucket", "Key": "matrix/% +?#/雪/./../key", "CopySource": "source/old% +?#/雪",
        "RenameSource": "source/old", "UploadId": "upload+/=id", "Id": "config+ id", "RequestRoute": "fixture",
        "RequestToken": "fixture-token", "ExpectedBucketOwner": "123456789012", "AccountId": "123456789012",
        "Policy": '{"Version":"2012-10-17","Statement":[]}', "Expression": "SELECT * FROM S3Object",
        "Expires": "Fri, 02 Jan 2026 03:04:05 GMT",
        "AccessKeyId": "FIXTUREACCESS", "SecretAccessKey": "fixture-session-secret", "SessionToken": "fixture-session-token"}
    value = overrides.get(field, "fixture & 雪")
    return value if len(value) >= shape.get("min", 0) else "x" * shape["min"]


def xml(shapes, reference, value, name):
    shape = shapes[reference["shape"]]
    kind = shape["type"]
    if kind == "list":
        member = shape["member"]
        if shape.get("flattened") or reference.get("flattened"):
            return "".join(xml(shapes, member, item, name) for item in value)
        return "<" + name + ">" + "".join(xml(shapes, member, item, member.get("locationName", "member")) for item in value) + "</" + name + ">"
    if kind == "structure":
        attributes = ""
        children = ""
        for key, item in value.items():
            member = shape["members"][key]
            if member.get("location"):
                continue
            child_name = member.get("locationName", key)
            if member.get("xmlAttribute"):
                if ":" in child_name:
                    attributes += ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
                attributes += " " + child_name + "=" + quoteattr(str(item))
            else:
                children += xml(shapes, member, item, child_name)
        return "<" + name + attributes + ">" + children + "</" + name + ">"
    if kind == "map":
        children = "".join("<entry><key>" + escape(key) + "</key>" + xml(shapes, shape["value"], item, "value") + "</entry>" for key, item in value.items())
        return "<" + name + ">" + children + "</" + name + ">"
    value = str(value).lower() if isinstance(value, bool) else str(value)
    return "<" + name + ">" + escape(value) + "</" + name + ">"


def event(event_type, payload=b""):
    headers = b""
    for key, value in [(":message-type", "event"), (":event-type", event_type), (":content-type", "application/octet-stream")]:
        key, value = key.encode(), value.encode()
        headers += bytes([len(key)]) + key + b"\x07" + struct.pack(">H", len(value)) + value
    prelude = struct.pack(">II", 16 + len(headers) + len(payload), len(headers))
    message = prelude + struct.pack(">I", zlib.crc32(prelude)) + headers + payload
    return message + struct.pack(">I", zlib.crc32(message))


def response_for(model, operation, denied):
    status = 403 if denied else operation["http"].get("responseCode", 200)
    headers = {"x-amz-request-id": "fixture-request", "content-type": "application/xml"}
    if denied:
        body = b"<Error><Code>AccessDenied</Code><Message>Denied by fixture</Message><RequestId>fixture-request</RequestId></Error>"
    else:
        reference = operation.get("output")
        shape = model["shapes"].get(reference["shape"], {}) if reference else {}
        values = sample(model["shapes"], reference["shape"], output=True) if reference else {}
        if values.get("EncodingType") == "url":
            encoded_fields = {"Key", "Prefix", "Delimiter", "Marker", "NextMarker", "StartAfter", "KeyMarker", "NextKeyMarker"}

            def encode_keys(value):
                if isinstance(value, list):
                    return [encode_keys(item) for item in value]
                if isinstance(value, dict):
                    return {key: quote(item, safe="") if key in encoded_fields and isinstance(item, str) else encode_keys(item) for key, item in value.items()}
                return value

            values = encode_keys(values)
        body = b""
        if operation["name"] in ("GetObject", "GetObjectTorrent"):
            body = bytes(range(256))
            headers = {"content-type": "application/octet-stream", "etag": '"fixture-etag"', "x-amz-version-id": "fixture-version"}
        elif operation["name"] == "SelectObjectContent":
            body = event("Records", b"fixture,1\n") + event("End")
            headers["content-type"] = "application/vnd.amazon.eventstream"
        elif shape.get("payload") and model["shapes"][shape["members"][shape["payload"]]["shape"]]["type"] == "string":
            body = b'{"Version":"2012-10-17","Statement":[]}'
        elif shape.get("payload"):
            payload = shape["payload"]
            body = xml(model["shapes"], shape["members"][payload], values[payload], payload).encode()
        elif operation["name"] == "GetBucketLocation":
            body = b'<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">eu-west-1</LocationConstraint>'
        elif operation["name"] == "GetBucketNotification":
            body = b'<NotificationConfiguration><TopicConfiguration><Id>legacy</Id><Event>s3:ReducedRedundancyLostObject</Event><Topic>arn:aws:sns:us-east-1:123456789012:fixture</Topic></TopicConfiguration></NotificationConfiguration>'
        elif shape.get("members"):
            body = xml(model["shapes"], reference, values, reference.get("locationName", reference["shape"])).encode()
        for name, reference in shape.get("members", {}).items():
            location = reference.get("location")
            if location != "header" or name not in values:
                continue
            value = values[name]
            if isinstance(value, (dict, list)):
                continue
            if model["shapes"][reference["shape"]]["type"] == "timestamp":
                value = "2026-01-02T03:04:05Z" if model["shapes"][reference["shape"]].get("timestampFormat") == "iso8601" else "Fri, 02 Jan 2026 03:04:05 GMT"
            if "checksum" in reference.get("locationName", "").lower():
                continue
            value = str(value).lower() if isinstance(value, bool) else str(value)
            if not value.isascii():
                value = "fixture"
            headers.setdefault(reference["locationName"].lower(), value)
    if operation["http"]["method"] == "HEAD":
        body = b""
    headers["content-length"] = str(len(body))
    return {"status": status, "headers": headers, "body_base64": base64.b64encode(body).decode()}


def run(program, args, response, env, directory, ess=False, paginate=False):
    peer = Peer(response)
    command = (["escript", program] if ess else [program]) + ["--endpoint-url", peer.endpoint, "--region", "us-east-1", "--no-cli-pager", "--cli-connect-timeout", "1", "--cli-read-timeout", "2"]
    if not paginate:
        command.append("--no-paginate")
    try:
        command += [argument.replace("@TMP@", str(directory)) for argument in args]
        result = subprocess.run(command, env=env, capture_output=True, timeout=15)
        record = {"status": result.returncode, "stdout": result.stdout.decode(errors="replace"), "stderr": result.stderr.decode(errors="replace")}
    except subprocess.TimeoutExpired:
        record = {"timeout": True}
    finally:
        peer.close()
    record.update(requests=peer.requests, peer_errors=peer.errors)
    output = directory / "result.bin"
    if output.exists():
        record["outfile_base64"] = base64.b64encode(output.read_bytes()).decode()
        output.unlink()
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", required=True)
    parser.add_argument("--ess", help="optional private escript for initial differential diagnostics")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--only", nargs="*")
    parser.add_argument("--extra-list-encoding", action="store_true")
    args = parser.parse_args()
    assert hashlib.sha256(Path(args.aws).read_bytes()).hexdigest() == AWS_SHA256, "Unpinned AWS executable"
    model_path = ROOT / "priv/compatibility/aws/service-2.json"
    model = json.loads(model_path.read_text())
    report = {"schema_version": 1, "aws_sha256": AWS_SHA256, "model_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(), "cases": [], "exclusions": []}
    if args.ess:
        report["initial_ess_sha256"] = hashlib.sha256(Path(args.ess).read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory(prefix="ess-operations-oracle-") as temporary:
        directory = Path(temporary)
        env = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "ESS_", "MC_"))}
        env.update(AWS_CONFIG_FILE="/dev/null", AWS_SHARED_CREDENTIALS_FILE="/dev/null", AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret", AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="1", AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off", AWS_RESPONSE_CHECKSUM_VALIDATION="when_required", ESS_CONFIG_DIR=str(directory / "config"), ESS_STATE_DIR=str(directory / "state"), ERL_FLAGS="+S 2:2", NO_PROXY="*", PAGER="cat")
        (directory / "body.bin").write_bytes(b"fixture\x00\xff")
        for name, operation in model["operations"].items():
            if args.only and name not in args.only:
                continue
            if name == "WriteGetObjectResponse":
                report["exclusions"].append({"operation": name, "reason": "AWS host-prefix injection requires a controlled DNS/TLS host fixture; not safely represented by a loopback IP endpoint"})
                continue
            shape = model["shapes"][operation["input"]["shape"]]
            document = sample(model["shapes"], operation["input"]["shape"])
            for key, value in [("ExpectedBucketOwner", "123456789012"), ("RequestPayer", "requester")]:
                if key in shape["members"]:
                    document[key] = value
            payload = shape.get("payload")
            if payload and payload not in document:
                document[payload] = sample(model["shapes"], shape["members"][payload]["shape"], payload)
            if name == "SelectObjectContent":
                document.update(InputSerialization={"CSV": {"FileHeaderInfo": "NONE"}}, OutputSerialization={"CSV": {}})
            if name == "RenameObject":
                document["ClientToken"] = "00000000-0000-4000-8000-000000000001"
            cli_name = re.sub(r"(?<!^)(?=[A-Z][a-z])|(?<=[a-z0-9])(?=[A-Z])", "-", name).lower()
            command = ["s3api", cli_name, "--cli-input-json", json.dumps(document, ensure_ascii=False)]
            if "Body" in shape["members"] and shape["members"]["Body"].get("streaming"):
                document.pop("Body", None)
                command = ["s3api", cli_name, "--cli-input-json", json.dumps(document, ensure_ascii=False), "--body", "@TMP@/body.bin"]
            if name in ("GetObject", "GetObjectTorrent", "SelectObjectContent"):
                # AWS removes --cli-input-json from these customized commands.
                command = ["s3api", cli_name]
                for required, value in document.items():
                    flag = re.sub(r"(?<!^)(?=[A-Z][a-z])|(?<=[a-z0-9])(?=[A-Z])", "-", required).lower()
                    command += ["--" + flag, json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)]
                command.append("@TMP@/result.bin")
            for denied in (False, True):
                response = response_for(model, operation, denied)
                case = {"operation": name, "scenario": "access-denied" if denied else "success", "args": command, "response": response, "aws": run(args.aws, command, response, env, directory)}
                if args.ess:
                    case["initial_ess"] = run(args.ess, command, response, env, directory, ess=True)
                report["cases"].append(case)
                print(name, case["scenario"], "aws=" + str(case["aws"].get("status", "timeout")), "ess=" + str(case.get("initial_ess", {}).get("status", "not-run")), "requests=" + str(len(case["aws"]["requests"])), flush=True)
                args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
            if name == "RenameObject":
                success = response_for(model, operation, False)
                transient_body = b"<Error><Code>SlowDown</Code><Message>Retry fixture</Message></Error>"
                transient = {"status": 503, "headers": {"content-type": "application/xml"}, "body_base64": base64.b64encode(transient_body).decode()}
                for generated, retry in [(True, False), (True, True), (False, True)]:
                    selected = dict(document)
                    if generated:
                        selected.pop("ClientToken")
                    selected_command = ["s3api", cli_name, "--cli-input-json", json.dumps(selected, ensure_ascii=False)]
                    responses = [transient, success] if retry else [success]
                    response = lambda _request, index: responses[min(index - 1, len(responses) - 1)]
                    environment = {"AWS_MAX_ATTEMPTS": "2" if retry else "1", "AWS_RETRY_MODE": "standard"}
                    selected_env = dict(env, **environment)
                    scenario = ("retry-" if retry else "") + ("omitted-client-token" if generated else "explicit-client-token")
                    case = {"operation": name, "scenario": scenario, "args": selected_command, "response": success,
                        "responses": responses, "environment": environment, "generated_client_token": generated,
                        "aws": run(args.aws, selected_command, response, selected_env, directory)}
                    if args.ess:
                        case["initial_ess"] = run(args.ess, selected_command, response, selected_env, directory, ess=True)
                    tokens = [request["headers"].get("x-amz-client-token") for request in case["aws"]["requests"]]
                    assert len(tokens) == len(responses) and len(set(tokens)) == 1, tokens
                    if generated:
                        assert re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", tokens[0]), tokens
                    else:
                        assert tokens[0] == selected["ClientToken"]
                    assert case["aws"]["status"] == 0 and not case["aws"]["peer_errors"], case["aws"]
                    report["cases"].append(case)
                    print(name, scenario, "aws=" + str(case["aws"]["status"]), "requests=" + str(len(tokens)), flush=True)
                    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
            if args.extra_list_encoding and name in ("ListObjects", "ListObjectsV2", "ListObjectVersions"):
                fields = '<Name>bucket</Name><IsTruncated>false</IsTruncated><EncodingType>url</EncodingType><Prefix>prefix%252F%2B%2F%E9%9B%AA</Prefix><Delimiter>%2F</Delimiter><CommonPrefixes><Prefix>common%252F%2B%2F</Prefix></CommonPrefixes>'
                key = '<Key>key%252F%2B%20%E9%9B%AA</Key><LastModified>2026-01-02T03:04:05Z</LastModified>'
                if name == "ListObjectVersions":
                    fields += '<KeyMarker>marker%252F%2B</KeyMarker><VersionIdMarker>opaque%252F+==</VersionIdMarker><NextKeyMarker>next%252F%2B</NextKeyMarker><NextVersionIdMarker>next-opaque%252F+==</NextVersionIdMarker><Version>' + key + '<VersionId>version%252F+==</VersionId></Version><DeleteMarker>' + key + '<VersionId>deleted%252F+==</VersionId></DeleteMarker>'
                else:
                    fields += '<Contents>' + key + '<Size>3</Size></Contents>'
                    fields += '<Marker>marker%252F%2B</Marker><NextMarker>next%252F%2B</NextMarker>' if name == "ListObjects" else '<StartAfter>after%252F%2B</StartAfter><ContinuationToken>opaque%252F+==</ContinuationToken><NextContinuationToken>next-opaque%252F+==</NextContinuationToken>'
                body = ('<ListResult xmlns="' + NAMESPACE + '">' + fields + '</ListResult>').encode()
                response = {"status": 200, "headers": {"content-type": "application/xml"}, "body_base64": base64.b64encode(body).decode()}
                for explicit in (False, True):
                    selected = command + (["--encoding-type", "url"] if explicit else [])
                    case = {"operation": name, "scenario": "literal-encoding-explicit-url" if explicit else "literal-encoding-auto", "args": selected, "response": response, "aws": run(args.aws, selected, response, env, directory)}
                    if args.ess:
                        case["initial_ess"] = run(args.ess, selected, response, env, directory, ess=True)
                    report["cases"].append(case)
                    print(name, case["scenario"], "aws=" + str(case["aws"].get("status")), flush=True)
                    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    # Exclusions can follow the final captured operation; persist the final audit.
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
