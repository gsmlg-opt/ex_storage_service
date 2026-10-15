#!/usr/bin/env python3
"""Capture explicit optional-member variants from locked AWS CLI on loopback only."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

from capture_operations import AWS_SHA256, ROOT, run


def response(body="", status=200, headers=None):
    fields = {"content-type": "application/xml", "x-amz-request-id": "optional-request",
              "x-amz-id-2": "optional-host-id", **(headers or {})}
    return {"status": status, "headers": fields,
            "body_base64": base64.b64encode(body.encode() if isinstance(body, str) else body).decode()}


def variants():
    cases = []

    def add(operation, scenario, document, reply, body=False):
        name = re.sub(r"(?<!^)(?=[A-Z][a-z])|(?<=[a-z0-9])(?=[A-Z])", "-", operation).lower()
        args = ["s3api", name, "--cli-input-json", json.dumps(document, ensure_ascii=False)]
        if body:
            args += ["--body", "@TMP@/body.bin"]
        if operation == "GetObject":
            args = ["s3api", name]
            for key, value in document.items():
                flag = re.sub(r"(?<!^)(?=[A-Z][a-z])|(?<=[a-z0-9])(?=[A-Z])", "-", key).lower()
                args += ["--" + flag, str(value)]
            args.append("@TMP@/result.bin")
        cases.append({"operation": operation, "scenario": scenario,
                      "input": document, "args": args, "response": reply})

    def pair(suffix, field, document, wire, put_headers=None, get_headers=None):
        add("PutBucket" + suffix, "expanded", {"Bucket": "bucket", field: document,
            "ExpectedBucketOwner": "123456789012", **(put_headers or {})}, response())
        add("GetBucket" + suffix, "expanded", {"Bucket": "bucket",
            "ExpectedBucketOwner": "123456789012"}, response(wire, headers=get_headers))

    pair("LifecycleConfiguration", "LifecycleConfiguration", {"Rules": [
        {"ID": "archive & expire", "Status": "Enabled", "Filter": {"And": {
            "Prefix": "archive/雪/", "Tags": [{"Key": "team", "Value": "storage"},
            {"Key": "empty", "Value": ""}], "ObjectSizeGreaterThan": 0,
            "ObjectSizeLessThan": 1048576}}, "Expiration": {"Days": 365},
         "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"},
                         {"Days": 90, "StorageClass": "GLACIER"}],
         "NoncurrentVersionTransitions": [{"NoncurrentDays": 45, "NewerNoncurrentVersions": 2,
                                            "StorageClass": "GLACIER"}],
         "NoncurrentVersionExpiration": {"NoncurrentDays": 400, "NewerNoncurrentVersions": 3},
         "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}},
        {"ID": "dated", "Status": "Disabled", "Filter": {"Prefix": ""},
         "Expiration": {"Date": "2030-01-02T00:00:00Z"}}]},
        '<LifecycleConfiguration><Rule><ID>archive &amp; expire</ID><Filter><And><Prefix>archive/雪/</Prefix><Tag><Key>team</Key><Value>storage</Value></Tag><Tag><Key>empty</Key><Value/></Tag><ObjectSizeGreaterThan>0</ObjectSizeGreaterThan><ObjectSizeLessThan>1048576</ObjectSizeLessThan></And></Filter><Status>Enabled</Status><Expiration><Days>365</Days></Expiration><Transition><Days>30</Days><StorageClass>STANDARD_IA</StorageClass></Transition><Transition><Days>90</Days><StorageClass>GLACIER</StorageClass></Transition><NoncurrentVersionTransition><NoncurrentDays>45</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions><StorageClass>GLACIER</StorageClass></NoncurrentVersionTransition><NoncurrentVersionExpiration><NoncurrentDays>400</NoncurrentDays><NewerNoncurrentVersions>3</NewerNoncurrentVersions></NoncurrentVersionExpiration><AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule><Rule><ID>dated</ID><Status>Disabled</Status><Filter><Prefix/></Filter><Expiration><Date>2030-01-02T00:00:00Z</Date></Expiration></Rule></LifecycleConfiguration>',
        {"TransitionDefaultMinimumObjectSize": "all_storage_classes_128K"},
        {"x-amz-transition-default-minimum-object-size": "all_storage_classes_128K"})

    pair("Replication", "ReplicationConfiguration", {"Role": "arn:aws:iam::123456789012:role/replicator",
        "Rules": [{"ID": "replica", "Priority": 7, "Status": "Enabled",
            "Filter": {"And": {"Prefix": "source/", "Tags": [{"Key": "team", "Value": "storage"},
                                                              {"Key": "empty", "Value": ""}]}},
            "DeleteMarkerReplication": {"Status": "Disabled"},
            "SourceSelectionCriteria": {"SseKmsEncryptedObjects": {"Status": "Enabled"},
                                         "ReplicaModifications": {"Status": "Enabled"}},
            "Destination": {"Bucket": "arn:aws:s3:::replica-bucket", "Account": "123456789013",
                "StorageClass": "STANDARD_IA", "AccessControlTranslation": {"Owner": "Destination"},
                "EncryptionConfiguration": {"ReplicaKmsKeyID": "arn:aws:kms:us-east-1:123456789013:key/fixture"},
                "ReplicationTime": {"Status": "Enabled", "Time": {"Minutes": 15}},
                "Metrics": {"Status": "Enabled", "EventThreshold": {"Minutes": 15}}}}]},
        '<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/replicator</Role><Rule><ID>replica</ID><Priority>7</Priority><Status>Enabled</Status><Filter><And><Prefix>source/</Prefix><Tag><Key>team</Key><Value>storage</Value></Tag><Tag><Key>empty</Key><Value/></Tag></And></Filter><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><SourceSelectionCriteria><SseKmsEncryptedObjects><Status>Enabled</Status></SseKmsEncryptedObjects><ReplicaModifications><Status>Enabled</Status></ReplicaModifications></SourceSelectionCriteria><Destination><Bucket>arn:aws:s3:::replica-bucket</Bucket><Account>123456789013</Account><StorageClass>STANDARD_IA</StorageClass><AccessControlTranslation><Owner>Destination</Owner></AccessControlTranslation><EncryptionConfiguration><ReplicaKmsKeyID>arn:aws:kms:us-east-1:123456789013:key/fixture</ReplicaKmsKeyID></EncryptionConfiguration><ReplicationTime><Status>Enabled</Status><Time><Minutes>15</Minutes></Time></ReplicationTime><Metrics><Status>Enabled</Status><EventThreshold><Minutes>15</Minutes></EventThreshold></Metrics></Destination></Rule></ReplicationConfiguration>')

    notifications = {"TopicConfigurations": [{"Id": "topic", "TopicArn": "arn:aws:sns:us-east-1:123456789012:events",
        "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:Delete"],
        "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "incoming/雪/"},
                                           {"Name": "suffix", "Value": ".json"}]}}}],
        "QueueConfigurations": [{"Id": "queue", "QueueArn": "arn:aws:sqs:us-east-1:123456789012:queue",
                                  "Events": ["s3:ObjectRestore:Completed"]}],
        "LambdaFunctionConfigurations": [{"Id": "lambda", "LambdaFunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:handler",
                                           "Events": ["s3:ObjectCreated:Put"]}],
        "EventBridgeConfiguration": {}}
    pair("NotificationConfiguration", "NotificationConfiguration", notifications,
        '<NotificationConfiguration><TopicConfiguration><Id>topic</Id><Topic>arn:aws:sns:us-east-1:123456789012:events</Topic><Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:Delete</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>incoming/雪/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.json</Value></FilterRule></S3Key></Filter></TopicConfiguration><QueueConfiguration><Id>queue</Id><Queue>arn:aws:sqs:us-east-1:123456789012:queue</Queue><Event>s3:ObjectRestore:Completed</Event></QueueConfiguration><CloudFunctionConfiguration><Id>lambda</Id><CloudFunction>arn:aws:lambda:us-east-1:123456789012:function:handler</CloudFunction><Event>s3:ObjectCreated:Put</Event></CloudFunctionConfiguration><EventBridgeConfiguration/></NotificationConfiguration>',
        {"SkipDestinationValidation": True})

    pair("Logging", "BucketLoggingStatus", {"LoggingEnabled": {"TargetBucket": "logs", "TargetPrefix": "",
        "TargetGrants": [{"Grantee": {"Type": "Group", "URI": "http://acs.amazonaws.com/groups/s3/LogDelivery"}, "Permission": "WRITE"},
                         {"Grantee": {"Type": "CanonicalUser", "ID": "owner", "DisplayName": "fixture owner"}, "Permission": "FULL_CONTROL"}],
        "TargetObjectKeyFormat": {"PartitionedPrefix": {"PartitionDateSource": "EventTime"}}}},
        '<BucketLoggingStatus><LoggingEnabled><TargetBucket>logs</TargetBucket><TargetPrefix/><TargetGrants><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>http://acs.amazonaws.com/groups/s3/LogDelivery</URI></Grantee><Permission>WRITE</Permission></Grant><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>owner</ID><DisplayName>fixture owner</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></TargetGrants><TargetObjectKeyFormat><PartitionedPrefix><PartitionDateSource>EventTime</PartitionDateSource></PartitionedPrefix></TargetObjectKeyFormat></LoggingEnabled></BucketLoggingStatus>')

    pair("Website", "WebsiteConfiguration", {"IndexDocument": {"Suffix": "index.html"},
        "ErrorDocument": {"Key": "errors/404.html"}, "RoutingRules": [
            {"Condition": {"KeyPrefixEquals": "old/", "HttpErrorCodeReturnedEquals": "404"},
             "Redirect": {"HostName": "example.invalid", "Protocol": "https", "HttpRedirectCode": "302", "ReplaceKeyPrefixWith": "new/"}},
            {"Condition": {"KeyPrefixEquals": "gone"}, "Redirect": {"ReplaceKeyWith": "landing.html"}}]},
        '<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>errors/404.html</Key></ErrorDocument><RoutingRules><RoutingRule><Condition><KeyPrefixEquals>old/</KeyPrefixEquals><HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals></Condition><Redirect><HostName>example.invalid</HostName><Protocol>https</Protocol><HttpRedirectCode>302</HttpRedirectCode><ReplaceKeyPrefixWith>new/</ReplaceKeyPrefixWith></Redirect></RoutingRule><RoutingRule><Condition><KeyPrefixEquals>gone</KeyPrefixEquals></Condition><Redirect><ReplaceKeyWith>landing.html</ReplaceKeyWith></Redirect></RoutingRule></RoutingRules></WebsiteConfiguration>')

    pair("Cors", "CORSConfiguration", {"CORSRules": [{"ID": "primary", "AllowedHeaders": ["x-amz-*", "content-type"],
        "AllowedMethods": ["GET", "PUT"], "AllowedOrigins": ["https://example.invalid", "https://other.invalid"],
        "ExposeHeaders": ["ETag", "x-amz-version-id"], "MaxAgeSeconds": 0},
        {"ID": "read", "AllowedMethods": ["HEAD"], "AllowedOrigins": ["*"]}]},
        '<CORSConfiguration><CORSRule><ID>primary</ID><AllowedHeader>x-amz-*</AllowedHeader><AllowedHeader>content-type</AllowedHeader><AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><AllowedOrigin>https://example.invalid</AllowedOrigin><AllowedOrigin>https://other.invalid</AllowedOrigin><ExposeHeader>ETag</ExposeHeader><ExposeHeader>x-amz-version-id</ExposeHeader><MaxAgeSeconds>0</MaxAgeSeconds></CORSRule><CORSRule><ID>read</ID><AllowedMethod>HEAD</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>')

    pair("Encryption", "ServerSideEncryptionConfiguration", {"Rules": [{"ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "aws:kms", "KMSMasterKeyID": "arn:aws:kms:us-east-1:123456789012:key/fixture"}, "BucketKeyEnabled": False}]},
        '<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>arn:aws:kms:us-east-1:123456789012:key/fixture</KMSMasterKeyID></ApplyServerSideEncryptionByDefault><BucketKeyEnabled>false</BucketKeyEnabled></Rule></ServerSideEncryptionConfiguration>')

    pair("Acl", "AccessControlPolicy", {"Owner": {"ID": "owner", "DisplayName": "fixture owner"},
        "Grants": [{"Grantee": {"Type": "CanonicalUser", "ID": "owner", "DisplayName": "fixture owner"}, "Permission": "FULL_CONTROL"},
                   {"Grantee": {"Type": "Group", "URI": "http://acs.amazonaws.com/groups/global/AllUsers"}, "Permission": "READ"}]},
        '<AccessControlPolicy><Owner><ID>owner</ID><DisplayName>fixture owner</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>owner</ID><DisplayName>fixture owner</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>')

    pair("Tagging", "Tagging", {"TagSet": [{"Key": "team", "Value": "storage & 雪"}, {"Key": "empty", "Value": ""}]},
        '<Tagging><TagSet><Tag><Key>team</Key><Value>storage &amp; 雪</Value></Tag><Tag><Key>empty</Key><Value/></Tag></TagSet></Tagging>')

    key = "optional/雪 % +?#/key"
    headers = {"x-amz-version-id": "version+/=", "x-amz-server-side-encryption": "aws:kms",
               "x-amz-server-side-encryption-aws-kms-key-id": "fixture-kms", "x-amz-server-side-encryption-bucket-key-enabled": "false",
               "x-amz-request-charged": "requester", "etag": '"object-etag"'}
    add("PutObject", "headers-metadata-timestamps-blob", {"Bucket": "bucket", "Key": key,
        "CacheControl": "max-age=0", "ContentDisposition": "attachment; filename=fixture.bin",
        "ContentEncoding": "gzip", "ContentLanguage": "en", "ContentType": "application/x-fixture",
        "Expires": "2030-01-02T03:04:05Z", "Metadata": {"empty": "", "two": "two words", "owner": "fixture"},
        "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "fixture-kms", "SSEKMSEncryptionContext": "eyJmb28iOiJiYXIifQ==",
        "BucketKeyEnabled": False, "StorageClass": "STANDARD_IA", "WebsiteRedirectLocation": "/landing",
        "Tagging": "team=storage&empty=", "ObjectLockMode": "GOVERNANCE", "ObjectLockRetainUntilDate": "2030-01-02T03:04:05Z",
        "ObjectLockLegalHoldStatus": "OFF", "IfNoneMatch": "*", "RequestPayer": "requester", "ExpectedBucketOwner": "123456789012",
        "ChecksumAlgorithm": "SHA256"}, response(headers=headers), body=True)
    add("GetObject", "query-conditionals-timestamps-binary", {"Bucket": "bucket", "Key": key,
        "IfMatch": '"object-etag"', "IfModifiedSince": "2026-01-02T03:04:05Z", "IfUnmodifiedSince": "2030-01-02T03:04:05Z",
        "Range": "bytes=0-8", "ResponseCacheControl": "max-age=0", "ResponseContentDisposition": "attachment; filename=out.bin",
        "ResponseContentEncoding": "gzip", "ResponseContentLanguage": "en", "ResponseContentType": "application/x-fixture",
        "ResponseExpires": "2030-01-02T03:04:05Z", "VersionId": "version+/=", "PartNumber": 1,
        "RequestPayer": "requester", "ExpectedBucketOwner": "123456789012", "ChecksumMode": "ENABLED"},
        response(b"fixture\x00\xff", 206, {**headers, "content-type": "application/x-fixture", "content-range": "bytes 0-8/9",
            "last-modified": "Fri, 02 Jan 2026 03:04:05 GMT", "expires": "Wed, 02 Jan 2030 03:04:05 GMT",
            "cache-control": "max-age=0", "content-disposition": "attachment; filename=out.bin", "content-language": "en",
            "x-amz-meta-empty": "", "x-amz-meta-owner": "fixture", "x-amz-tagging-count": "0",
            "x-amz-object-lock-mode": "GOVERNANCE", "x-amz-object-lock-retain-until-date": "2030-01-02T03:04:05Z",
            "x-amz-object-lock-legal-hold": "OFF", "x-amz-mp-parts-count": "2"}))
    add("CopyObject", "copy-source-conditionals-metadata", {"Bucket": "bucket", "Key": key,
        "CopySource": "source/old%20key?versionId=old%2B%2F%3D", "CopySourceIfMatch": '"source-etag"',
        "CopySourceIfModifiedSince": "2026-01-02T03:04:05Z", "CopySourceIfUnmodifiedSince": "2030-01-02T03:04:05Z",
        "MetadataDirective": "REPLACE", "TaggingDirective": "REPLACE", "Metadata": {"empty": "", "owner": "fixture"},
        "Tagging": "a=1&empty=", "ContentType": "application/x-copy", "CacheControl": "max-age=0",
        "Expires": "2030-01-02T03:04:05Z", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "fixture-kms",
        "BucketKeyEnabled": False, "StorageClass": "STANDARD_IA", "RequestPayer": "requester",
        "ExpectedBucketOwner": "123456789012", "ExpectedSourceBucketOwner": "123456789013"},
        response('<CopyObjectResult><LastModified>2026-01-02T03:04:05Z</LastModified><ETag>"copy-etag"</ETag><ChecksumSHA256>YWJj</ChecksumSHA256><ChecksumType>FULL_OBJECT</ChecksumType></CopyObjectResult>',
                 headers={**headers, "x-amz-copy-source-version-id": "source+/="}))
    add("UploadPart", "checksummed-binary-query", {"Bucket": "bucket", "Key": key, "PartNumber": 7, "UploadId": "upload+/=",
        "ContentLength": 9, "ChecksumAlgorithm": "SHA256", "RequestPayer": "requester", "ExpectedBucketOwner": "123456789012"},
        response(headers={"etag": '"part-etag"', "x-amz-checksum-sha256": base64.b64encode(hashlib.sha256(b"fixture\x00\xff").digest()).decode(),
                          "x-amz-request-charged": "requester"}), body=True)
    cases.append({**cases[-1], "scenario": "invalid-response-checksum", "response":
        response(headers={"etag": '"part-etag"', "x-amz-checksum-sha256": "YWJj", "x-amz-request-charged": "requester"})})
    add("ListObjectsV2", "query-owner-restore-list", {"Bucket": "bucket", "Prefix": "prefix/雪 +%", "Delimiter": "/",
        "EncodingType": "url", "MaxKeys": 7, "ContinuationToken": "token+/=", "FetchOwner": True,
        "StartAfter": "after/雪 +%", "OptionalObjectAttributes": ["RestoreStatus"], "RequestPayer": "requester",
        "ExpectedBucketOwner": "123456789012"}, response('<ListBucketResult><Name>bucket</Name><Prefix>prefix%2F%E9%9B%AA%20%2B%25</Prefix><Delimiter>%2F</Delimiter><EncodingType>url</EncodingType><KeyCount>2</KeyCount><MaxKeys>7</MaxKeys><IsTruncated>false</IsTruncated><ContinuationToken>token+/=</ContinuationToken><StartAfter>after%2F%E9%9B%AA%20%2B%25</StartAfter><Contents><Key>key%2F%E9%9B%AA%20%2B%25</Key><LastModified>2026-01-02T03:04:05Z</LastModified><ETag>"list-etag"</ETag><Size>0</Size><StorageClass>GLACIER</StorageClass><Owner><ID>owner</ID><DisplayName>fixture owner</DisplayName></Owner><ChecksumAlgorithm>SHA256</ChecksumAlgorithm><ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType><RestoreStatus><IsRestoreInProgress>false</IsRestoreInProgress><RestoreExpiryDate>2030-01-02T03:04:05Z</RestoreExpiryDate></RestoreStatus></Contents><CommonPrefixes><Prefix>common%2F</Prefix></CommonPrefixes></ListBucketResult>', headers={"x-amz-request-charged": "requester"}))
    add("ListObjectVersions", "query-version-delete-marker-list", {"Bucket": "bucket", "Prefix": "prefix/", "Delimiter": "/",
        "EncodingType": "url", "MaxKeys": 7, "KeyMarker": "key/雪 +%", "VersionIdMarker": "version+/=",
        "OptionalObjectAttributes": ["RestoreStatus"], "ExpectedBucketOwner": "123456789012", "RequestPayer": "requester"},
        response('<ListVersionsResult><Name>bucket</Name><Prefix>prefix%2F</Prefix><Delimiter>%2F</Delimiter><EncodingType>url</EncodingType><MaxKeys>7</MaxKeys><IsTruncated>false</IsTruncated><KeyMarker>key%2F%E9%9B%AA%20%2B%25</KeyMarker><VersionIdMarker>version+/=</VersionIdMarker><Version><Key>data%2F%E9%9B%AA</Key><VersionId>v+/=</VersionId><IsLatest>false</IsLatest><LastModified>2026-01-02T03:04:05Z</LastModified><ETag>"v-etag"</ETag><Size>0</Size><StorageClass>GLACIER</StorageClass><Owner><ID>owner</ID></Owner><ChecksumAlgorithm>SHA256</ChecksumAlgorithm><RestoreStatus><IsRestoreInProgress>true</IsRestoreInProgress></RestoreStatus></Version><DeleteMarker><Key>gone%2F%E9%9B%AA</Key><VersionId>d+/=</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-03T03:04:05Z</LastModified><Owner><ID>owner</ID></Owner></DeleteMarker><CommonPrefixes><Prefix>common%2F</Prefix></CommonPrefixes></ListVersionsResult>'))
    add("CompleteMultipartUpload", "parts-list-checksum-conditionals", {"Bucket": "bucket", "Key": key, "UploadId": "upload+/=",
        "MultipartUpload": {"Parts": [{"ETag": '"part-one"', "PartNumber": 1, "ChecksumSHA256": "YWJj"},
                                      {"ETag": '"part-two"', "PartNumber": 2, "ChecksumSHA256": "ZGVm"}]},
        "ChecksumType": "COMPOSITE", "MpuObjectSize": 9, "IfMatch": '"old-etag"', "RequestPayer": "requester",
        "ExpectedBucketOwner": "123456789012"}, response('<CompleteMultipartUploadResult><Location>https://bucket.example.invalid/key</Location><Bucket>bucket</Bucket><Key>optional/雪 % +?#/key</Key><ETag>"complete-etag"</ETag><ChecksumSHA256>YWJj</ChecksumSHA256><ChecksumType>COMPOSITE</ChecksumType></CompleteMultipartUploadResult>', headers=headers))

    for document, label in [({}, "omitted"), ({"EventBridgeConfiguration": {}}, "empty-structure"),
                            ({"TopicConfigurations": []}, "empty-list")]:
        add("PutBucketNotificationConfiguration", label, {"Bucket": "bucket", "NotificationConfiguration": document}, response())
    for body, label in [("<NotificationConfiguration/>", "omitted"),
                        ("<NotificationConfiguration><EventBridgeConfiguration/></NotificationConfiguration>", "empty-structure")]:
        add("GetBucketNotificationConfiguration", label, {"Bucket": "bucket"}, response(body))
    add("PutBucketLogging", "omitted-disable", {"Bucket": "bucket", "BucketLoggingStatus": {}}, response())
    add("GetBucketLogging", "omitted", {"Bucket": "bucket"}, response('<BucketLoggingStatus/>'))
    add("PutBucketTagging", "empty-list", {"Bucket": "bucket", "Tagging": {"TagSet": []}}, response())
    add("GetBucketTagging", "empty-list", {"Bucket": "bucket"}, response('<Tagging><TagSet/></Tagging>'))
    add("PutObject", "empty-metadata", {"Bucket": "bucket", "Key": "empty", "Metadata": {}}, response(headers=headers), body=True)
    add("PutObject", "empty-binary", {"Bucket": "bucket", "Key": "empty"}, response(headers=headers), body=True)
    cases[-1]["source_body_base64"] = ""
    add("GetObject", "empty-binary", {"Bucket": "bucket", "Key": "empty"},
        response(b"", headers={"content-type": "application/octet-stream", "etag": '"empty-etag"'}))

    denied = response('<Error><Code>AccessDenied</Code><Message>Optional fixture denied</Message><RequestId>optional-request</RequestId><HostId>optional-host-id</HostId><BucketName>bucket</BucketName><Key>optional/key</Key></Error>',
                      403, {"x-amz-bucket-region": "us-east-1"})
    for operation in ["PutBucketReplication", "GetBucketNotificationConfiguration", "GetObject", "CopyObject"]:
        source = next(case for case in cases if case["operation"] == operation)
        case = {**source, "scenario": "access-denied-metadata", "response": denied}
        cases.append(case)
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    assert hashlib.sha256(args.aws.read_bytes()).hexdigest() == AWS_SHA256, "Unpinned AWS executable"
    model = ROOT / "priv/compatibility/aws/service-2.json"
    report = {"schema_version": 1, "aws_sha256": AWS_SHA256,
              "model_sha256": hashlib.sha256(model.read_bytes()).hexdigest(),
              "scope": "25 explicit representative operations; optional member variants, not exhaustive model coverage",
              "cases": []}
    with tempfile.TemporaryDirectory(prefix="ess-optional-oracle-") as temporary:
        directory = Path(temporary)
        (directory / "body.bin").write_bytes(b"fixture\x00\xff")
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("AWS_", "ESS_", "MC_")) and "proxy" not in key.lower()}
        env.update(AWS_CONFIG_FILE="/dev/null", AWS_SHARED_CREDENTIALS_FILE="/dev/null",
                   AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret",
                   AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="1", AWS_PAGER="",
                   AWS_CLI_AUTO_PROMPT="off", AWS_RESPONSE_CHECKSUM_VALIDATION="when_required",
                   NO_PROXY="*", PAGER="cat")
        for case in variants():
            (directory / "body.bin").write_bytes(base64.b64decode(case.get("source_body_base64", "Zml4dHVyZQD/")))
            case["aws"] = run(str(args.aws), case["args"], case["response"], env, directory)
            assert not case["aws"]["peer_errors"], case
            assert len(case["aws"]["requests"]) == 1, case
            expected = 254 if case["scenario"] == "access-denied-metadata" else 0
            assert case["aws"]["status"] == expected, case
            report["cases"].append(case)
            print(case["operation"], case["scenario"], case["aws"]["status"], flush=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
