# S3 request authorization

The operation resolver supplies one descriptor to authorization and dispatch.
Signatures authenticate an identity; signed and presigned requests both require
an active access key, an active user, and current policy authorization. Root
retains its explicit administrative bypass. Concord reads use the locked
library's linearizable VSR read barrier; revocation is checked on subsequent
requests, not through a process-local policy cache.

New policies created through `IAM.Policy.create_policy/2` carry
`semantics: :aws_s3_v1` and use AWS policy actions. API names are not necessarily
policy actions:

| S3 API operation | AWS policy action |
| --- | --- |
| HeadBucket, ListObjects | `s3:ListBucket` |
| GetObject, HeadObject | `s3:GetObject` |
| Explicit-version GetObject, HeadObject | `s3:GetObjectVersion` |
| PutObject, CopyObject destination | `s3:PutObject` |
| CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload | `s3:PutObject` |
| CopyObject, UploadPartCopy source | `s3:GetObject` or `s3:GetObjectVersion` |
| PutObject/CreateMultipartUpload supplying tags; CopyObject producing a nonempty tag set | Additional destination `s3:PutObjectTagging` |
| GetObjectTagging, PutObjectTagging, DeleteObjectTagging | Matching `s3:GetObjectTagging`, `s3:PutObjectTagging`, `s3:DeleteObjectTagging` |
| Explicit-version tagging | Matching `s3:GetObjectVersionTagging`, `s3:PutObjectVersionTagging`, `s3:DeleteObjectVersionTagging` |
| DeleteObject, each ordinary DeleteObjects entry | `s3:DeleteObject` |
| Explicit-version deletion, including each batch entry | `s3:DeleteObjectVersion` |
| ListParts | `s3:ListMultipartUploadParts` |
| AbortMultipartUpload | `s3:AbortMultipartUpload` |

Every source/destination resource is authorized independently. DeleteObjects
returns AccessDenied for individual forbidden entries, including in quiet mode.
Its XML document requires Content-MD5 or a supported checksum, has an 8 MiB body
limit, and accepts at most 1000 entries. No entry mutates before document integrity
and structure validation. All multipart endpoints bind upload IDs to their stored
bucket and key before reading parts or accepting data.

Upload tagging permission is independent of object write permission. CopyObject
checks the actual selected source tags or replacement tag set before transferring
bytes; an untagged copy or empty REPLACE does not require a tag-write grant.
CopyObject's source action remains GetObject/GetObjectVersion; separately calling
GetObjectTagging requires its own tag-read grant. Object reads continue to return
authorized bytes without a tag-read grant, but omit `x-amz-tagging-count` unless
the caller has the matching current/version tag-read permission. Explicit denies
still apply. These action distinctions follow the
[AWS API permissions table](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html)
and [GetObject response contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).

Canonical resources use `arn:aws:s3:::bucket` and
`arn:aws:s3:::bucket/key`. Legacy `arn:ess:::` prefixes are exact aliases: only
the prefix is normalized, preserving every bucket/key glob and its original
scope. Missing or unreadable attached policies fail the whole evaluation closed.
Unsupported statement keys such as conditions/principals are rejected rather
than ignored; malformed stored statements deny access. This is a bounded S3
identity-policy engine, not full IAM or STS.

Existing stored policies without a semantics marker retain operation-specific
ESS allow semantics. A legacy `s3:HeadObject` grant remains HEAD-only; a legacy
ordinary `s3:PutObject` grant does not acquire multipart initiation/completion.
Legacy explicit denies remain effective across canonical and original operation
actions. Newly created AWS policies evaluate version operations against the
version action: `s3:GetObject` alone does not grant `s3:GetObjectVersion`.
Deprecated exact action names (HeadObject, HeadBucket, CreateMultipartUpload,
UploadPart, CompleteMultipartUpload) remain accepted only for their matching
request operation. Policy updates preserve the stored semantics marker; there
is no automatic rewrite that broadens existing grants.

ReadOnly/ReadWrite built-in templates now use real AWS read actions; their
version reads include GetObjectVersion. Bucket-scoped templates emit canonical
S3 ARNs. Existing attachments and stored documents are retained. To adopt AWS
semantics for an old policy, explicitly create a new reviewed policy and replace
its attachment; do not remove the marker to bypass validation or silently relabel
old grants.

Tests: `iam/policy_hardening_test.exs`, `plugs/authorize_test.exs`,
`authorization_hardening_test.exs`, and `authorization_http_test.exs`.
Tag-grant and tag-count regressions use `tagging_authorization_http_test.exs`.
Executed results and any outstanding HTTP/cluster/gateway acceptance are recorded in
`docs/plans/s3-hardening-auth-evidence.md`; this document does not claim unexecuted
profiles passed.
