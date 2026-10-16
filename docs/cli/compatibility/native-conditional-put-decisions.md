# Native conditional creation

`ess put LOCAL_OR_STDIN TARGET --if-not-exists` sends `If-None-Match: *` on
the write that publishes the object. It does not perform an existence read that
could race with a concurrent writer. The flag belongs only to native `put`;
download, remote copy and injected use on another command fail before I/O.

Single uploads sign the condition on PutObject. Multipart uploads retain the
condition on CompleteMultipartUpload, without adding it to creation, parts or
the post-completion HEAD. That HEAD still confirms the returned version, ETag
and length. A service 412 remains a failure; cleanup aborts only the owned upload.
The journal's input digest includes the condition, so resume cannot silently
turn a conditional creation into an overwrite.

The pinned mc flag also requests conditional creation. ESS retains its existing
two-operand `put` grammar, bounded transfer engine and structured failures;
this addition does not claim complete mc argument/default/output parity or
conditional-write support from every provider. A provider that rejects the
condition returns its actual service error.

The reproducing suite initially failed seven of eight tests. After the change,
the conditional-put, multipart and high-level suites passed **33 tests, zero
failures**. Tests exercise signed single PUT, rejection without overwrite,
multipart completion and version verification, owned abort and resume fencing.
See `cli/native_conditional_put_test.exs` under the CLI test application.
Live-provider conditional-write acceptance remains separate.
