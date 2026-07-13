# Refs & Versions (Git-Style Data Model Phase 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire S3 PUT/DELETE (and CopyObject/multipart-complete) through immutable version records so versioning-enabled buckets get real S3 semantics: every PUT creates a version, DELETE creates a delete marker, old versions stay readable by `versionId`, and specific versions can be permanently deleted (including marker deletion = undelete).

**Architecture (deviation from PRD §7.2/§7.3, to be recorded in the PRD):** The existing `obj:{bucket}:{key}` record **is** the mutable ref — it is read by ~15 callsites across all three apps (handlers, cloud backend, lifecycle, replication, web UI, ContentGC), so no new `ref:*` namespace is introduced. The existing `obj_ver:{bucket}:{key}:{vid}` + `obj_ver_list:{bucket}:{key}` schema **is** the `ver:*` store (no key rename, no migration — the schema already matches). `bucket_versioning:{bucket}` stays. What changes: `Storage.Versioning` gets correct S3 semantics (markers remove the `obj:` latest-view instead of corrupting it; deleting a specific version repoints the latest), version records gain `object_type`/`parent_version_id` fields, and the S3 handlers actually call it. Write order per PRD §8.1: version record first, `obj:` ref last.

**Tech Stack:** Same as Phase 1. All content already flows through the global CAS, so versions of identical content share one blob for free.

## Global Constraints

- Same as Phase 1 plan (no Ecto, no new deps, `--warnings-as-errors` clean, conventional commits, S3 integration tests via Req on `localhost:9001`, `async: false`).
- `obj:{bucket}:{key}` remains the record every existing reader consumes; after a delete marker it must be **absent** (GET/HEAD/list return not-found), never contain marker metadata.
- Existing tests pin behavior and must keep passing, especially `extended_features_test.exs` "bucket versioning" describe block (`get_version(bucket, key, nil)` returns the delete marker as latest via the version list) and all Phase 1 `global_cas_test.exs` tests.
- Cloud-cache buckets: no versioning wiring (PRD §3) — the cloud branches in `delete_object`/`copy_object` stay byte-identical.
- Out of scope (record as follow-ups, do not implement): `ListObjectVersions` API, `HeadObject` with `versionId`, `x-amz-copy-source` with `?versionId=`.

---

### Task 1: Correct S3 semantics in `Storage.Versioning`

**Files:**
- Modify: `apps/ex_storage_service/lib/ex_storage_service/storage/versioning.ex`
- Test: Create `apps/ex_storage_service/test/ex_storage_service/storage/versioning_test.exs`

**Interfaces:**
- Consumes: `Metadata.put_object_meta/3`, `get_object_meta/2`, `delete_object_meta/2`; `Concord.get/put/delete`.
- Produces (later tasks call these):
  - `Versioning.put_version(bucket, key, meta) :: {:ok, version_id}` — unchanged signature; now stamps `object_type: :blob` (unless meta already has one) and `parent_version_id` on the version record, and writes version-record → version-list → `obj:` in that order.
  - `Versioning.delete_version(bucket, key, version_id \\ nil) :: {:ok, version_id, :delete_marker | :deleted}` — unchanged signature; `nil` + enabled/suspended creates a marker **and removes `obj:`**; explicit `version_id` permanently removes that version and repoints `obj:` to the new latest (absent if none or if the new latest is a marker).
  - `Versioning.get_version/3`, `list_versions/2`, `get_versioning/1`, `set_versioning/2` — behavior unchanged.

- [ ] **Step 1: Write the failing tests**

```elixir
# apps/ex_storage_service/test/ex_storage_service/storage/versioning_test.exs
defmodule ExStorageService.Storage.VersioningTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Versioning

  defp unique_bucket, do: "ver-#{:erlang.unique_integer([:positive])}"

  defp meta_for(hash) do
    %{
      content_hash: hash,
      size: 10,
      etag: "etag-#{hash}",
      content_type: "application/octet-stream",
      metadata: %{},
      created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      updated_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end

  test "put_version stamps object_type and parent_version_id" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, v2} = Versioning.put_version(bucket, "k", meta_for("h2"))

    assert {:ok, ver1} = Versioning.get_version(bucket, "k", v1)
    assert ver1.object_type == :blob
    assert ver1.parent_version_id == nil

    assert {:ok, ver2} = Versioning.get_version(bucket, "k", v2)
    assert ver2.parent_version_id == v1

    # obj: ref points at the latest version
    assert {:ok, obj} = Metadata.get_object_meta(bucket, "k")
    assert obj.version_id == v2
    assert obj.content_hash == "h2"
  end

  test "delete marker removes the obj: latest view but keeps versions readable" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, marker_id, :delete_marker} = Versioning.delete_version(bucket, "k")

    # latest view is gone — GET/HEAD/list see no object
    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "k")

    # marker is the latest version; the old version is still readable
    assert {:ok, %{is_delete_marker: true}} = Versioning.get_version(bucket, "k", nil)
    assert {:ok, %{content_hash: "h1"}} = Versioning.get_version(bucket, "k", v1)
    assert marker_id != v1
  end

  test "PUT after a delete marker restores the latest view" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, _v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, _marker, :delete_marker} = Versioning.delete_version(bucket, "k")
    {:ok, v3} = Versioning.put_version(bucket, "k", meta_for("h3"))

    assert {:ok, obj} = Metadata.get_object_meta(bucket, "k")
    assert obj.version_id == v3
    assert obj.content_hash == "h3"
  end

  test "deleting the latest version by id repoints obj: to the previous version" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, v2} = Versioning.put_version(bucket, "k", meta_for("h2"))

    {:ok, ^v2, :deleted} = Versioning.delete_version(bucket, "k", v2)

    assert {:ok, obj} = Metadata.get_object_meta(bucket, "k")
    assert obj.version_id == v1
    assert obj.content_hash == "h1"
    assert {:error, :not_found} = Versioning.get_version(bucket, "k", v2)
  end

  test "deleting a delete-marker version by id undeletes the object" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, marker_id, :delete_marker} = Versioning.delete_version(bucket, "k")
    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "k")

    {:ok, ^marker_id, :deleted} = Versioning.delete_version(bucket, "k", marker_id)

    assert {:ok, obj} = Metadata.get_object_meta(bucket, "k")
    assert obj.version_id == v1
  end

  test "deleting the only version removes obj: entirely" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, ^v1, :deleted} = Versioning.delete_version(bucket, "k", v1)

    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "k")
    assert {:ok, []} = Versioning.list_versions(bucket, "k")
  end

  test "disabled buckets keep plain semantics" do
    bucket = unique_bucket()

    {:ok, "null"} = Versioning.put_version(bucket, "k", meta_for("h1"))
    assert {:ok, %{content_hash: "h1"}} = Metadata.get_object_meta(bucket, "k")

    {:ok, "null", :deleted} = Versioning.delete_version(bucket, "k")
    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "k")
  end
end
```

- [ ] **Step 2: Run tests to verify the semantic ones fail**

Run: `mix test apps/ex_storage_service/test/ex_storage_service/storage/versioning_test.exs`
Expected: FAIL — `object_type`/`parent_version_id` missing; "delete marker removes the obj:" fails (marker meta currently written into `obj:`); repoint/undelete tests fail.

- [ ] **Step 3: Implement**

In `versioning.ex`:

**(a)** Replace `put_version/3`'s `:enabled`/`:suspended` clauses so they stamp fields (the `:disabled` clause stays as-is except for field stamping):

```elixir
  @spec put_version(String.t(), String.t(), map()) :: {:ok, String.t()}
  def put_version(bucket, key, meta) do
    state = get_versioning(bucket)
    meta = stamp_version_fields(bucket, key, meta)

    case state do
      :enabled ->
        version_id = generate_version_id()
        store_version(bucket, key, version_id, meta)
        {:ok, version_id}

      :suspended ->
        store_version(bucket, key, "null", meta)
        {:ok, "null"}

      :disabled ->
        Metadata.put_object_meta(bucket, key, meta)
        {:ok, "null"}
    end
  end

  # Every version record carries its object type (Phase 3 adds :manifest)
  # and a parent pointer to the version it superseded.
  defp stamp_version_fields(bucket, key, meta) do
    parent =
      case Metadata.get_object_meta(bucket, key) do
        {:ok, %{version_id: vid}} -> vid
        _ -> nil
      end

    meta
    |> Map.put_new(:object_type, :blob)
    |> Map.put(:parent_version_id, parent)
  end
```

**(b)** Reorder `store_version/4` so the mutable ref (`obj:`) is written last (PRD §8.1 crash ordering):

```elixir
  defp store_version(bucket, key, version_id, meta) do
    meta_with_version = Map.put(meta, :version_id, version_id)

    # Write order matters for crash safety: immutable version record first,
    # then the version index, then the mutable obj: ref last so the latest
    # view never points at a version record that does not exist.
    Concord.put("obj_ver:#{bucket}:#{key}:#{version_id}", meta_with_version)
    add_to_version_list(bucket, key, version_id)
    Metadata.put_object_meta(bucket, key, meta_with_version)
  end
```

**(c)** Replace `delete_version/3` marker clauses so markers remove `obj:` instead of writing marker meta into it, and specific-version deletion repoints the latest view:

```elixir
  @spec delete_version(String.t(), String.t(), String.t() | nil) ::
          {:ok, String.t(), :delete_marker | :deleted}
  def delete_version(bucket, key, version_id \\ nil) do
    state = get_versioning(bucket)

    case {state, version_id} do
      {:disabled, _} ->
        Metadata.delete_object_meta(bucket, key)
        {:ok, "null", :deleted}

      {:enabled, nil} ->
        marker_version_id = generate_version_id()
        create_delete_marker(bucket, key, marker_version_id)
        {:ok, marker_version_id, :delete_marker}

      {:suspended, nil} ->
        create_delete_marker(bucket, key, "null")
        {:ok, "null", :delete_marker}

      {_, vid} ->
        Concord.delete("obj_ver:#{bucket}:#{key}:#{vid}")
        remove_from_version_list(bucket, key, vid)
        repoint_latest(bucket, key, vid)
        {:ok, vid, :deleted}
    end
  end

  # A delete marker is an immutable version record; the mutable obj: latest
  # view is removed so GET/HEAD/list treat the key as absent (PRD §10.2).
  defp create_delete_marker(bucket, key, marker_version_id) do
    marker_meta = %{
      is_delete_marker: true,
      object_type: :blob,
      parent_version_id: current_version_id(bucket, key),
      created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      version_id: marker_version_id
    }

    Concord.put("obj_ver:#{bucket}:#{key}:#{marker_version_id}", marker_meta)
    add_to_version_list(bucket, key, marker_version_id)
    Metadata.delete_object_meta(bucket, key)
  end

  defp current_version_id(bucket, key) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, %{version_id: vid}} -> vid
      _ -> nil
    end
  end

  # After permanently deleting a specific version, the newest remaining
  # version becomes latest: a normal version repopulates obj:; a delete
  # marker (or nothing) leaves the key absent. Only needed when the
  # deleted version was the current latest.
  defp repoint_latest(bucket, key, deleted_vid) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, %{version_id: current}} when current != deleted_vid ->
        :ok

      _ ->
        case get_version_list(bucket, key) do
          [] ->
            Metadata.delete_object_meta(bucket, key)

          [head | _] ->
            case Concord.get("obj_ver:#{bucket}:#{key}:#{head}") do
              {:ok, %{is_delete_marker: true}} ->
                Metadata.delete_object_meta(bucket, key)

              {:ok, head_meta} when is_map(head_meta) ->
                Metadata.put_object_meta(bucket, key, head_meta)

              _ ->
                Metadata.delete_object_meta(bucket, key)
            end
        end
    end
  end
```

Keep `get_version/3`, `list_versions/2`, `get_version_list/2`, `add_to_version_list/3`, `remove_from_version_list/3`, `generate_version_id/0` unchanged. Update the moduledoc's key-schema section to note `obj:{bucket}:{key}` is the mutable ref (absent when latest is a delete marker).

- [ ] **Step 4: Run core + S3 suites**

Run: `mix test apps/ex_storage_service/test && mix test apps/ex_storage_service_s3/test`
Expected: all pass, including `extended_features_test.exs` (its marker test reads latest via `get_version(bucket, key, nil)` which uses the version list — unaffected by `obj:` removal).

- [ ] **Step 5: Commit**

```bash
git add apps/ex_storage_service/lib/ex_storage_service/storage/versioning.ex \
        apps/ex_storage_service/test/ex_storage_service/storage/versioning_test.exs
git commit -m "fix(core): correct S3 delete-marker and version-repoint semantics"
```

---

### Task 2: PUT paths create versions and return x-amz-version-id

**Files:**
- Modify: `apps/ex_storage_service_s3/lib/ex_storage_service_s3/handlers/object/local_backend.ex` (both PUT paths)
- Modify: `apps/ex_storage_service_s3/lib/ex_storage_service_s3/multipart_handlers.ex:~170` (complete-upload metadata write)
- Modify: `apps/ex_storage_service_s3/lib/ex_storage_service_s3/handlers/object.ex:~228` (CopyObject destination write)
- Test: Create `apps/ex_storage_service_s3/test/ex_storage_service_s3/versioned_objects_test.exs`

**Interfaces:**
- Consumes: `Versioning.put_version/3` (Task 1).
- Produces: every local-bucket write path creates a version when versioning is enabled and sets the `x-amz-version-id` response header (only when the id is not `"null"`).

- [ ] **Step 1: Write the failing integration tests**

```elixir
# apps/ex_storage_service_s3/test/ex_storage_service_s3/versioned_objects_test.exs
defmodule ExStorageServiceS3.VersionedObjectsTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.CAS

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "vobj-#{:erlang.unique_integer([:positive])}"

  defp create_versioned_bucket do
    bucket = unique_bucket()
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")

    versioning_xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>
    """

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}?versioning", body: versioning_xml)
    bucket
  end

  defp version_id(resp) do
    [vid] = Req.Response.get_header(resp, "x-amz-version-id")
    vid
  end

  test "each PUT creates a distinct version; old versions readable by versionId" do
    bucket = create_versioned_bucket()

    {:ok, r1} = Req.put("#{@base_url}/#{bucket}/doc.txt", body: "version-one")
    {:ok, r2} = Req.put("#{@base_url}/#{bucket}/doc.txt", body: "version-two")
    v1 = version_id(r1)
    v2 = version_id(r2)
    assert v1 != v2

    {:ok, %{status: 200, body: latest}} = Req.get("#{@base_url}/#{bucket}/doc.txt")
    assert latest == "version-two"

    {:ok, %{status: 200, body: old}} = Req.get("#{@base_url}/#{bucket}/doc.txt?versionId=#{v1}")
    assert old == "version-one"
  end

  test "PUT on unversioned bucket returns no x-amz-version-id header" do
    bucket = unique_bucket()
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")

    {:ok, resp} = Req.put("#{@base_url}/#{bucket}/k.txt", body: "plain")
    assert resp.status == 200
    assert Req.Response.get_header(resp, "x-amz-version-id") == []
  end

  test "versions of identical content share one CAS blob" do
    bucket = create_versioned_bucket()
    data = "same-content-#{System.unique_integer()}"
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)

    {:ok, r1} = Req.put("#{@base_url}/#{bucket}/dup.txt", body: data)
    {:ok, r2} = Req.put("#{@base_url}/#{bucket}/dup.txt", body: data)
    assert version_id(r1) != version_id(r2)

    assert File.exists?(CAS.blob_path(hash))

    # both versions resolve to the same blob
    {:ok, %{status: 200, body: b1}} =
      Req.get("#{@base_url}/#{bucket}/dup.txt?versionId=#{version_id(r1)}")

    assert b1 == data
  end

  test "CopyObject destination gets a version in a versioned bucket" do
    src = unique_bucket()
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{src}", body: "")
    dst = create_versioned_bucket()

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/a.txt", body: "copy-src")

    {:ok, copy_resp} =
      Req.put("#{@base_url}/#{dst}/b.txt",
        headers: [{"x-amz-copy-source", "/#{src}/a.txt"}],
        body: ""
      )

    assert copy_resp.status == 200
    vid = version_id(copy_resp)

    {:ok, %{status: 200, body: body}} = Req.get("#{@base_url}/#{dst}/b.txt?versionId=#{vid}")
    assert body == "copy-src"
  end
end
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/versioned_objects_test.exs`
Expected: FAIL — no `x-amz-version-id` header (PUT never calls `put_version`); `?versionId=` GET of old version 404s (only one version exists).

- [ ] **Step 3: Implement**

**(a)** `local_backend.ex` — add `alias ExStorageService.Storage.Versioning` at the top. In `put_decoded_object/7`, replace:

```elixir
        Metadata.put_object_meta(bucket, key, meta)
        Hooks.after_put(bucket, key)
        broadcast_bucket_change(bucket, :put, key)

        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("etag", "\"#{etag}\"")
        |> send_resp(200, "")
```

with:

```elixir
        {:ok, version_id} = Versioning.put_version(bucket, key, meta)
        Hooks.after_put(bucket, key)
        broadcast_bucket_change(bucket, :put, key)

        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("etag", "\"#{etag}\"")
        |> maybe_put_version_header(version_id)
        |> send_resp(200, "")
```

Apply the identical replacement in `put_object_local_streamed/6` (same three lines plus header). Add the helper at the bottom of the module:

```elixir
  defp maybe_put_version_header(conn, "null"), do: conn
  defp maybe_put_version_header(conn, version_id),
    do: put_resp_header(conn, "x-amz-version-id", version_id)
```

**(b)** `multipart_handlers.ex` — add `alias ExStorageService.Storage.Versioning`; at line ~170 replace `Metadata.put_object_meta(bucket, key, meta)` with:

```elixir
                    {:ok, _version_id} = Versioning.put_version(bucket, key, meta)
```

(The complete-multipart XML response does not carry the version header in this phase; note it in the PRD follow-ups.)

**(c)** `handlers/object.ex` `copy_object/3` — add `x-amz-version-id` to the copy response. Replace:

```elixir
                  :ok ->
                    Metadata.put_object_meta(bucket, key, new_meta)
                    Hooks.after_put(bucket, key)
                    broadcast_bucket_change(bucket, :put, key)
                    # CopyObjectResult requires ISO 8601 (not HTTP date format)
                    body = XML.copy_object_response("\"#{source_meta.etag}\"", now)
                    xml_response(conn, 200, body, request_id)
```

with:

```elixir
                  :ok ->
                    {:ok, version_id} = Versioning.put_version(bucket, key, new_meta)
                    Hooks.after_put(bucket, key)
                    broadcast_bucket_change(bucket, :put, key)
                    # CopyObjectResult requires ISO 8601 (not HTTP date format)
                    body = XML.copy_object_response("\"#{source_meta.etag}\"", now)

                    conn = maybe_put_version_header(conn, version_id)
                    xml_response(conn, 200, body, request_id)
```

and add the same two-clause `maybe_put_version_header/2` helper to this module (it is private in each module rather than shared — `Handlers.Shared` is an `import` module; keep the helper local to avoid import ambiguity).

`new_meta` in copy_object merges the source meta — it may carry the source's `version_id`/`parent_version_id`/`object_type`; `put_version` re-stamps `parent_version_id` and `store_version` overwrites `version_id`, and `Map.put_new(:object_type, :blob)` keeps the source's type. That is correct.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/versioned_objects_test.exs`
Expected: 4 tests, 0 failures

- [ ] **Step 5: Run full S3 + core suites**

Run: `mix test apps/ex_storage_service_s3/test && mix test apps/ex_storage_service/test`
Expected: all pass (unversioned buckets hit the `:disabled` branch of `put_version`, which writes `obj:` exactly as before)

- [ ] **Step 6: Commit**

```bash
git add apps/ex_storage_service_s3/lib apps/ex_storage_service_s3/test/ex_storage_service_s3/versioned_objects_test.exs
git commit -m "feat(s3): create object versions on PUT/copy/multipart and return x-amz-version-id"
```

---

### Task 3: DELETE paths — markers, versionId deletes, defensive GET

**Files:**
- Modify: `apps/ex_storage_service_s3/lib/ex_storage_service_s3/handlers/object.ex` (`delete_object/3`, `delete_objects/2`)
- Modify: `apps/ex_storage_service_s3/lib/ex_storage_service_s3/handlers/object/local_backend.ex` (`get_object/4` delete-marker guard)
- Test: extend `apps/ex_storage_service_s3/test/ex_storage_service_s3/versioned_objects_test.exs`

**Interfaces:**
- Consumes: `Versioning.delete_version/3` (Task 1).
- Produces: `DELETE /{bucket}/{key}` creates a marker on versioned buckets (204 + `x-amz-delete-marker: true` + `x-amz-version-id`); `DELETE /{bucket}/{key}?versionId=X` permanently deletes version X (204 + `x-amz-version-id: X`, plus `x-amz-delete-marker: true` when X was a marker); batch `DeleteObjects` uses the same versioned semantics per key.

- [ ] **Step 1: Write the failing integration tests** (append to `versioned_objects_test.exs`)

```elixir
  test "DELETE creates a marker; old version readable; PUT restores; versionId deletes repoint" do
    bucket = create_versioned_bucket()

    {:ok, r1} = Req.put("#{@base_url}/#{bucket}/life.txt", body: "v-one")
    v1 = version_id(r1)
    {:ok, _r2} = Req.put("#{@base_url}/#{bucket}/life.txt", body: "v-two")

    # DELETE → delete marker
    {:ok, del} = Req.delete("#{@base_url}/#{bucket}/life.txt")
    assert del.status == 204
    assert Req.Response.get_header(del, "x-amz-delete-marker") == ["true"]
    [marker_id] = Req.Response.get_header(del, "x-amz-version-id")

    # latest view gone, old versions remain readable
    {:ok, %{status: 404}} = Req.get("#{@base_url}/#{bucket}/life.txt")
    {:ok, %{status: 200, body: "v-one"}} =
      Req.get("#{@base_url}/#{bucket}/life.txt?versionId=#{v1}")

    # GET of the marker version 404s and flags the marker
    {:ok, marker_get} = Req.get("#{@base_url}/#{bucket}/life.txt?versionId=#{marker_id}")
    assert marker_get.status == 404
    assert Req.Response.get_header(marker_get, "x-amz-delete-marker") == ["true"]

    # PUT after marker restores visibility
    {:ok, r3} = Req.put("#{@base_url}/#{bucket}/life.txt", body: "v-three")
    v3 = version_id(r3)
    {:ok, %{status: 200, body: "v-three"}} = Req.get("#{@base_url}/#{bucket}/life.txt")

    # permanently delete the current version → marker becomes latest → 404
    {:ok, del_v3} = Req.delete("#{@base_url}/#{bucket}/life.txt?versionId=#{v3}")
    assert del_v3.status == 204
    {:ok, %{status: 404}} = Req.get("#{@base_url}/#{bucket}/life.txt")

    # permanently delete the marker → undelete: v-two visible again
    {:ok, del_marker} = Req.delete("#{@base_url}/#{bucket}/life.txt?versionId=#{marker_id}")
    assert del_marker.status == 204
    {:ok, %{status: 200, body: "v-two"}} = Req.get("#{@base_url}/#{bucket}/life.txt")
  end

  test "batch DeleteObjects creates markers on versioned buckets" do
    bucket = create_versioned_bucket()

    {:ok, r1} = Req.put("#{@base_url}/#{bucket}/batch.txt", body: "keep-me")
    v1 = version_id(r1)

    delete_xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <Delete><Object><Key>batch.txt</Key></Object></Delete>
    """

    {:ok, %{status: 200}} = Req.post("#{@base_url}/#{bucket}?delete", body: delete_xml)

    {:ok, %{status: 404}} = Req.get("#{@base_url}/#{bucket}/batch.txt")
    {:ok, %{status: 200, body: "keep-me"}} =
      Req.get("#{@base_url}/#{bucket}/batch.txt?versionId=#{v1}")
  end

  test "DELETE on unversioned bucket has no marker headers and stays idempotent" do
    bucket = unique_bucket()
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/p.txt", body: "x")

    {:ok, del} = Req.delete("#{@base_url}/#{bucket}/p.txt")
    assert del.status == 204
    assert Req.Response.get_header(del, "x-amz-delete-marker") == []

    {:ok, del_again} = Req.delete("#{@base_url}/#{bucket}/p.txt")
    assert del_again.status == 204
  end
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/versioned_objects_test.exs`
Expected: the three new tests FAIL (no marker headers; `?versionId=` DELETE ignored; batch delete removes `obj:` without marker)

- [ ] **Step 3: Implement**

**(a)** `handlers/object.ex` `delete_object/3` — replace the metadata-delete block (keep the cloud-cache branch above it byte-identical):

```elixir
      version_id = conn.query_params["versionId"]

      case Versioning.delete_version(bucket, key, version_id) do
        {:ok, marker_id, :delete_marker} ->
          Hooks.after_delete(bucket, key)
          broadcast_bucket_change(bucket, :delete, key)

          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("x-amz-delete-marker", "true")
          |> put_resp_header("x-amz-version-id", marker_id)
          |> send_resp(204, "")

        {:ok, "null", :deleted} when is_nil(version_id) ->
          Hooks.after_delete(bucket, key)
          broadcast_bucket_change(bucket, :delete, key)

          conn
          |> put_s3_headers(request_id)
          |> send_resp(204, "")

        {:ok, deleted_vid, :deleted} ->
          Hooks.after_delete(bucket, key)
          broadcast_bucket_change(bucket, :delete, key)

          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("x-amz-version-id", deleted_vid)
          |> send_resp(204, "")
      end
```

Notes:
- The previous `Metadata.get_object_meta` existence check goes away: S3 DELETE is idempotent and `delete_version` handles absent keys (`:disabled` path deletes a nonexistent key harmlessly; marker creation on a never-existing key matches AWS, which also creates markers for nonexistent keys).
- `conn.query_params` is already fetched by the router (it reads `conn.query_params` before dispatch).

**(b)** `delete_objects/2` — inside the per-key `Enum.map`, replace:

```elixir
                case Metadata.get_object_meta(bucket, key) do
                  {:ok, _meta} ->
                    Metadata.delete_object_meta(bucket, key)
                    Hooks.after_delete(bucket, key)
                    {:deleted, key}

                  {:error, :not_found} ->
                    {:deleted, key}
                end
```

with:

```elixir
                {:ok, _vid, _kind} = Versioning.delete_version(bucket, key)
                Hooks.after_delete(bucket, key)
                {:deleted, key}
```

**(c)** `local_backend.ex` `get_object/4` — guard against pre-existing marker metadata in `obj:` (data written before Task 1's fix). At the top of the `{:ok, meta}` branch, before `content_hash = meta.content_hash`, insert:

```elixir
        if Map.get(meta, :is_delete_marker) do
          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("x-amz-delete-marker", "true")
          |> send_resp(404, "")
        else
          ... existing body (content_hash = meta.content_hash ...) ...
        end
```

(Wrap the existing body in the `else`; do not otherwise change it.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test apps/ex_storage_service_s3/test/ex_storage_service_s3/versioned_objects_test.exs`
Expected: 7 tests, 0 failures

- [ ] **Step 5: Full S3 + core suites**

Run: `mix test apps/ex_storage_service_s3/test && mix test apps/ex_storage_service/test`
Expected: all pass (unversioned delete flows through `{:disabled}` → same 204 behavior as before)

- [ ] **Step 6: Commit**

```bash
git add apps/ex_storage_service_s3/lib apps/ex_storage_service_s3/test
git commit -m "feat(s3): versioned DeleteObject with markers and versionId deletes"
```

---

### Task 4: Web delete goes through versioning; PRD sync; full verification

**Files:**
- Modify: `apps/ex_storage_service_web/lib/ex_storage_service_web/live/bucket_live/files.ex`
- Modify: `docs/prd/git-style-data-model.md` (§7.2/§7.3 deviation note, §20 Phase 2 status, follow-ups)

**Interfaces:**
- Consumes: `Versioning.delete_version/2` (Task 1).

- [ ] **Step 1: Web delete via versioning**

In `files.ex`, add `alias ExStorageService.Storage.Versioning` and in `handle_event("confirm_delete_object", ...)` replace:

```elixir
    case Metadata.get_object_meta(bucket, key) do
      {:ok, _meta} ->
        Metadata.delete_object_meta(bucket, key)
```

with:

```elixir
    case Metadata.get_object_meta(bucket, key) do
      {:ok, _meta} ->
        Versioning.delete_version(bucket, key)
```

(Admin deletes now create delete markers on versioned buckets, same as the S3 API.)

- [ ] **Step 2: PRD sync**

In `docs/prd/git-style-data-model.md`:
- Add to the Revision-2 changelog: "11. Phase 2 implementation note: the existing `obj:{bucket}:{key}` record serves as the mutable ref (absent when the latest version is a delete marker) and the existing `obj_ver:*` / `obj_ver_list:*` keys serve as the version store — no `ref:*`/`ver:*` key rename or metadata migration was needed. `bucket_versioning:{bucket}` is retained."
- In §7.2 and §7.3, add a one-line "Implemented as:" note stating the actual key names.
- In §20 Phase 2, mark the phase's bullets with their status and list follow-ups: `ListObjectVersions` API, `HeadObject?versionId`, copy-source `?versionId`, `x-amz-version-id` on CompleteMultipartUpload response.

- [ ] **Step 3: Full verification**

Run:
```bash
mix format && git diff --exit-code
mix compile --warnings-as-errors
mix test apps/ex_storage_service/test
mix test apps/ex_storage_service_s3/test
mix test apps/ex_storage_service_web/test
```
Expected: all clean/passing.

- [ ] **Step 4: Commit**

```bash
git add apps/ex_storage_service_web/lib/ex_storage_service_web/live/bucket_live/files.ex docs/prd/git-style-data-model.md
git commit -m "feat(web): create delete markers from admin object browser"
```
