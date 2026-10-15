Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transfer.MultipartTest do
  # Journal mutex ports share the host namespace with concurrent wire fixtures.
  use ExUnit.Case, async: false

  alias ExStorageServiceCli.{
    Context,
    Error,
    Transfer.Journal,
    Transfer.Multipart,
    Transfer.Planner,
    Transfer.Source
  }

  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @limits %{
    max_single_put_bytes: 5,
    min_nonfinal_part_bytes: 5,
    max_part_bytes: 10,
    max_parts: 10,
    max_object_bytes: 100
  }
  @create "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>owned</UploadId></InitiateMultipartUploadResult>"
  @complete "<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><ETag>&quot;done-2&quot;</ETag></CompleteMultipartUploadResult>"
  @denied "<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>"

  setup %{tmp_dir: root} do
    path = Path.join(root, "source")
    File.write!(path, "abcdefghij")
    {:ok, source} = Source.file(path)
    {:ok, plan} = Planner.plan(10, @limits)
    %{source: source, plan: plan, journal_root: Path.join(root, "journals")}
  end

  test "owned multipart progress is checkpointed, completed and independently verified", data do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], @create},
        {200, [{"ETag", "\"first\""}], ""},
        {200, [{"ETag", "\"second\""}], ""},
        {200, [], @complete},
        {200, [{"ETag", "\"done-2\""}, {"Content-Length", "10"}], ""}
      ])

    context = %Context{endpoint: endpoint, access_key_id: "access", secret_access_key: "secret"}

    assert {:ok, %{bytes: 10, etag: "done-2", journal_id: id}} =
             Multipart.upload(context, "bucket", "key", data.source, data.plan,
               state_root: data.journal_root
             )

    assert {:ok, %{"phase" => "completed", "parts" => [first, second]}} =
             Journal.get(data.journal_root, id)

    assert first["etag"] == "\"first\""
    assert second["etag"] == "\"second\""
    refute File.read!(Path.join(data.journal_root, id <> ".json")) =~ "secret"
    assert_receive {:wire_request, %{target: "/bucket/key?uploads="}}
    assert_receive {:wire_request, %{body: "abcde", headers: headers}}
    assert headers["content-length"] == "5"
    assert_receive {:wire_request, %{body: "fghij"}}
    assert_receive {:wire_request, %{method: "POST", body: completion}}
    assert completion =~ "<PartNumber>1</PartNumber>"
    assert completion =~ "<PartNumber>2</PartNumber>"
    assert_receive {:wire_request, %{method: "HEAD"}}
  end

  test "part failure aborts only our own upload and records cleanup", data do
    {endpoint, _} = WireServer.fixture([{200, [], @create}, {403, [], @denied}, {204, [], ""}])

    assert {:error, %Error{details: %{journal_id: id, cleanup: "aborted"}}} =
             Multipart.upload(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               data.source,
               data.plan,
               state_root: data.journal_root
             )

    assert {:ok, %{"phase" => "failed", "cleanup" => "aborted"}} =
             Journal.get(data.journal_root, id)

    assert_receive {:wire_request, %{method: "POST"}}
    assert_receive {:wire_request, %{method: "PUT"}}
    assert_receive {:wire_request, %{method: "DELETE", target: "/bucket/key?uploadId=owned"}}
  end

  test "keep-incomplete retains upload and resume rejects changed input options", data do
    {endpoint, _} = WireServer.fixture([{200, [], @create}, {403, [], @denied}])
    context = %Context{endpoint: endpoint, unsigned: true}
    opts = [state_root: data.journal_root, keep_incomplete: true]

    assert {:error, %Error{details: %{journal_id: id, cleanup: "retained"}}} =
             Multipart.upload(context, "bucket", "key", data.source, data.plan, opts)

    assert {:error, %Error{code: :resume_conflict}} =
             Multipart.resume(
               context,
               id,
               Keyword.put(opts, :input, %{"ContentType" => "changed"})
             )

    refute_receive {:wire_request, %{method: "DELETE"}}
  end

  test "a lost completion response is inspectable and never blindly aborted or duplicated",
       data do
    {endpoint, _} =
      WireServer.start(fn listener, owner ->
        for response <- [
              {200, [], @create},
              {200, [{"ETag", "\"first\""}], ""},
              {200, [{"ETag", "\"second\""}], ""}
            ] do
          {:ok, socket} = :gen_tcp.accept(listener, 5_000)
          send(owner, {:wire_request, WireServer.request(socket)})
          {status, headers, body} = response
          WireServer.response(socket, status, headers, body)
          :gen_tcp.close(socket)
        end

        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        send(owner, {:wire_request, WireServer.request(socket)})
        :gen_tcp.close(socket)
      end)

    assert {:error, %Error{code: :completion_indeterminate, details: %{journal_id: id}}} =
             Multipart.upload(
               %Context{endpoint: endpoint, unsigned: true},
               "bucket",
               "key",
               data.source,
               data.plan,
               state_root: data.journal_root
             )

    assert {:ok, %{"phase" => "indeterminate"}} = Journal.get(data.journal_root, id)
    refute_receive {:wire_request, %{method: "DELETE"}}
  end

  test "resume reconciles complete remote parts and reuploads unacknowledged data", data do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], @create},
        {200, [{"ETag", "\"first\""}], ""},
        {403, [], @denied},
        {200, [],
         "<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><ETag>&quot;first&quot;</ETag><Size>5</Size></Part><Part><PartNumber>2</PartNumber><ETag>&quot;unacknowledged&quot;</ETag><Size>5</Size></Part></ListPartsResult>"},
        {200, [{"ETag", "\"second\""}], ""},
        {200, [], @complete},
        {200, [{"ETag", "\"done-2\""}, {"Content-Length", "10"}], ""}
      ])

    context = %Context{endpoint: endpoint, unsigned: true}
    opts = [state_root: data.journal_root, keep_incomplete: true]

    assert {:error, %Error{details: %{journal_id: id}}} =
             Multipart.upload(context, "bucket", "key", data.source, data.plan, opts)

    assert {:ok, %{journal_id: ^id, bytes: 10}} = Multipart.resume(context, id, opts)

    for method <- ["POST", "PUT", "PUT", "GET", "PUT", "POST", "HEAD"] do
      assert_receive {:wire_request, %{method: ^method}}
    end
  end
end
