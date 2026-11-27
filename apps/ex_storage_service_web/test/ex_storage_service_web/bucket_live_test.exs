defmodule ExStorageServiceWeb.BucketLiveTest do
  use ExStorageServiceWeb.ConnCase

  import Phoenix.LiveViewTest

  alias ExStorageService.CloudCache.Config, as: CloudConfig
  alias ExStorageService.CrossClusterReplication.Config, as: ReplicationConfig
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.ObjectService
  alias ExStorageService.Storage.{Multipart, Versioning}

  defmodule ListingUpstream do
    import Plug.Conn

    def init(agent), do: agent

    def call(conn, agent) do
      conn = fetch_query_params(conn)

      state =
        Agent.get_and_update(agent, fn state ->
          {state, %{state | requests: [conn.query_params | state.requests]}}
        end)

      {entries, truncated, token} =
        case conn.query_params do
          %{"prefix" => "z-folder/"} ->
            {"<Contents><Key>z-folder/child</Key><Size>1</Size></Contents>", false, ""}

          %{"continuation-token" => "opaque+/=& value"} ->
            token = if state.repeat_token, do: "opaque+/=&amp; value", else: ""

            {"<CommonPrefixes><Prefix>z-folder/</Prefix></CommonPrefixes><Contents><Key>z-last</Key><Size>1</Size></Contents>",
             state.repeat_token, token}

          _ ->
            token = if state.invalid_token, do: "", else: "opaque+/=&amp; value"

            {"<CommonPrefixes><Prefix>a-folder/</Prefix></CommonPrefixes><Contents><Key>a-first</Key><Size>1</Size></Contents>",
             true, token}
        end

      send_resp(
        conn,
        200,
        "<ListBucketResult><IsTruncated>#{truncated}</IsTruncated><NextContinuationToken>#{token}</NextContinuationToken>#{entries}</ListBucketResult>"
      )
    end
  end

  setup %{conn: conn} do
    conn =
      conn
      |> post("/login", %{"username" => "admin", "password" => "admin"})
      |> recycle()

    %{conn: conn}
  end

  test "admin object deletion publishes its durable replication event and refreshes other views",
       %{conn: conn} do
    bucket = bucket()
    put_object(bucket, "object")

    assert :ok =
             ReplicationConfig.set_bucket_replicas(bucket, [
               %{endpoint: "http://127.0.0.1:1", access_key: "fixture", bucket: "replica"}
             ])

    Phoenix.PubSub.subscribe(ExStorageService.PubSub, "bucket:#{bucket}")
    {:ok, view, _} = live(conn, "/buckets/#{bucket}/files")
    {:ok, other_view, _} = live(conn, "/buckets/#{bucket}/files")
    assert has_element?(other_view, "button[phx-value-key=object]")

    view |> element("button[phx-value-key=object]") |> render_click()
    html = view |> element("#confirm-modal-overlay button", "Delete") |> render_click()
    assert html =~ "Deleted"
    refute has_element?(view, "button[phx-value-key=object]")
    assert_receive {:bucket_changed, %{action: :delete, key: "object"}}
    refute has_element?(other_view, "button[phx-value-key=object]")

    assert {:ok, operations} = Backend.prefix_scan(Keys.outbox_prefix())

    assert Enum.any?(operations, fn {_key, operation} ->
             Enum.any?(Map.get(operation, :events, []), fn event ->
               event.kind == :cross_cluster_delete and event.payload.bucket == bucket and
                 event.payload.key == "object"
             end)
           end)
  end

  test "failed object deletion keeps the object visible and reports the failure", %{conn: conn} do
    bucket = bucket()
    put_object(bucket, "object")
    {:ok, view, _} = live(conn, "/buckets/#{bucket}/files")
    view |> element("button[phx-value-key=object]") |> render_click()
    assert {:ok, %{value: metadata}} = Backend.get("bucket:#{bucket}")
    assert :ok = Backend.delete("bucket:#{bucket}")

    try do
      html = view |> element("#confirm-modal-overlay button", "Delete") |> render_click()
      assert html =~ "Failed to delete"
      refute html =~ "Deleted"
      assert has_element?(view, "button[phx-value-key=object]")
    after
      assert :ok = Backend.put("bucket:#{bucket}", metadata)
    end
  end

  for pending <- [:versions, :multipart], page <- [:index, :settings] do
    @pending pending
    @page page
    test "#{page} rejects bucket deletion with retained #{pending}", %{conn: conn} do
      bucket = bucket()

      case @pending do
        :versions ->
          assert :ok = Versioning.set_versioning(bucket, :enabled)
          put_object(bucket, "object")
          assert {:ok, _, :delete_marker} = Versioning.delete_version(bucket, "object")

        :multipart ->
          assert {:ok, upload_id} = Multipart.init_upload(bucket, "object")
          on_exit(fn -> Multipart.abort_upload(bucket, upload_id) end)
      end

      assert {:ok, %{keys: []}} = Metadata.list_objects(bucket)
      path = if @page == :index, do: "/buckets", else: "/buckets/#{bucket}/settings"
      {:ok, view, _} = live(conn, path)
      event = if @page == :index, do: "delete_bucket", else: "confirm_delete_bucket"
      html = render_click(view, event, %{"name" => bucket})

      assert html =~ "not empty"
      refute html =~ "deleted"
      assert {:ok, _} = Metadata.get_bucket(bucket)
    end
  end

  test "an empty bucket can be deleted through its settings", %{conn: conn} do
    bucket = bucket()
    {:ok, view, _} = live(conn, "/buckets/#{bucket}/settings")
    render_click(view, "confirm_delete_bucket")
    assert_redirect(view, "/buckets")
    assert {:error, :not_found} = Metadata.get_bucket(bucket)
  end

  test "hosted files paginate objects and folders and reset the cursor when navigating",
       %{conn: conn} do
    bucket = bucket()

    for number <- 1..100 do
      put_object(bucket, "a-" <> String.pad_leading(Integer.to_string(number), 3, "0"))
    end

    put_object(bucket, "z-folder/child")
    put_object(bucket, "z-last")
    {:ok, view, _} = live(conn, "/buckets/#{bucket}/files")

    assert has_element?(view, "button[phx-value-key=a-001]")
    refute has_element?(view, "button[phx-value-key=z-last]")
    assert has_element?(view, "#next-file-page")

    view |> element("#next-file-page") |> render_click()
    page_path = assert_patch(view)
    assert has_element?(view, "button[phx-value-key=z-last]")
    assert has_element?(view, "tr[phx-value-prefix='z-folder/']")
    refute has_element?(view, "button[phx-value-key=a-001]")
    refute has_element?(view, "#next-file-page")

    {:ok, reloaded, _} = live(conn, page_path)
    assert has_element?(reloaded, "button[phx-value-key=z-last]")
    refute has_element?(reloaded, "button[phx-value-key=a-001]")

    view |> element("#refresh-files") |> render_click()
    assert has_element?(view, "button[phx-value-key=z-last]")

    send(view.pid, {:bucket_changed, %{action: :put, key: "a-001"}})
    assert render(view) =~ "z-last"
    refute has_element?(view, "button[phx-value-key=a-001]")

    view |> element("tr[phx-value-prefix='z-folder/']") |> render_click()
    assert_patch(view, "/buckets/#{bucket}/files?prefix=z-folder%2F")
    assert has_element?(view, "button[phx-value-key='z-folder/child']")
    refute has_element?(view, "#first-file-page")

    view |> element("tr[phx-click=navigate_up]") |> render_click()
    assert has_element?(view, "button[phx-value-key=a-001]")
  end

  test "an exhausted page can navigate back to objects in the same bucket", %{conn: conn} do
    bucket = bucket()
    put_object(bucket, "only")
    {:ok, view, html} = live(conn, "/buckets/#{bucket}/files?continuation_token=only")
    assert html =~ "No entries on this page."
    refute html =~ "No objects in this bucket."
    view |> element("#first-file-page") |> render_click()
    assert has_element?(view, "button[phx-value-key=only]")
  end

  test "gateway pagination preserves opaque tokens across refresh and resets them for folders",
       %{conn: conn} do
    {bucket, agent} = gateway()
    {:ok, view, _} = live(conn, "/buckets/#{bucket}/files")
    assert has_element?(view, "td", "a-first")
    refute has_element?(view, "td", "z-last")

    view |> element("#next-file-page") |> render_click()
    page_path = assert_patch(view)
    assert has_element?(view, "td", "z-last")
    assert last_query(agent)["continuation-token"] == "opaque+/=& value"
    assert last_query(agent)["max-keys"] == "100"

    {:ok, reloaded, _} = live(conn, page_path)
    assert has_element?(reloaded, "td", "z-last")
    assert last_query(agent)["continuation-token"] == "opaque+/=& value"

    view |> element("#refresh-files") |> render_click()
    assert has_element?(view, "td", "z-last")
    assert last_query(agent)["continuation-token"] == "opaque+/=& value"

    send(view.pid, {:bucket_changed, %{action: :put, key: "z-last"}})
    assert render(view) =~ "z-last"
    assert last_query(agent)["continuation-token"] == "opaque+/=& value"

    view |> element("#first-file-page") |> render_click()
    assert has_element?(view, "td", "a-first")
    refute last_query(agent)["continuation-token"]

    view |> element("#next-file-page") |> render_click()
    view |> element("tr[phx-value-prefix='z-folder/']") |> render_click()
    assert has_element?(view, "td", "child")
    assert last_query(agent)["prefix"] == "z-folder/"
    refute last_query(agent)["continuation-token"]

    render_click(view, "confirm_delete_object", %{"key" => "z-folder/child"})
    assert has_element?(view, "td", "child")
    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "z-folder/child")
  end

  test "gateway truncation without a continuation token reports an error", %{conn: conn} do
    {bucket, agent} = gateway()
    Agent.update(agent, &%{&1 | invalid_token: true})
    {:ok, view, html} = live(conn, "/buckets/#{bucket}/files")

    assert html =~ "continuation token"
    assert has_element?(view, "td", "a-first")
    refute has_element?(view, "#next-file-page")
  end

  test "a gateway cannot advance pagination with a repeated continuation token", %{conn: conn} do
    {bucket, agent} = gateway()
    Agent.update(agent, &%{&1 | repeat_token: true})
    {:ok, view, _} = live(conn, "/buckets/#{bucket}/files")
    html = view |> element("#next-file-page") |> render_click()

    assert html =~ "continuation token"
    assert has_element?(view, "td", "z-last")
    refute has_element?(view, "#next-file-page")
    assert has_element?(view, "#first-file-page")
  end

  defp bucket do
    bucket = "web-bucket-#{System.unique_integer([:positive, :monotonic])}"
    assert :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      ReplicationConfig.remove_bucket_replicas(bucket)
      Metadata.delete_bucket(bucket)
    end)

    bucket
  end

  defp put_object(bucket, key) do
    assert {:ok, _} = ObjectService.put(bucket, key, "fixture", "text/plain", %{})

    on_exit(fn ->
      case Versioning.list_versions(bucket, key) do
        {:ok, versions} ->
          Enum.each(versions, &Versioning.delete_version(bucket, key, &1.version_id))

        _ ->
          :ok
      end
    end)
  end

  defp gateway do
    bucket = bucket()

    agent =
      start_supervised!(
        {Agent, fn -> %{requests: [], invalid_token: false, repeat_token: false} end}
      )

    server =
      start_supervised!(
        {Gaoxy.HTTP, plug: {ListingUpstream, agent}, port: 0, limits: %{max_bytes: 4_194_304}}
      )

    %{local: {:ok, {_, port}}} = Gaoxy.info(server, :http)

    assert :ok =
             CloudConfig.set_config(bucket, %{
               enabled: true,
               provider: :s3_compat,
               endpoint: "http://localhost:#{port}",
               bucket: "remote",
               access_key_id: "fixture",
               secret_access_key: "fixture"
             })

    {bucket, agent}
  end

  defp last_query(agent), do: Agent.get(agent, &hd(&1.requests))
end
