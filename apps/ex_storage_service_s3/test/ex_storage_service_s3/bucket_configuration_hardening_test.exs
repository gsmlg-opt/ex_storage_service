defmodule ExStorageServiceS3.BucketConfigurationHardeningTest do
  use ExUnit.Case, async: false
  alias ExStorageService.Metadata
  alias ExStorageServiceS3.Handlers.{Lifecycle, Notification, Versioning}

  defmodule Server do
    def init(opts), do: opts

    def call(conn, _opts) do
      [bucket, operation] = conn.path_info

      case operation do
        "versioning" -> Versioning.put_bucket_versioning(conn, bucket)
        "lifecycle" -> Lifecycle.put_bucket_lifecycle(conn, bucket)
        "notification" -> Notification.put_bucket_notification(conn, bucket)
      end
    end
  end

  test "buffered configurations reject oversized and malformed XML over HTTP without mutation" do
    bucket = "bounded-config-#{System.unique_integer([:positive])}"
    assert :ok = Metadata.create_bucket(bucket)
    assert :ok = ExStorageService.Storage.Versioning.set_versioning(bucket, :suspended)
    assert :ok = ExStorageService.Storage.Lifecycle.put_rules(bucket, [%{id: "retained"}])
    assert :ok = ExStorageService.Notifications.put_config(bucket, [%{id: "retained"}])
    on_exit(fn -> Metadata.delete_bucket(bucket) end)

    server = start_supervised!({Bandit, plug: Server, port: 0})
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    for {operation, prefix, xml} <- [
          {"versioning", "bucket_versioning",
           "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"},
          {"lifecycle", "lifecycle", "<LifecycleConfiguration/>"},
          {"notification", "notification", "<NotificationConfiguration/>"}
        ] do
      assert {:ok, original} = Concord.get("#{prefix}:#{bucket}")

      for {body, status, code} <- [
            {xml <> String.duplicate(" ", 1_048_576), 413, "EntityTooLarge"},
            {"<", 400, "MalformedXML"},
            {"<Configuration>", 400, "MalformedXML"},
            {"<!DOCTYPE root [<!ENTITY external SYSTEM 'file:///not-readable'>]>" <> xml, 400,
             "MalformedXML"},
            {<<255, 254>>, 400, "MalformedXML"}
          ] do
        assert {:ok, response} =
                 Req.put("http://localhost:#{port}/#{bucket}/#{operation}",
                   body: body,
                   decode_body: false,
                   retry: false
                 )

        assert response.status == status
        assert response.body =~ "<Code>#{code}</Code>"
        refute response.body =~ "not-readable"
        assert {:ok, ^original} = Concord.get("#{prefix}:#{bucket}")
      end
    end
  end

  defmodule RecreatingAdapter do
    def read_req_body(state, opts) do
      unless state.changed do
        :ok = Metadata.delete_bucket(state.bucket)
        :ok = Metadata.create_bucket(state.bucket)
      end

      {status, body, inner} = Plug.Adapters.Test.Conn.read_req_body(state.inner, opts)
      {status, body, %{state | inner: inner, changed: true}}
    end

    def send_resp(state, status, headers, body) do
      {:ok, body, inner} = Plug.Adapters.Test.Conn.send_resp(state.inner, status, headers, body)
      {:ok, body, %{state | inner: inner}}
    end

    def get_http_protocol(state), do: Plug.Adapters.Test.Conn.get_http_protocol(state.inner)
  end

  test "PUT config requests cannot mutate a bucket recreated while their bodies arrive" do
    for {handler, xml, prefix} <- [
          {&Versioning.put_bucket_versioning/2,
           "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
           "bucket_versioning"},
          {&Lifecycle.put_bucket_lifecycle/2,
           "<LifecycleConfiguration><Rule><ID>test</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
           "lifecycle"},
          {&Notification.put_bucket_notification/2, "<NotificationConfiguration/>",
           "notification"}
        ] do
      bucket = "stale-config-#{System.unique_integer([:positive])}"
      :ok = Metadata.create_bucket(bucket)
      conn = Plug.Test.conn(:put, "/#{bucket}", xml)
      {Plug.Adapters.Test.Conn, inner} = conn.adapter

      conn = %{
        conn
        | adapter: {RecreatingAdapter, %{inner: inner, bucket: bucket, changed: false}}
      }

      response = handler.(conn, bucket)
      assert response.status == 409
      assert response.resp_body =~ "ConditionalRequestConflict"
      assert {:error, :not_found} = Concord.get("#{prefix}:#{bucket}")
      assert :ok = Metadata.delete_bucket(bucket)
    end
  end

  test "DELETE configuration for a missing bucket reports absence instead of success" do
    bucket = "absent-config-#{System.unique_integer([:positive])}"

    for handler <- [
          &Lifecycle.delete_bucket_lifecycle/2,
          &Notification.delete_bucket_notification/2
        ] do
      response = handler.(Plug.Test.conn(:delete, "/#{bucket}"), bucket)
      assert response.status == 404
      assert response.resp_body =~ "NoSuchBucket"
    end
  end
end
