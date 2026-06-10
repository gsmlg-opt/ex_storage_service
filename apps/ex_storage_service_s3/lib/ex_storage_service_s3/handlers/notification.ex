defmodule ExStorageServiceS3.Handlers.Notification do
  @moduledoc false

  import Plug.Conn
  require Logger
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageServiceS3.XML
  alias ExStorageService.Metadata
  alias ExStorageService.Notifications

  def put_bucket_notification(conn, bucket) do
    request_id = request_id(conn)

    case Metadata.head_bucket(bucket) do
      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchBucket",
          "The specified bucket does not exist.",
          "/#{bucket}",
          request_id
        )

      :ok ->
        case read_full_body(conn) do
          {:ok, body, _conn} ->
            case parse_notification_xml(body) do
              {:ok, configs} ->
                Notifications.put_config(bucket, configs)

                conn
                |> put_s3_headers(request_id)
                |> send_resp(200, "")

              {:error, _} ->
                error_response(
                  conn,
                  "MalformedXML",
                  "The XML you provided was not well-formed.",
                  "/#{bucket}?notification",
                  request_id
                )
            end

          {:error, reason} ->
            error_response(
              conn,
              "InternalError",
              inspect(reason),
              "/#{bucket}?notification",
              request_id
            )
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  def get_bucket_notification(conn, bucket) do
    request_id = request_id(conn)

    case Metadata.head_bucket(bucket) do
      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchBucket",
          "The specified bucket does not exist.",
          "/#{bucket}",
          request_id
        )

      :ok ->
        case Notifications.get_config(bucket) do
          {:ok, configs} ->
            body = build_notification_xml(configs)
            xml_response(conn, 200, body, request_id)

          {:error, :not_found} ->
            body = """
            <?xml version="1.0" encoding="UTF-8"?>
            <NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></NotificationConfiguration>
            """

            xml_response(conn, 200, String.trim(body), request_id)
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  def delete_bucket_notification(conn, bucket) do
    request_id = request_id(conn)
    Notifications.delete_config(bucket)

    conn
    |> put_s3_headers(request_id)
    |> send_resp(204, "")
  end

  # Private helpers

  # Returns {:ok, cloud_config} if cloud cache is active for bucket, :disabled otherwise.
  defp parse_notification_xml(xml_body) do
    try do
      {doc, _} = :xmerl_scan.string(String.to_charlist(xml_body))

      configs =
        :xmerl_xpath.string(~c"//TopicConfiguration", doc)
        |> Enum.map(fn config_elem ->
          id = xpath_text(config_elem, ~c"Id") || ""
          endpoint = xpath_text(config_elem, ~c"Topic") || ""

          events =
            :xmerl_xpath.string(~c"Event/text()", config_elem)
            |> Enum.map(fn {:xmlText, _, _, _, value, _} -> to_string(value) end)

          %{
            id: id,
            endpoint: endpoint,
            events: events,
            enabled: true
          }
        end)

      {:ok, configs}
    rescue
      _ -> {:error, :malformed_xml}
    catch
      :exit, _ -> {:error, :malformed_xml}
    end
  end

  defp build_notification_xml(configs) do
    config_elements =
      Enum.map(configs, fn config ->
        events =
          Enum.map(Map.get(config, :events, []), fn event ->
            "<Event>#{XML.escape(event)}</Event>"
          end)
          |> Enum.join()

        """
        <TopicConfiguration>\
        <Id>#{XML.escape(Map.get(config, :id, ""))}</Id>\
        <Topic>#{XML.escape(Map.get(config, :endpoint, ""))}</Topic>\
        #{events}\
        </TopicConfiguration>\
        """
      end)
      |> Enum.join()

    """
    <?xml version="1.0" encoding="UTF-8"?>\
    <NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">#{config_elements}</NotificationConfiguration>\
    """
  end
end
