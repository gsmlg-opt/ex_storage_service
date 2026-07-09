defmodule ExStorageServiceS3.Handlers.Lifecycle do
  @moduledoc false

  import Plug.Conn
  require Logger
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageServiceS3.XML
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Lifecycle

  def put_bucket_lifecycle(conn, bucket) do
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
            case parse_lifecycle_xml(body) do
              {:ok, rules} ->
                Lifecycle.put_rules(bucket, rules)

                conn
                |> put_s3_headers(request_id)
                |> send_resp(200, "")

              {:error, _} ->
                error_response(
                  conn,
                  "MalformedXML",
                  "The XML you provided was not well-formed.",
                  "/#{bucket}?lifecycle",
                  request_id
                )
            end

          {:error, reason} ->
            error_response(
              conn,
              "InternalError",
              inspect(reason),
              "/#{bucket}?lifecycle",
              request_id
            )
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  def get_bucket_lifecycle(conn, bucket) do
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
        case Lifecycle.get_rules(bucket) do
          {:ok, rules} ->
            body = build_lifecycle_xml(rules)
            xml_response(conn, 200, body, request_id)

          {:error, :not_found} ->
            error_response(
              conn,
              "NoSuchLifecycleConfiguration",
              "The lifecycle configuration does not exist.",
              "/#{bucket}?lifecycle",
              request_id
            )
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  def delete_bucket_lifecycle(conn, bucket) do
    request_id = request_id(conn)
    Lifecycle.delete_rules(bucket)

    conn
    |> put_s3_headers(request_id)
    |> send_resp(204, "")
  end

  ## Notification handlers

  defp parse_lifecycle_xml(xml_body) do
    if xml_has_doctype?(xml_body) do
      {:error, :malformed_xml}
    else
      do_parse_lifecycle_xml(xml_body)
    end
  end

  defp do_parse_lifecycle_xml(xml_body) do
    try do
      {doc, _} = :xmerl_scan.string(String.to_charlist(xml_body))

      rules =
        :xmerl_xpath.string(~c"//Rule", doc)
        |> Enum.map(fn rule_elem ->
          id = xpath_text(rule_elem, ~c"ID")

          prefix =
            xpath_text(rule_elem, ~c"Filter/Prefix") || xpath_text(rule_elem, ~c"Prefix") || ""

          status = xpath_text(rule_elem, ~c"Status") || "Enabled"
          days_str = xpath_text(rule_elem, ~c"Expiration/Days") || "0"
          days = String.to_integer(days_str)
          transition_days_str = xpath_text(rule_elem, ~c"Transition/Days") || "0"
          transition_days = String.to_integer(transition_days_str)

          transition_storage_class =
            xpath_text(rule_elem, ~c"Transition/StorageClass") || "PACKED"

          %{
            id: id || "",
            prefix: prefix,
            status: status,
            expiration_days: days,
            transition_days: transition_days,
            transition_storage_class: transition_storage_class
          }
        end)

      {:ok, rules}
    rescue
      _ -> {:error, :malformed_xml}
    catch
      :exit, _ -> {:error, :malformed_xml}
    end
  end

  defp build_lifecycle_xml(rules) do
    rule_elements =
      Enum.map(rules, fn rule ->
        id = Map.get(rule, :id, "")
        prefix = Map.get(rule, :prefix, "")
        status = Map.get(rule, :status, "Enabled")
        days = Map.get(rule, :expiration_days, 0)
        transition_days = Map.get(rule, :transition_days, 0)
        transition_storage_class = Map.get(rule, :transition_storage_class, "PACKED")
        expiration_xml = lifecycle_expiration_xml(days)
        transition_xml = lifecycle_transition_xml(transition_days, transition_storage_class)

        """
        <Rule>\
        <ID>#{XML.escape(id)}</ID>\
        <Filter><Prefix>#{XML.escape(prefix)}</Prefix></Filter>\
        <Status>#{XML.escape(status)}</Status>\
        #{expiration_xml}\
        #{transition_xml}\
        </Rule>\
        """
      end)
      |> Enum.join()

    """
    <?xml version="1.0" encoding="UTF-8"?>\
    <LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">#{rule_elements}</LifecycleConfiguration>\
    """
  end

  defp lifecycle_expiration_xml(days) when is_integer(days) and days > 0,
    do: "<Expiration><Days>#{days}</Days></Expiration>"

  defp lifecycle_expiration_xml(_days), do: ""

  defp lifecycle_transition_xml(days, storage_class) when is_integer(days) and days > 0 do
    "<Transition><Days>#{days}</Days><StorageClass>#{XML.escape(storage_class)}</StorageClass></Transition>"
  end

  defp lifecycle_transition_xml(_days, _storage_class), do: ""
end
