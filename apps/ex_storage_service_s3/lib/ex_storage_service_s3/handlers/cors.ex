defmodule ExStorageServiceS3.Handlers.Cors do
  @moduledoc false
  import Plug.Conn
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.BucketLifecycle
  alias ExStorageService.Storage.BucketCORS
  alias ExStorageServiceS3.{XML, Handlers.ControlXML}
  @max_body 65_536
  @fields ~w(ID AllowedOrigin AllowedMethod AllowedHeader ExposeHeader MaxAgeSeconds)

  def get_bucket_cors(conn, bucket) do
    with :ok <- Metadata.head_bucket(bucket), {:ok, rules} <- BucketCORS.get(bucket) do
      xml_response(conn, 200, document(rules), request_id(conn))
    else
      {:error, :not_found} ->
        case Metadata.head_bucket(bucket) do
          :ok ->
            error_response(
              conn,
              "NoSuchCORSConfiguration",
              "The CORS configuration does not exist.",
              "/#{bucket}",
              request_id(conn)
            )

          {:error, :not_found} ->
            storage_error_response(conn, :bucket_not_found, "/#{bucket}", request_id(conn))

          {:error, reason} ->
            storage_error_response(conn, reason, "/#{bucket}", request_id(conn))
        end

      {:ok, nil} ->
        error_response(
          conn,
          "NoSuchCORSConfiguration",
          "The CORS configuration does not exist.",
          "/#{bucket}",
          request_id(conn)
        )

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}", request_id(conn))
    end
  end

  def put_bucket_cors(conn, bucket) do
    with {:ok, meta} <- Metadata.get_bucket(bucket),
         {:ok, body, conn} <- read_full_body(conn, @max_body),
         {:ok, rules} <- parse(body),
         :ok <- BucketCORS.put(bucket, rules, bucket_generation: BucketLifecycle.generation(meta)) do
      conn |> put_s3_headers(request_id(conn)) |> send_resp(200, "")
    else
      {:error, :not_found} ->
        storage_error_response(conn, :bucket_not_found, "/#{bucket}", request_id(conn))

      {:error, :malformed_xml} ->
        error_response(
          conn,
          "MalformedXML",
          "The XML you provided was not well-formed.",
          "/#{bucket}",
          request_id(conn)
        )

      {:error, :invalid_cors} ->
        error_response(
          conn,
          "InvalidRequest",
          "The CORS configuration is invalid.",
          "/#{bucket}",
          request_id(conn)
        )

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}", request_id(conn))
    end
  end

  def delete_bucket_cors(conn, bucket) do
    case BucketCORS.delete(bucket) do
      :ok -> conn |> put_s3_headers(request_id(conn)) |> send_resp(204, "")
      {:error, reason} -> storage_error_response(conn, reason, "/#{bucket}", request_id(conn))
    end
  end

  def parse(body) do
    with {:ok, root} <-
           ControlXML.parse(body, "CORSConfiguration", ["CORSConfiguration", "CORSRule" | @fields]),
         true <- String.trim(root.text) == "",
         {:ok, rules} <- parse_rules(root.children) do
      BucketCORS.validate(rules)
    else
      _ -> {:error, :malformed_xml}
    end
  end

  def document(rules) do
    entries =
      Enum.map(rules, fn rule ->
        [
          "<CORSRule>",
          element("ID", rule.id),
          Enum.map(rule.allowed_origins, &element("AllowedOrigin", &1)),
          Enum.map(rule.allowed_methods, &element("AllowedMethod", &1)),
          Enum.map(rule.allowed_headers, &element("AllowedHeader", &1)),
          Enum.map(rule.expose_headers, &element("ExposeHeader", &1)),
          if(is_nil(rule.max_age_seconds),
            do: [],
            else: element("MaxAgeSeconds", to_string(rule.max_age_seconds))
          ),
          "</CORSRule>"
        ]
      end)

    IO.iodata_to_binary([
      ~s(<?xml version="1.0" encoding="UTF-8"?><CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">),
      entries,
      "</CORSConfiguration>"
    ])
  end

  defp parse_rules(nodes) do
    Enum.reduce_while(nodes, {:ok, []}, fn node, {:ok, rules} ->
      with true <-
             node.name == "CORSRule" and String.trim(node.text) == "" and
               Enum.all?(node.children, &(&1.name in @fields and &1.children == [])),
           {:ok, id} <- optional_text(node, "ID", ""),
           {:ok, age} <- optional_text(node, "MaxAgeSeconds", nil),
           {:ok, age} <- age(age) do
        rule = %{
          id: id,
          allowed_origins: values(node, "AllowedOrigin"),
          allowed_methods: values(node, "AllowedMethod"),
          allowed_headers: values(node, "AllowedHeader"),
          expose_headers: values(node, "ExposeHeader"),
          max_age_seconds: age
        }

        {:cont, {:ok, [rule | rules]}}
      else
        _ -> {:halt, {:error, :malformed_xml}}
      end
    end)
    |> case do
      {:ok, rules} -> {:ok, Enum.reverse(rules)}
      error -> error
    end
  end

  defp optional_text(node, name, default) do
    case ControlXML.children(node, name) do
      [] -> {:ok, default}
      [_] -> ControlXML.text(node, name)
      _ -> {:error, :malformed_xml}
    end
  end

  defp values(node, name), do: Enum.map(ControlXML.children(node, name), & &1.text)
  defp age(nil), do: {:ok, nil}

  defp age(value) do
    case Integer.parse(value) do
      {number, ""} when number >= 0 -> {:ok, number}
      _ -> {:error, :malformed_xml}
    end
  end

  defp element(_name, ""), do: []
  defp element(name, value), do: ["<", name, ">", XML.escape(value), "</", name, ">"]
end
