defmodule ExStorageServiceS3.Handlers.Tagging do
  @moduledoc false
  import Plug.Conn
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageService.{Metadata, Storage.Tags}
  alias ExStorageService.Metadata.{BucketLifecycle, ObjectCommit}
  alias ExStorageService.CloudCache.Config
  alias ExStorageServiceS3.{XML, Handlers.ControlXML}

  @max_body 131_072

  def get_bucket_tagging(conn, bucket) do
    with {:ok, _} <- bucket_snapshot(bucket),
         {:ok, tags} <- Tags.get_bucket(bucket) do
      xml_response(conn, 200, document(tags), request_id(conn))
    else
      {:error, :not_found} -> failure(conn, :no_tag_set, bucket)
      {:ok, nil} -> failure(conn, :no_tag_set, bucket)
      {:error, reason} -> failure(conn, reason, bucket)
    end
  end

  def put_bucket_tagging(conn, bucket) do
    with {:ok, meta} <- bucket_snapshot(bucket),
         {:ok, body, conn} <- read_full_body(conn, @max_body),
         {:ok, tags} <- parse(body, 50),
         :ok <- Tags.put_bucket(bucket, tags, bucket_generation: BucketLifecycle.generation(meta)) do
      conn |> put_s3_headers(request_id(conn)) |> send_resp(204, "")
    else
      {:error, reason} -> failure(conn, reason, bucket)
    end
  end

  def delete_bucket_tagging(conn, bucket) do
    case Tags.delete_bucket(bucket) do
      :ok -> conn |> put_s3_headers(request_id(conn)) |> send_resp(204, "")
      {:error, reason} -> failure(conn, reason, bucket)
    end
  end

  def get_object_tagging(conn, bucket, key) do
    with :ok <- hosted_bucket(bucket),
         {:ok, %{tags: tags, version_id: version}} <-
           ObjectCommit.get_tags(bucket, key, version(conn)) do
      conn |> version_header(version) |> xml_response(200, document(tags), request_id(conn))
    else
      {:error, reason} -> failure(conn, reason, bucket)
    end
  end

  def put_object_tagging(conn, bucket, key) do
    with :ok <- hosted_bucket(bucket),
         {:ok, meta} <- bucket_snapshot(bucket),
         {:ok, body, conn} <- read_full_body(conn, @max_body),
         {:ok, tags} <- parse(body, 10),
         {:ok, %{version_id: version}} <-
           ObjectCommit.set_tags(bucket, key, version(conn), tags,
             bucket_generation: BucketLifecycle.generation(meta)
           ) do
      conn |> version_header(version) |> put_s3_headers(request_id(conn)) |> send_resp(200, "")
    else
      {:error, reason} -> failure(conn, reason, bucket)
    end
  end

  def delete_object_tagging(conn, bucket, key) do
    with :ok <- hosted_bucket(bucket),
         {:ok, %{version_id: version}} <- ObjectCommit.set_tags(bucket, key, version(conn), %{}) do
      conn |> version_header(version) |> put_s3_headers(request_id(conn)) |> send_resp(204, "")
    else
      {:error, reason} -> failure(conn, reason, bucket)
    end
  end

  def parse(body, limit \\ 10) do
    with {:ok, root} <- ControlXML.parse(body, "Tagging", ~w(Tagging TagSet Tag Key Value)),
         [%{children: entries} = set] <- root.children,
         true <-
           set.name == "TagSet" and String.trim(root.text) == "" and String.trim(set.text) == "",
         {:ok, pairs} <- tag_pairs(entries) do
      Tags.validate(pairs, limit)
    else
      _ -> {:error, :malformed_xml}
    end
  end

  def document(tags) do
    elements =
      tags
      |> Enum.sort()
      |> Enum.map(fn {key, value} ->
        ["<Tag><Key>", XML.escape(key), "</Key><Value>", XML.escape(value), "</Value></Tag>"]
      end)

    IO.iodata_to_binary([
      ~s(<?xml version="1.0" encoding="UTF-8"?><Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet>),
      elements,
      "</TagSet></Tagging>"
    ])
  end

  defp tag_pairs(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, pairs} ->
      with true <-
             entry.name == "Tag" and length(entry.children) == 2 and String.trim(entry.text) == "",
           {:ok, key} <- ControlXML.text(entry, "Key"),
           {:ok, value} <- ControlXML.text(entry, "Value") do
        {:cont, {:ok, [{key, value} | pairs]}}
      else
        _ -> {:halt, {:error, :malformed_xml}}
      end
    end)
  end

  defp bucket_snapshot(bucket) do
    case Metadata.get_bucket(bucket) do
      {:error, :not_found} -> {:error, :bucket_not_found}
      result -> result
    end
  end

  defp hosted_bucket(bucket) do
    case Config.get_config(bucket) do
      {:ok, %{enabled: true}} -> {:error, :unsupported_gateway_tagging}
      {:ok, _} -> :ok
      {:error, :not_found} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp version(conn),
    do: conn.assigns[:s3_operation][:version_id] || conn.query_params["versionId"]

  defp version_header(conn, nil), do: conn
  defp version_header(conn, version), do: put_resp_header(conn, "x-amz-version-id", version)

  defp failure(conn, reason, bucket) do
    {code, message} =
      case reason do
        :no_tag_set ->
          {"NoSuchTagSet", "The TagSet does not exist."}

        :invalid_tag ->
          {"InvalidTag", "The tag set is invalid or exceeds its limit."}

        :malformed_xml ->
          {"MalformedXML", "The XML you provided was not well-formed."}

        :object_not_found ->
          {"NoSuchKey", "The specified key does not exist."}

        :version_not_found ->
          {"NoSuchVersion", "The specified version does not exist."}

        :delete_marker ->
          if version(conn),
            do: {"MethodNotAllowed", "Tagging is unavailable for delete markers."},
            else: {"NoSuchKey", "The specified key does not exist."}

        :unsupported_gateway_tagging ->
          {"NotImplemented", "Tagging is unavailable for gateway objects."}

        _ ->
          {nil, nil}
      end

    if code,
      do: error_response(conn, code, message, "/#{bucket}", request_id(conn)),
      else: storage_error_response(conn, reason, "/#{bucket}", request_id(conn))
  end
end
