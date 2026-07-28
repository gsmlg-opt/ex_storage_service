defmodule ExStorageService.Metadata.Keys do
  @moduledoc """
  Unambiguous version 2 metadata keys.

  Bucket names and object keys use URL-safe, unpadded Base64 so delimiters in
  user-controlled values cannot alter the key structure.
  """

  @prefix "ess:v2"

  @spec encode_component(binary()) :: binary()
  def encode_component(component) when is_binary(component) do
    Base.url_encode64(component, padding: false)
  end

  @spec decode_component(binary()) :: {:ok, binary()} | :error
  def decode_component(component) when is_binary(component) do
    Base.url_decode64(component, padding: false)
  end

  @spec object_head(binary(), binary()) :: binary()
  def object_head(bucket, key) do
    object_head_prefix() <> Enum.join([encode_component(bucket), encode_component(key)], ":")
  end

  @spec object_head_prefix() :: binary()
  def object_head_prefix, do: "#{@prefix}:object_head:"

  @spec object_version(binary(), binary(), binary()) :: binary()
  def object_version(bucket, key, version_id) do
    object_version_prefix(bucket, key) <> version_id
  end

  @spec object_version_prefix(binary(), binary()) :: binary()
  def object_version_prefix(bucket, key) do
    Enum.join([@prefix, "object_version", encode_component(bucket), encode_component(key)], ":") <>
      ":"
  end

  @spec blob(binary()) :: binary()
  def blob(sha256) when is_binary(sha256), do: "#{@prefix}:blob:#{sha256}"

  @spec blob_location(binary(), binary()) :: binary()
  def blob_location(sha256, node_id) when is_binary(sha256) and is_binary(node_id) do
    blob_location_prefix(sha256) <> encode_component(node_id)
  end

  @spec blob_location_prefix(binary()) :: binary()
  def blob_location_prefix(sha256) when is_binary(sha256),
    do: "#{@prefix}:blob_location:#{sha256}:"

  @spec cluster_node(binary()) :: binary()
  def cluster_node(node_id) when is_binary(node_id),
    do: cluster_node_prefix() <> encode_component(node_id)

  @spec cluster_node_prefix() :: binary()
  def cluster_node_prefix, do: "#{@prefix}:cluster_node:"

  @spec outbox(binary()) :: binary()
  def outbox(operation_id) when is_binary(operation_id),
    do: "#{@prefix}:outbox:#{operation_id}"

  @spec outbox_prefix() :: binary()
  def outbox_prefix, do: "#{@prefix}:outbox:"

  @spec job(binary()) :: binary()
  def job(job_id) when is_binary(job_id),
    do: job_prefix() <> encode_component(job_id)

  @spec job_prefix() :: binary()
  def job_prefix, do: "#{@prefix}:job:"

  @spec multipart_upload(binary()) :: binary()
  def multipart_upload(upload_id) when is_binary(upload_id),
    do: multipart_upload_prefix() <> encode_component(upload_id)

  @spec multipart_upload_prefix() :: binary()
  def multipart_upload_prefix, do: "#{@prefix}:multipart_upload:"

  @spec multipart_part(binary(), pos_integer()) :: binary()
  def multipart_part(upload_id, part_number)
      when is_binary(upload_id) and is_integer(part_number) and part_number > 0 do
    multipart_part_prefix(upload_id) <> Integer.to_string(part_number)
  end

  @spec multipart_part_prefix(binary()) :: binary()
  def multipart_part_prefix(upload_id) when is_binary(upload_id),
    do: "#{@prefix}:multipart_part:#{encode_component(upload_id)}:"
end
