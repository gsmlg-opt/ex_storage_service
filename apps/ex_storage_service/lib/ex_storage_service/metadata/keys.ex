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

  @spec outbox(binary()) :: binary()
  def outbox(operation_id) when is_binary(operation_id),
    do: "#{@prefix}:outbox:#{operation_id}"
end
