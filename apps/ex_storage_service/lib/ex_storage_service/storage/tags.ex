defmodule ExStorageService.Storage.Tags do
  @moduledoc "Validated S3 tags and bucket tagging persisted with the bucket fence."
  alias ExStorageService.Metadata.BucketConfig

  def validate(tags, limit \\ 10) do
    pairs = if is_map(tags), do: Map.to_list(tags), else: tags

    if is_list(pairs) and length(pairs) <= limit and
         Enum.all?(pairs, &valid_pair?/1) and
         length(Enum.uniq_by(pairs, &elem(&1, 0))) == length(pairs) do
      {:ok, Map.new(pairs)}
    else
      {:error, :invalid_tag}
    end
  end

  def from_header(header) when is_binary(header) and byte_size(header) <= 16_384 do
    header |> URI.query_decoder() |> Enum.to_list() |> validate()
  rescue
    ArgumentError -> {:error, :invalid_tag}
  end

  def from_header(_), do: {:error, :invalid_tag}

  def get_bucket(bucket), do: Concord.get("bucket_tagging:#{bucket}", consistency: :linearizable)

  def put_bucket(bucket, tags, opts \\ []) do
    with {:ok, validated} <- validate(tags, 50),
         do: BucketConfig.put(bucket, "bucket_tagging", validated, opts)
  end

  def delete_bucket(bucket, opts \\ []),
    do: BucketConfig.delete(bucket, "bucket_tagging", opts)

  defp valid_pair?({key, value}) when is_binary(key) and is_binary(value) do
    String.valid?(key) and String.valid?(value) and
      String.length(key) in 1..128 and String.length(value) <= 256 and
      not String.starts_with?(key, "aws:") and
      Regex.match?(~r/\A[\p{L}\p{N}\p{Z} +\-=_.:\/@]*\z/u, key) and
      Regex.match?(~r/\A[\p{L}\p{N}\p{Z} +\-=_.:\/@]*\z/u, value)
  end

  defp valid_pair?(_), do: false
end
