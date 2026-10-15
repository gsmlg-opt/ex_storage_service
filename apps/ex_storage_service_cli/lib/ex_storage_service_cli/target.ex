defmodule ExStorageServiceCli.Target do
  @moduledoc """
  Parses targets without treating S3 keys as filesystem paths or decoding their bytes.
  Alias/local collisions require an explicit `ess://` or local path.
  """
  alias ExStorageServiceCli.Error
  defstruct [:kind, :original, :alias, :bucket, :key, :path, :arn, trailing_slash: false]

  def parse(value, opts \\ []) when is_binary(value) do
    aliases = Keyword.get(opts, :aliases, [])

    cond do
      value == "-" ->
        if Keyword.get(opts, :stdio, false),
          do: {:ok, %__MODULE__{kind: :stdio, original: value}},
          else: invalid("This command does not accept stdin/stdout targets")

      String.starts_with?(value, "ess://") ->
        alias_target(value, String.replace_prefix(value, "ess://", ""), aliases)

      String.starts_with?(value, "s3://") ->
        remote(value, String.replace_prefix(value, "s3://", ""))

      String.starts_with?(value, "arn:") ->
        remote(value, value)

      explicit_local?(value) ->
        path = String.replace_prefix(value, "file://", "")

        {:ok,
         %__MODULE__{kind: :local, original: value, path: path, trailing_slash: trailing?(value)}}

      true ->
        [first | _] = String.split(value, "/", parts: 2)

        cond do
          first in aliases ->
            exists? = Keyword.get(opts, :local_exists?, &File.exists?/1)

            if exists?.(first),
              do: invalid("Alias and local path collide; use ess:// or an explicit local path"),
              else: alias_target(value, value, aliases)

          Keyword.get(opts, :legacy, false) ->
            remote(value, value)

          value == "" ->
            invalid("Target must not be empty")

          true ->
            {:ok,
             %__MODULE__{
               kind: :local,
               original: value,
               path: value,
               trailing_slash: trailing?(value)
             }}
        end
    end
  end

  defp alias_target(original, value, aliases) do
    [name | rest] = String.split(value, "/", parts: 2)

    if name in aliases do
      case rest do
        [path] -> with {:ok, target} <- remote(original, path), do: {:ok, %{target | alias: name}}
        [] -> {:ok, %__MODULE__{kind: :alias, original: original, alias: name}}
      end
    else
      invalid("Unknown or malformed alias target")
    end
  end

  defp remote(original, "arn:" <> _ = value) do
    case Regex.run(
           ~r/^(arn:[^:]+:s3(?:-outposts|-object-lambda)?:[^:]*:[^:]+:(?:accesspoint\/[^\/]+|outpost\/[^\/]+\/accesspoint\/[^\/]+))(?:\/(.*))?$/s,
           value
         ) do
      [_, arn, key] ->
        {:ok,
         %__MODULE__{
           kind: :remote,
           original: original,
           bucket: arn,
           arn: arn,
           key: key,
           trailing_slash: trailing?(value)
         }}

      [_, arn] ->
        {:ok, %__MODULE__{kind: :remote, original: original, bucket: arn, arn: arn, key: ""}}

      _ ->
        invalid("Malformed S3 access-point ARN target")
    end
  end

  defp remote(original, value) do
    case String.split(value, "/", parts: 2) do
      [""] ->
        invalid("S3 bucket must not be empty")

      ["", _] ->
        invalid("S3 bucket must not be empty")

      [bucket] ->
        {:ok, %__MODULE__{kind: :remote, original: original, bucket: bucket, key: ""}}

      [bucket, key] ->
        {:ok,
         %__MODULE__{
           kind: :remote,
           original: original,
           bucket: bucket,
           key: key,
           trailing_slash: trailing?(value)
         }}
    end
  end

  defp explicit_local?(value) do
    String.starts_with?(value, ["./", "../", "/", "file://", "\\\\"]) or
      Regex.match?(~r/^[A-Za-z]:[\\\/]/, value)
  end

  defp trailing?(value), do: String.ends_with?(value, "/")
  defp invalid(message), do: {:error, Error.new(:syntax, message)}
end
