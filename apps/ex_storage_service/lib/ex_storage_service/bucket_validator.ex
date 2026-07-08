defmodule ExStorageService.BucketValidator do
  @moduledoc """
  Validates S3 bucket names and filesystem path segments.

  Enforces the same naming rules as AWS S3:
  - 3–63 characters
  - Lowercase letters, numbers, hyphens, dots only
  - Must start and end with a letter or number
  - Cannot be formatted as an IP address
  - Cannot contain consecutive dots or dot-hyphen/hyphen-dot sequences

  Also rejects names that could escape the data root directory via path traversal.
  """

  # Directory names under data_root reserved for internal storage layouts
  # (see docs/prd/git-style-data-model.md §6).
  @reserved_names ["cas"]

  @doc """
  Returns `true` if the bucket name is valid according to S3 rules and is
  safe to use as a filesystem directory name.

  ## Examples

      iex> BucketValidator.valid_bucket_name?("my-bucket")
      true

      iex> BucketValidator.valid_bucket_name?("../escape")
      false

      iex> BucketValidator.valid_bucket_name?("192.168.1.1")
      false

  """
  @spec valid_bucket_name?(String.t()) :: boolean()
  def valid_bucket_name?(name) when is_binary(name) do
    valid_length?(name) and
      valid_chars?(name) and
      valid_start_end?(name) and
      no_consecutive_dots?(name) and
      no_dot_hyphen_sequences?(name) and
      not ip_address_format?(name) and
      not reserved_name?(name) and
      path_safe?(name)
  end

  def valid_bucket_name?(_), do: false

  @doc """
  Returns a human-readable error message for an invalid bucket name, or
  `:ok` if the name is valid.
  """
  @spec validate(String.t()) :: :ok | {:error, String.t()}
  def validate(name) when is_binary(name) do
    cond do
      not valid_length?(name) ->
        {:error,
         "Bucket name must be between 3 and 63 characters long. Got #{byte_size(name)} characters."}

      not valid_chars?(name) ->
        {:error, "Bucket name can only contain lowercase letters, numbers, hyphens, and dots."}

      not valid_start_end?(name) ->
        {:error, "Bucket name must start and end with a lowercase letter or number."}

      not no_consecutive_dots?(name) ->
        {:error, "Bucket name must not contain consecutive dots."}

      not no_dot_hyphen_sequences?(name) ->
        {:error, "Bucket name must not contain '.-' or '-.'."}

      ip_address_format?(name) ->
        {:error, "Bucket name must not be formatted as an IP address."}

      reserved_name?(name) ->
        {:error, "Bucket name \"#{name}\" is reserved for internal use."}

      not path_safe?(name) ->
        {:error, "Bucket name contains characters not safe for use in filesystem paths."}

      true ->
        :ok
    end
  end

  def validate(_), do: {:error, "Bucket name must be a string."}

  # ── Private helpers ──────────────────────────────────────────────────────────

  defp valid_length?(name) do
    len = byte_size(name)
    len >= 3 and len <= 63
  end

  defp valid_chars?(name) do
    # Only lowercase letters, digits, hyphens, dots
    String.match?(name, ~r/^[a-z0-9.\-]+$/)
  end

  defp valid_start_end?(name) do
    first = String.first(name)
    last = String.last(name)
    Regex.match?(~r/^[a-z0-9]$/, first) and Regex.match?(~r/^[a-z0-9]$/, last)
  end

  defp no_consecutive_dots?(name), do: not String.contains?(name, "..")

  defp no_dot_hyphen_sequences?(name) do
    not String.contains?(name, ".-") and not String.contains?(name, "-.")
  end

  defp ip_address_format?(name) do
    Regex.match?(~r/^\d+\.\d+\.\d+\.\d+$/, name)
  end

  defp reserved_name?(name), do: name in @reserved_names

  # Reject any name that contains path traversal sequences or filesystem-unsafe
  # characters, even if encoded.  We check the raw name after it has been
  # extracted from the URL path by Plug, so URL-encoding should already be
  # decoded.
  defp path_safe?(name) do
    # Reject any control characters (ASCII 0–31)
    not String.contains?(name, "/") and
      not String.contains?(name, "\\") and
      not String.contains?(name, "\0") and
      not String.contains?(name, "%2e") and
      not String.contains?(name, "%2f") and
      not String.contains?(name, "%5c") and
      not (name == ".") and
      not (name == "..") and
      not Regex.match?(~r/[\x00-\x1f]/, name)
  end
end
