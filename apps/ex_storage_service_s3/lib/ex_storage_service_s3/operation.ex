defmodule ExStorageServiceS3.Operation do
  @moduledoc """
  Resolves a request once for both authorization and dispatch. The descriptor
  contains decoded domain identities; raw path, headers and query stay on the
  connection for signature verification.
  """

  @selectors ~w(acl retention legal-hold tagging versions versioning lifecycle notification location cors policy website logging replication encryption object-lock ownershipControls publicAccessBlock accelerate analytics inventory metrics requestPayment torrent restore select attributes uploads uploadId partNumber versionId delete)
  @excluded ~w(acl retention legal-hold policy website logging replication encryption object-lock ownershipControls publicAccessBlock accelerate analytics inventory metrics requestPayment torrent restore select attributes)
  @unsupported_headers ~w(x-amz-acl x-amz-server-side-encryption x-amz-server-side-encryption-customer-algorithm x-amz-object-lock-mode x-amz-object-lock-retain-until-date x-amz-object-lock-legal-hold)

  def resolve(conn) do
    pairs = URI.query_decoder(conn.query_string) |> Enum.to_list()
    selectors = Enum.filter(pairs, fn {key, _} -> key in @selectors end)
    params = Map.new(pairs)
    {bucket, key} = target(conn.request_path)

    with :ok <- validate_target(bucket, key),
         :ok <- validate_selectors(selectors, params),
         :ok <- validate_headers(conn.req_headers),
         {:ok, source} <- copy_source(conn),
         {:ok, name} <- identify(conn.method, bucket, key, params, source) do
      {:ok,
       %{
         name: name,
         bucket: bucket,
         key: key,
         version_id: params["versionId"],
         upload_id: params["uploadId"],
         part_number: params["partNumber"],
         copy_source: source,
         query_pairs: pairs
       }}
    end
  rescue
    ArgumentError -> invalid()
  end

  defp target("/"), do: {nil, nil}

  defp target(path) do
    case String.split(String.trim_leading(path, "/"), "/", parts: 2) do
      [bucket] -> {URI.decode(bucket), nil}
      [bucket, ""] -> {URI.decode(bucket), nil}
      [bucket, key] -> {URI.decode(bucket), URI.decode(key)}
    end
  end

  defp validate_target(bucket, key) do
    if Enum.all?([bucket, key], fn
         nil -> true
         value -> String.valid?(value) and not String.contains?(value, <<0>>)
       end) and (is_nil(key) or byte_size(key) <= 1024),
       do: :ok,
       else: invalid()
  end

  defp validate_selectors(selectors, params) do
    names = Enum.map(selectors, &elem(&1, 0))
    primary = names -- ["partNumber", "versionId"]

    cond do
      length(names) != length(Enum.uniq(names)) ->
        invalid()

      length(primary) > 1 ->
        invalid()

      Map.has_key?(params, "partNumber") and not Map.has_key?(params, "uploadId") ->
        invalid()

      Map.has_key?(params, "versionId") and primary not in [[], ["tagging"]] ->
        invalid()

      Enum.any?(~w(versionId uploadId), &(Map.has_key?(params, &1) and params[&1] == "")) ->
        invalid()

      Map.has_key?(params, "partNumber") and not valid_part?(params["partNumber"]) ->
        invalid()

      Enum.any?(names, &(&1 in @excluded)) ->
        unsupported()

      true ->
        :ok
    end
  end

  defp valid_part?(value) do
    case Integer.parse(value) do
      {number, ""} -> number in 1..10_000 and Integer.to_string(number) == value
      _ -> false
    end
  end

  defp validate_headers(headers) do
    metadata_bytes =
      Enum.reduce(headers, 0, fn
        {"x-amz-meta-" <> key, value}, bytes -> bytes + byte_size(key) + byte_size(value)
        _, bytes -> bytes
      end)

    cond do
      metadata_bytes > 2048 ->
        invalid()

      Enum.any?(headers, fn {key, _} ->
        key in @unsupported_headers or String.starts_with?(key, "x-amz-grant-")
      end) ->
        unsupported()

      true ->
        :ok
    end
  end

  defp copy_source(conn) do
    case Plug.Conn.get_req_header(conn, "x-amz-copy-source") do
      [] ->
        {:ok, nil}

      [source] ->
        [path | query] = String.split(source, "?", parts: 2)

        case target(path) do
          {bucket, key} when is_binary(bucket) and is_binary(key) ->
            pairs = if query == [], do: [], else: URI.query_decoder(hd(query)) |> Enum.to_list()

            with :ok <- validate_target(bucket, key),
                 true <- pairs == [] or match?([{"versionId", value}] when value != "", pairs) do
              {:ok, %{bucket: bucket, key: key, version_id: Map.new(pairs)["versionId"]}}
            else
              _ -> invalid()
            end

          _ ->
            invalid()
        end

      _ ->
        invalid()
    end
  end

  defp identify("GET", nil, nil, params, nil) when map_size(params) == 0,
    do: {:ok, :list_buckets}

  defp identify(method, bucket, nil, params, nil) when is_binary(bucket) do
    selector = Enum.find(Map.keys(params), &(&1 in @selectors))

    case {method, selector} do
      {"GET", nil} -> {:ok, :list_objects}
      {"PUT", nil} -> {:ok, :create_bucket}
      {"DELETE", nil} -> {:ok, :delete_bucket}
      {"HEAD", nil} -> {:ok, :head_bucket}
      {"POST", "delete"} -> {:ok, :delete_objects}
      {"GET", "versions"} -> {:ok, :list_object_versions}
      {"GET", "uploads"} -> {:ok, :list_multipart_uploads}
      {"GET", "location"} -> {:ok, :get_bucket_location}
      {"GET", "cors"} -> {:ok, :get_bucket_cors}
      {"PUT", "cors"} -> {:ok, :put_bucket_cors}
      {"DELETE", "cors"} -> {:ok, :delete_bucket_cors}
      {"GET", "tagging"} -> {:ok, :get_bucket_tagging}
      {"PUT", "tagging"} -> {:ok, :put_bucket_tagging}
      {"DELETE", "tagging"} -> {:ok, :delete_bucket_tagging}
      {"GET", "versioning"} -> {:ok, :get_bucket_versioning}
      {"PUT", "versioning"} -> {:ok, :put_bucket_versioning}
      {"GET", "lifecycle"} -> {:ok, :get_bucket_lifecycle}
      {"PUT", "lifecycle"} -> {:ok, :put_bucket_lifecycle}
      {"DELETE", "lifecycle"} -> {:ok, :delete_bucket_lifecycle}
      {"GET", "notification"} -> {:ok, :get_bucket_notification}
      {"PUT", "notification"} -> {:ok, :put_bucket_notification}
      {"DELETE", "notification"} -> {:ok, :delete_bucket_notification}
      {_, nil} -> method_error()
      _ -> unsupported()
    end
  end

  defp identify(method, bucket, key, params, source) when is_binary(bucket) and is_binary(key) do
    selectors = Map.keys(params) |> Enum.filter(&(&1 in @selectors)) |> Enum.sort()

    case {method, selectors, source} do
      {"GET", [], nil} ->
        {:ok, :get_object}

      {"GET", ["versionId"], nil} ->
        {:ok, :get_object_version}

      {"HEAD", selectors, nil} when selectors in [[], ["versionId"]] ->
        {:ok, :head_object}

      {"PUT", [], nil} ->
        {:ok, :put_object}

      {"PUT", [], %{}} ->
        {:ok, :copy_object}

      {"DELETE", selectors, nil} when selectors in [[], ["versionId"]] ->
        {:ok, :delete_object}

      {"GET", selectors, nil} when selectors in [["tagging"], ["tagging", "versionId"]] ->
        {:ok, :get_object_tagging}

      {"PUT", selectors, nil} when selectors in [["tagging"], ["tagging", "versionId"]] ->
        {:ok, :put_object_tagging}

      {"DELETE", selectors, nil} when selectors in [["tagging"], ["tagging", "versionId"]] ->
        {:ok, :delete_object_tagging}

      {"POST", ["uploads"], nil} ->
        {:ok, :create_multipart_upload}

      {"POST", ["uploadId"], nil} ->
        {:ok, :complete_multipart_upload}

      {"DELETE", ["uploadId"], nil} ->
        {:ok, :abort_multipart_upload}

      {"GET", ["uploadId"], nil} ->
        {:ok, :list_parts}

      {"PUT", ["partNumber", "uploadId"], nil} ->
        {:ok, :upload_part}

      {"PUT", ["partNumber", "uploadId"], %{}} ->
        {:ok, :upload_part_copy}

      {_, [], nil} ->
        method_error()

      _ ->
        unsupported()
    end
  end

  defp identify(_, _, _, _, _), do: invalid()

  defp invalid,
    do: {:error, "InvalidArgument", "The operation selectors or identifiers are invalid."}

  defp unsupported, do: {:error, "NotImplemented", "The requested operation is not implemented."}

  defp method_error,
    do: {:error, "MethodNotAllowed", "The specified method is not allowed against this resource."}
end
