defmodule ExStorageServiceCli.CLI.NativeShare do
  @moduledoc "Portable signed shares with private local history and bounded recursive downloads."
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.CLI.NativeShare.History
  alias ExStorageServiceCli.S3.{PostPolicy, Presign}
  alias ExStorageServiceCli.Transfer.{Inventory, Manifest}

  def commands do
    common = %{
      "expires" => {:expires, :integer},
      "expire" => {:expire, :repeated_string},
      "recursive" => {:recursive, :boolean}
    }

    listing = %{
      "reveal" => {:reveal, :boolean},
      "limit" => {:limit, :integer},
      "cursor" => {:cursor, :integer}
    }

    %{
      ["share", "download"] =>
        {1..1,
         Map.merge(common, %{
           "version-id" => {:version_id, :string},
           "vid" => {:version_id, :string},
           "limit" => {:limit, :integer}
         }), "Create signed GET shares; recursive batches are bounded"},
      ["share", "upload"] =>
        {1..1,
         Map.merge(common, %{
           "post" => {:post, :boolean},
           "min-bytes" => {:min_bytes, :integer},
           "max-bytes" => {:max_bytes, :integer},
           "content-type" => {:content_type, :string}
         }), "Create PUT or POST shares; recursive uploads use a POST prefix policy"},
      ["share", "list", "download"] =>
        {0..0, listing, "List unexpired local download shares; --reveal exposes signed URLs"},
      ["share", "list", "upload"] =>
        {0..0, listing, "List unexpired local upload shares; --reveal exposes signed forms"}
    }
  end

  def run(invocation, target, context, opts \\ []) do
    ["share", action] = invocation.command
    options = invocation.options
    opts = Keyword.merge(opts, Map.to_list(options)) |> Keyword.put_new(:now, DateTime.utc_now())

    with {:ok, expires} <- expiry(options),
         :ok <- validate(action, options),
         {:ok, shares} <- generate(action, target, context, Keyword.put(opts, :expires, expires)),
         :ok <- History.record(action, shares, opts) do
      if options[:recursive] == true and action == "download",
        do: {:ok, %{"Shares" => Enum.map(shares, &elem(&1, 0)), "Count" => length(shares)}},
        else: {:ok, elem(hd(shares), 0)}
    end
  end

  def list(kind, options), do: History.list(kind, Map.to_list(options))

  defp generate("download", target, context, opts) do
    if opts[:recursive] do
      recursive_download(target, context, opts)
    else
      with {:ok, share} <- Presign.url(context, target, "GET", opts),
           do: {:ok, [{share, target, opts[:version_id]}]}
    end
  end

  defp generate("upload", target, context, opts) do
    result =
      if opts[:post] || opts[:recursive],
        do:
          PostPolicy.generate(
            context,
            target,
            Keyword.put(opts, :key_prefix, opts[:recursive] == true)
          ),
        else: Presign.url(context, target, "PUT", opts)

    with {:ok, share} <- result, do: {:ok, [{share, target, nil}]}
  end

  defp recursive_download(target, context, opts) do
    limit = opts[:limit] || 1000

    if is_integer(limit) and limit in 1..1000 do
      with {:ok, inventory} <- Inventory.remote(context, target, opts) do
        try do
          entries =
            Manifest.stream(inventory)
            |> Stream.reject(&String.ends_with?(&1["key"], "/"))
            |> Enum.take(limit + 1)

          if length(entries) > limit do
            invalid(
              "Recursive sharing exceeds its complete-batch limit (maximum 1000)",
              :share_limit
            )
          else
            Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, shares} ->
              object = %{target | key: entry["key"], trailing_slash: false}

              case Presign.url(context, object, "GET", opts) do
                {:ok, share} -> {:cont, {:ok, [{share, object, nil} | shares]}}
                error -> {:halt, error}
              end
            end)
            |> case do
              {:ok, shares} -> {:ok, Enum.reverse(shares)}
              error -> error
            end
          end
        after
          Manifest.cleanup(inventory)
        end
      end
    else
      invalid("Recursive share limit must be between 1 and 1000", :share_limit)
    end
  end

  defp validate(action, options) do
    cond do
      Map.has_key?(options, :limit) and options[:recursive] != true ->
        invalid("--limit requires recursive download sharing")

      options[:recursive] == true and options[:version_id] ->
        invalid("Selected versions cannot be shared recursively")

      options[:version_id] != nil and
          (action != "download" or options[:version_id] == "" or
             byte_size(options[:version_id]) > 8192) ->
        invalid("Invalid selected download version")

      action == "upload" and options[:post] != true and options[:recursive] != true and
          Enum.any?([:min_bytes, :max_bytes, :content_type], &Map.has_key?(options, &1)) ->
        invalid("Upload form fields require --post or --recursive")

      true ->
        :ok
    end
  end

  defp expiry(options) do
    duration = options[:expire]

    cond do
      duration != nil and options[:expires] != nil ->
        invalid("Use either --expire duration or --expires seconds")

      duration != nil ->
        duration(duration)

      is_integer(options[:expires]) and options[:expires] in 1..604_800 ->
        {:ok, options[:expires]}

      options[:expires] == nil ->
        {:ok, 3600}

      true ->
        invalid("Sharing expiry must be between 1 and 604800 seconds")
    end
  end

  defp duration(value) when is_binary(value) and byte_size(value) <= 128 do
    parts = Regex.scan(~r/(\d+(?:\.\d+)?)(h|m|s)/, value)

    if parts != [] and Enum.map_join(parts, &hd/1) == value do
      seconds =
        Enum.reduce(parts, 0.0, fn [_, number, unit], sum ->
          {number, ""} = Float.parse(number)
          sum + number * %{"h" => 3600, "m" => 60, "s" => 1}[unit]
        end)

      if seconds >= 1 and seconds <= 604_800,
        do: {:ok, trunc(seconds)},
        else: invalid("Sharing duration must be between 1s and 168h")
    else
      invalid("Sharing duration must use h, m or s (for example 1h30m)")
    end
  end

  defp duration(_), do: invalid("Invalid sharing duration")

  defp invalid(message, code \\ :invalid_share),
    do: {:error, Error.new(:syntax, message, code: code)}
end
