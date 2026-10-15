defmodule ExStorageServiceCli.CLI.Find do
  @moduledoc "Bounded native object discovery with explicit portable predicates and version identities."
  alias ExStorageServiceCli.{Error, Target}
  alias ExStorageServiceCli.Transfer.{Filter, Inventory, Manifest}

  @doc false
  def time_cutoff(value, now), do: cutoff(value, now)

  def run(target, context, settings) do
    with {:ok, predicates} <- compile(settings.options, settings.opts),
         {:ok, inventory} <- inventory(target, context, settings) do
      try do
        summarize(inventory, settings, predicates)
      after
        Manifest.cleanup(inventory)
      end
    end
  end

  defp inventory(%Target{kind: :local}, _context, %{options: %{versions: true}}),
    do: failure(:find_versions, "Local files have no portable object-version inventory")

  defp inventory(%Target{kind: :local, path: path}, _, settings),
    do:
      Inventory.local(
        path,
        Keyword.merge(settings.opts,
          follow_symlinks: Map.get(settings.options, :follow_symlinks, true)
        )
      )

  defp inventory(%Target{kind: :remote} = target, context, settings) do
    opts =
      Keyword.merge(settings.opts,
        page_size: settings.options[:page_size] || 1000,
        input: Map.take(settings.input.input, ["RequestPayer", "ExpectedBucketOwner"])
      )

    if settings.options[:versions],
      do: Inventory.versions(context, target, opts),
      else: Inventory.remote(context, target, opts)
  end

  defp inventory(_, _, _),
    do: failure(:find_target, "Find requires a local path or remote bucket/prefix")

  defp compile(options, opts) do
    now = DateTime.to_unix(opts[:now] || DateTime.utc_now(), :nanosecond)

    with true <- is_integer(options[:maxdepth] || 0) and (options[:maxdepth] || 0) in 0..10_000,
         true <- is_integer(options[:limit] || 1000) and (options[:limit] || 1000) in 0..10_000,
         {:ok, name} <- glob(options[:name]),
         {:ok, path} <- glob(options[:path]),
         {:ok, ignore} <- glob(options[:ignore]),
         {:ok, regex} <- regex(options[:regex]),
         {:ok, larger} <- size(options[:larger]),
         {:ok, smaller} <- size(options[:smaller]),
         {:ok, older} <- cutoff(options[:older_than], now),
         {:ok, newer} <- cutoff(options[:newer_than], now) do
      {:ok,
       %{
         name: name,
         name_literal: options[:name],
         path: path,
         ignore: ignore,
         regex: regex,
         larger: larger,
         smaller: smaller,
         older: older,
         newer: newer,
         maxdepth: options[:maxdepth] || 0,
         limit: options[:limit] || 1000
       }}
    else
      false ->
        failure(
          :find_limit,
          "Find depth and result limits must be integers between zero and 10000"
        )

      error ->
        error
    end
  end

  defp summarize(inventory, settings, predicates) do
    result =
      inventory
      |> Manifest.stream()
      |> Enum.reduce_while({:ok, %{count: 0, returned: 0, entries: []}}, fn entry, {:ok, acc} ->
        original = if settings.options[:versions], do: entry["object_name"], else: entry["name"]
        name = project(original, predicates.maxdepth)

        with true <- Filter.include?(settings.filters, original),
             {:ok, true} <- matches(entry, name, predicates) do
          value = if settings.options[:versions], do: Map.put(entry, "name", name), else: name

          updated =
            if acc.returned < predicates.limit,
              do: %{
                acc
                | count: acc.count + 1,
                  returned: acc.returned + 1,
                  entries: [value | acc.entries]
              },
              else: %{acc | count: acc.count + 1}

          {:cont, {:ok, updated}}
        else
          false -> {:cont, {:ok, acc}}
          {:ok, false} -> {:cont, {:ok, acc}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    with {:ok, summary} <- result do
      {:ok,
       %{
         "Entries" => Enum.reverse(summary.entries),
         "Returned" => summary.returned,
         "MatchedObjects" => summary.count,
         "Partial" => summary.count > summary.returned
       }}
    end
  end

  defp matches(entry, name, predicates) do
    basename = name |> String.trim_trailing("/") |> String.split("/") |> List.last()

    name_match =
      glob_match?(predicates.name, basename) or predicates.name_literal in String.split(name, "/")

    if name_match and glob_match?(predicates.path, name) and
         (predicates.ignore == nil or not glob_match?(predicates.ignore, name)) and
         (predicates.larger == nil or entry["size"] > predicates.larger) and
         (predicates.smaller == nil or entry["size"] < predicates.smaller) do
      with {:ok, true} <- time_match(entry, predicates), do: regex_match(predicates.regex, name)
    else
      {:ok, false}
    end
  end

  defp project(name, 0), do: name

  defp project(name, depth) do
    name
    |> String.split("/")
    |> Enum.take(depth)
    |> Enum.join("/")
    |> then(fn prefix -> if prefix == name, do: name, else: prefix <> "/" end)
  end

  defp glob(value) when value in [nil, ""], do: {:ok, nil}
  defp glob(value), do: Filter.compile(exclude: "*", include: value)
  defp glob_match?(nil, _), do: true
  defp glob_match?(filter, value), do: Filter.include?(filter, value)

  defp time_match(_entry, %{older: nil, newer: nil}), do: {:ok, true}

  defp time_match(%{"modified" => modified}, predicates)
       when is_integer(modified) and modified > 0,
       do:
         {:ok,
          (predicates.older == nil or modified * 1000 <= predicates.older) and
            (predicates.newer == nil or modified * 1000 > predicates.newer)}

  defp time_match(_, _),
    do:
      failure(
        :find_timestamp,
        "Time predicates require an available object modification timestamp"
      )

  defp regex(value) when value in [nil, ""], do: {:ok, nil}

  defp regex(value) when is_binary(value) and byte_size(value) <= 4096 do
    groups =
      if String.valid?(value),
        do: Regex.replace(~r/\(\?(?:[imsU-]+[:)]|:)/u, value, ""),
        else: "(?invalid"

    unsupported =
      not String.valid?(value) or String.contains?(groups, ["(?", "(*", "++", "*+", "?+", "}+"]) or
        Regex.match?(~r/\\(?:[1-9]|g|k|K|C|R|X)/u, value)

    if String.valid?(value) and not unsupported do
      case :re.compile(value, [:unicode]) do
        {:ok, compiled} -> {:ok, compiled}
        _ -> failure(:find_regex, "Invalid regular expression in the bounded supported subset")
      end
    else
      failure(
        :find_regex,
        "Unsupported regex construct; backreferences, lookaround and PCRE-only control constructs are excluded"
      )
    end
  end

  defp regex(_), do: failure(:find_regex, "Regex pattern must be valid UTF-8 within 4096 bytes")

  defp regex_match(nil, _), do: {:ok, true}

  defp regex_match(regex, value) do
    case :re.run(value, regex, [
           {:capture, :none},
           {:match_limit, 100_000},
           {:match_limit_recursion, 1000},
           :report_errors
         ]) do
      :match ->
        {:ok, true}

      :nomatch ->
        {:ok, false}

      {:error, _} ->
        failure(:find_regex_budget, "Regex matching exceeded its bounded execution budget")
    end
  end

  defp size(value) when value in [nil, ""], do: {:ok, nil}

  defp size(value) when is_binary(value) and byte_size(value) <= 128 do
    case if(String.valid?(value), do: Regex.run(~r/\A([0-9]+(?:\.[0-9]+)?)\s*([a-z]*)\z/i, value)) do
      [_, number, unit] ->
        unit = String.downcase(unit)
        suffixes = ["", "k", "m", "g", "t", "p", "e"]

        factor =
          Enum.find_value(Enum.with_index(suffixes), fn {suffix, exponent} ->
            cond do
              unit in [suffix, suffix <> "b"] ->
                Integer.pow(1000, exponent)

              suffix != "" and unit in [suffix <> "i", suffix <> "ib"] ->
                Integer.pow(1024, exponent)

              true ->
                nil
            end
          end)

        bytes = if factor, do: decimal(number, factor)

        if bytes != nil and bytes <= 18_446_744_073_709_551_615,
          do: {:ok, if(bytes == 0, do: nil, else: bytes)},
          else:
            failure(
              :find_size,
              "Size filter has an unsupported unit or exceeds unsigned 64-bit bytes"
            )

      _ ->
        failure(:find_size, "Size filters require nonnegative decimal bytes or metric/IEC units")
    end
  end

  defp size(_), do: failure(:find_size, "Size filter exceeds its input bound")

  defp decimal(value, multiplier) do
    case String.split(value, ".", parts: 2) do
      [whole] ->
        String.to_integer(whole) * multiplier

      [whole, fraction] ->
        scale = Integer.pow(10, byte_size(fraction))

        div(
          (String.to_integer(if(whole == "", do: "0", else: whole)) * scale +
             String.to_integer(if(fraction == "", do: "0", else: fraction))) * multiplier,
          scale
        )
    end
  end

  defp cutoff(value, _now) when value in [nil, ""], do: {:ok, nil}

  defp cutoff(value, now) when is_binary(value) and byte_size(value) <= 256 do
    case if(String.valid?(value), do: duration(value), else: :invalid) do
      {:ok, nanos} ->
        {:ok, now - nanos}

      :error ->
        case absolute(value) do
          {:ok, date, _} ->
            {:ok, DateTime.to_unix(date, :nanosecond)}

          _ ->
            failure(
              :find_time,
              "Time filters require a bounded duration or an absolute UTC/RFC3339 timestamp"
            )
        end

      :invalid ->
        failure(:find_time, "Time filter must be valid UTF-8")
    end
  end

  defp cutoff(_, _), do: failure(:find_time, "Time filter exceeds its input bound")

  defp duration(value) do
    {sign, value} =
      case value do
        "-" <> rest -> {-1, rest}
        "+" <> rest -> {1, rest}
        other -> {1, other}
      end

    parts = Regex.scan(~r/([0-9]+(?:\.[0-9]*)?|\.[0-9]+)(ns|us|µs|μs|ms|s|m|h|d|w|y)/u, value)

    units = %{
      "ns" => 1,
      "us" => 1000,
      "µs" => 1000,
      "μs" => 1000,
      "ms" => 1_000_000,
      "s" => 1_000_000_000,
      "m" => 60_000_000_000,
      "h" => 3_600_000_000_000,
      "d" => 86_400_000_000_000,
      "w" => 604_800_000_000_000,
      "y" => 31_536_000_000_000_000
    }

    nanos =
      Enum.reduce(parts, 0, fn [_, number, unit], acc -> acc + decimal(number, units[unit]) end)

    if (value == "0" or (parts != [] and Enum.map_join(parts, &hd/1) == value)) and
         nanos <= 9_223_372_036_854_775_807,
       do: {:ok, sign * nanos},
       else: :error
  end

  defp absolute(value) do
    value = Regex.replace(~r/\A([0-9]{4})\.([0-9]{2})\.([0-9]{2})/, value, "\\1-\\2-\\3")

    value =
      case String.split(value, " ", parts: 2) do
        [date, time] -> date <> "T" <> String.replace_suffix(time, " UTC", "Z")
        _ -> value
      end

    value =
      cond do
        Regex.match?(~r/\A[0-9]{4}-[0-9]{2}-[0-9]{2}\z/, value) ->
          value <> "T00:00:00Z"

        Regex.match?(~r/\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}\z/, value) ->
          value <> ":00Z"

        Regex.match?(~r/\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\z/, value) ->
          value <> "Z"

        true ->
          value
      end

    DateTime.from_iso8601(value)
  end

  defp failure(code, message), do: {:error, Error.new(:syntax, message, code: code)}
end
