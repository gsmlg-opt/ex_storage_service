defmodule ExStorageServiceCli.CLI.NativeUtility do
  @moduledoc "Portable diagnostics, sharing and Select adapters with explicit provider boundaries."
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.S3.{Capabilities, Client, Presign, Select}
  alias ExStorageServiceCli.CLI.NativeShare
  alias ExStorageServiceCli.Transfer.Sink

  def commands do
    %{
      ["doctor"] =>
        {0..1, %{"probe" => {:probe, :boolean}},
         "Explain the selected context and declared capabilities"},
      ["ping"] => {0..1, %{}, "Measure an authenticated S3 metadata request"},
      ["ready"] => {0..1, %{}, "Check S3 metadata access without ESS health paths"},
      ["admin", "info"] => {0..0, %{}, "Read genuine ESS health metadata"},
      ["sql"] =>
        {3..3,
         %{
           "input-serialization" => {:input_serialization, :string},
           "output-serialization" => {:output_serialization, :string}
         }, "Run S3 Select and atomically publish Records to a file"},
      ["transfer", "list"] =>
        {0..0, %{"limit" => {:limit, :integer}, "cursor" => {:cursor, :integer}},
         "Inspect a bounded page of local transfer journals"},
      ["transfer", "resume"] =>
        {1..1, recovery_flags(), "Resume exactly one selected multipart journal"},
      ["transfer", "abort"] =>
        {1..1, recovery_flags(), "Abort exactly one selected multipart journal"}
    }
    |> Map.merge(NativeShare.commands())
  end

  defp recovery_flags,
    do: %{
      "frontend" => {:frontend, {:enum, ~w(native aws)}},
      "source-alias" => {:source_alias, :string},
      "request-input" => {:request_input, :string}
    }

  def run(invocation, target, context, opts \\ []) do
    case invocation.command do
      ["doctor"] ->
        doctor(invocation.options, target, context, opts)

      [command] when command in ["ping", "ready"] ->
        probe(command, target, context, opts)

      ["share", _] ->
        NativeShare.run(invocation, target, context, opts)

      ["presign"] ->
        Presign.url(
          context,
          target,
          String.upcase(invocation.options[:method] || "GET"),
          Map.to_list(invocation.options) ++ opts
        )

      ["sql"] ->
        sql(invocation, target, context, opts)
    end
  end

  defp doctor(options, target, context, opts) do
    result =
      Capabilities.describe(context)
      |> Map.merge(%{
        "Region" => context.region,
        "Addressing" => Atom.to_string(context.addressing),
        "TLSVerified" => context.tls[:verify] != :verify_none,
        "Signed" => not context.unsigned,
        "CredentialProvider" => provider(context)
      })

    if options[:probe] do
      with {:ok, probe} <- probe("ready", target, context, opts),
           do: {:ok, Map.put(result, "Probe", probe)}
    else
      {:ok, result}
    end
  end

  defp probe(command, target, context, opts) do
    started = System.monotonic_time(:microsecond)
    call = opts[:call] || fn operation, input -> Client.call(context, operation, input, opts) end

    {operation, input} =
      if target && target.bucket,
        do: {"HeadBucket", %{"Bucket" => target.bucket}},
        else: {"ListBuckets", %{}}

    with {:ok, _} <- call.(operation, input),
         do:
           {:ok,
            %{
              "Ready" => true,
              "Operation" => operation,
              "Command" => command,
              "ElapsedMilliseconds" => (System.monotonic_time(:microsecond) - started) / 1000
            }}
  end

  defp sql(invocation, target, context, opts) do
    [_target, expression, filename] = invocation.args

    with {:ok, input} <- json(invocation.options[:input_serialization]),
         {:ok, output} <- json(invocation.options[:output_serialization]),
         {:ok, sink} <- Sink.open(filename) do
      try do
        reducer = fn
          %{type: "Records", payload: bytes}, result ->
            with :ok <- Sink.write(sink, bytes), do: {:ok, result}

          _event, result ->
            {:ok, result}
        end

        input = %{
          "Bucket" => target.bucket,
          "Key" => target.key,
          "ExpressionType" => "SQL",
          "Expression" => expression,
          "InputSerialization" => input,
          "OutputSerialization" => output
        }

        with :ok <- Capabilities.check(context, "SelectObjectContent", input),
             {:ok, _, summary} <- Select.call(context, input, nil, reducer, opts),
             :ok <- Sink.publish(sink),
             do: {:ok, summary}
      after
        Sink.abort(sink)
      end
    end
  end

  defp json(value) when is_binary(value) and byte_size(value) <= 1_048_576 do
    case JSON.decode(value) do
      {:ok, map} when is_map(map) -> {:ok, map}
      _ -> {:error, Error.new(:syntax, "Select serializations must be JSON objects")}
    end
  end

  defp json(_),
    do:
      {:error,
       Error.new(
         :syntax,
         "Select requires input-serialization and output-serialization JSON objects"
       )}

  defp provider(%{unsigned: true}), do: "anonymous"

  defp provider(%{credentials: %{provenance: %{provider: provider}}})
       when is_atom(provider) or is_binary(provider),
       do: to_string(provider)

  defp provider(_), do: "configured"
end
