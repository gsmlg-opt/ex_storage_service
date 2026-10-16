defmodule ExStorageServiceCli.CLI.NativeUtility do
  @moduledoc "Portable diagnostics, sharing and Select adapters with explicit provider boundaries."
  alias ExStorageServiceCli.S3.{Capabilities, Client, Presign}
  alias ExStorageServiceCli.CLI.{NativeShare, NativeSQL}

  def commands do
    %{
      ["doctor"] =>
        {0..1, %{"probe" => {:probe, :boolean}},
         "Explain the selected context and declared capabilities"},
      ["ping"] => {0..1, %{}, "Measure an authenticated S3 metadata request"},
      ["ready"] => {0..1, %{}, "Check S3 metadata access without ESS health paths"},
      ["admin", "info"] => {0..0, %{}, "Read genuine ESS health metadata"},
      ["sql"] =>
        {1..3,
         %{
           "query" => {:sql_query, :string},
           "recursive" => {:recursive, :boolean},
           "csv-input" => {:csv_input, :string},
           "json-input" => {:json_input, :string},
           "compression" => {:compression, :string},
           "csv-output" => {:csv_output, :string},
           "csv-output-header" => {:csv_output_header, :string},
           "json-output" => {:json_output, :string},
           "limit" => {:limit, :integer},
           "input-serialization" => {:input_serialization, :string},
           "output-serialization" => {:output_serialization, :string}
         }, "Stream S3 Select Records or atomically publish legacy outfile results"},
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
        NativeSQL.run(invocation, target, context, opts)
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

  defp provider(%{unsigned: true}), do: "anonymous"

  defp provider(%{credentials: %{provenance: %{provider: provider}}})
       when is_atom(provider) or is_binary(provider),
       do: to_string(provider)

  defp provider(_), do: "configured"
end
