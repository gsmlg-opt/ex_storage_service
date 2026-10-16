defmodule ExStorageServiceCli.S3.RequestChecksum do
  @moduledoc "Pinned AWS CLI CRC64NVME defaults and modeled request checksum effects."
  alias ExStorageServiceCli.{Error, Transfer.Source}
  alias ExStorageServiceCli.S3.{Checksum, OperationCatalog}

  def input(context, operation, input) do
    with {:ok, model} <- OperationCatalog.operation(operation) do
      validation = get_in(model, ["httpChecksum", "requestValidationModeMember"])

      if validation && context.options[:response_checksum_validation] != "when_required",
        do: {:ok, Map.put_new(input, validation, "ENABLED")},
        else: {:ok, input}
    end
  end

  def prepare(request, input, context, opts) do
    with {:ok, model} <- OperationCatalog.operation(request.operation) do
      traits = model["httpChecksum"] || %{}
      member = traits["requestAlgorithmMember"]
      required = traits["requestChecksumRequired"] == true
      selected = if member, do: input[member]

      enabled =
        member &&
          (selected || required ||
             context.options[:request_checksum_calculation] != "when_required")

      if enabled && not supplied?(request.headers) && opts[:streaming_spec] == nil do
        algorithm = selected || "CRC64NVME"

        with {:ok, digest} <-
               digest(
                 algorithm,
                 request.body,
                 Keyword.put(opts, :coordinator, context.coordinator)
               ) do
          headers =
            Enum.reject(request.headers, fn {name, _} ->
              String.downcase(name) == "x-amz-sdk-checksum-algorithm"
            end)

          {:ok,
           %{
             request
             | headers:
                 headers ++
                   [
                     {Checksum.header(algorithm), digest},
                     {"x-amz-sdk-checksum-algorithm", algorithm}
                   ]
           }}
        end
      else
        {:ok, request}
      end
    end
  end

  defp supplied?(headers) do
    names = ["content-md5"] ++ Enum.map(Checksum.algorithms(), &Checksum.header/1)
    Enum.any?(headers, fn {name, _value} -> String.downcase(name) in names end)
  end

  defp digest(algorithm, body, opts) do
    with {:ok, state} <- Checksum.new(algorithm) do
      cond do
        is_struct(opts[:checksum_source], Source) ->
          with {:ok, state} <-
                 Source.reduce(
                   %{opts[:checksum_source] | coordinator: opts[:coordinator]},
                   state,
                   &Checksum.update(&2, &1)
                 ),
               do: {:ok, Checksum.encoded(state)}

        is_list(opts[:body_source]) ->
          state =
            Enum.reduce(opts[:body_source], state, fn bytes, state ->
              Checksum.update(state, IO.iodata_to_binary(bytes))
            end)

          {:ok, Checksum.encoded(state)}

        Keyword.has_key?(opts, :body_source) ->
          {:error,
           Error.new(:client, "A streaming checksum requires a captured replayable source",
             code: :checksum_source_required
           )}

        is_binary(body) ->
          {:ok, state |> Checksum.update(body) |> Checksum.encoded()}

        is_nil(body) ->
          {:ok, Checksum.encoded(state)}

        true ->
          {:error,
           Error.new(:client, "Request checksum body is not replayable",
             code: :checksum_source_required
           )}
      end
    end
  end
end
