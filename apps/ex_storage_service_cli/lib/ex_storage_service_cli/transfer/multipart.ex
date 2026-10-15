defmodule ExStorageServiceCli.Transfer.Multipart do
  @moduledoc "Owned, checkpointed multipart uploads with conservative completion recovery."
  alias ExStorageServiceCli.{Context, Error, S3.Client, S3.OperationCatalog, S3.Paginator}
  alias ExStorageServiceCli.S3.UploadChecksum

  alias ExStorageServiceCli.Transfer.{
    Engine,
    Journal,
    Planner,
    Reducer,
    RemoteSource,
    Source,
    State
  }

  def upload(
        %Context{} = context,
        bucket,
        key,
        source,
        %Planner{mode: :multipart} = plan,
        opts \\ []
      ) do
    root = root(opts)

    with :ok <- Source.validate(source),
         {:ok, contract} <- select_checksum(context, opts),
         opts = Keyword.put(opts, :checksum_contract, contract),
         {:ok, id} <- Journal.create(root, record(context, bucket, key, source, plan, opts)) do
      Journal.with_claim(root, id, fn claim ->
        with {:ok, record} <- Journal.get(root, id) do
          state = Reducer.new(plan, opts)

          case initiate(context, state, record, claim, opts) do
            {:ok, state, record} -> run(context, source, state, record, claim, opts)
            {:error, error, state, record} -> failed(context, error, state, record, claim, opts)
          end
        end
      end)
    end
  end

  def resume(%Context{} = context, id, opts \\ []) do
    root = root(opts)

    Journal.with_claim(root, id, fn claim ->
      with {:ok, record} <- Journal.get(root, id),
           {:ok, contract} <- select_checksum(context, opts),
           opts = Keyword.put(opts, :checksum_contract, contract),
           {:ok, source} <- Source.restore(context, record["source"], opts),
           plan = plan(record),
           expected =
             record(
               context,
               record["target"]["bucket"],
               record["target"]["key"],
               source,
               plan,
               opts
             ),
           :ok <- Journal.resume_matches(record, expected) do
        resume_record(context, source, record, plan, claim, opts)
      end
    end)
  end

  @doc "Aborts only the upload selected from a matching owned journal."
  def abort(%Context{} = context, id, opts \\ []) do
    Journal.with_claim(root(opts), id, fn claim ->
      with {:ok, record} <- Journal.get(root(opts), id),
           true <-
             record["endpoint"] == endpoint(context) and
               record["credential_provider"] == provider(context),
           true <- is_binary(record["upload_id"]) and record["phase"] != "completed" do
        cleanup(context, record, claim, :cancelled, opts)
        |> case do
          {:ok, record} -> {:ok, %{journal_id: id, cleanup: record["cleanup"]}}
          {:error, error, record} -> {:error, details(error, record)}
        end
      else
        {:error, _} = error -> error
        _ -> failure(:resume_conflict, "The selected upload cannot be aborted with this context")
      end
    end)
  end

  defp initiate(context, state, record, claim, opts) do
    case advance(state, :start, record, claim) do
      {:ok, state, record} -> initiate_request(context, state, record, claim, opts)
      {:error, error, state, record} -> {:error, error, state, record}
    end
  end

  defp initiate_request(context, state, record, claim, opts) do
    with {:ok, result} <-
           Client.call(
             context,
             "CreateMultipartUpload",
             input("CreateMultipartUpload", record, opts),
             opts
           ),
         id when is_binary(id) and id != "" <- result["UploadId"] do
      # Retain the owned ID even when the first durable checkpoint fails, so cleanup
      # can target our upload and never a pre-existing upload at the same key.
      {:ok, active, _effects} = Reducer.step(state, {:initiated, id})
      record = Map.put(record, "upload_id", id)

      with :ok <- UploadChecksum.acknowledge(opts[:checksum_contract], result),
           {:ok, record} <- persist(record, claim, %{"phase" => "transferring"}) do
        {:ok, active, record}
      else
        {:error, error} -> {:error, error, active, record}
      end
    else
      {:error, error, state, record} ->
        {:error, error, state, record}

      {:error, error} ->
        {:error, error, state, record}

      _ ->
        {:error,
         Error.new(:protocol, "Multipart initiation lacks an upload ID", code: :invalid_upload_id),
         state, record}
    end
  end

  defp run(context, source, state, record, claim, opts) do
    case parts(context, source, state, record, claim, opts) do
      {:ok, state, record} -> complete(context, source, state, record, claim, opts)
      {:error, error, state, record} -> failed(context, error, state, record, claim, opts)
    end
  end

  defp parts(context, source, state, record, claim, opts) do
    Enum.reduce_while(record["parts"], {:ok, state, record}, fn part, {:ok, state, record} ->
      if part["status"] == "completed" do
        {:cont, {:ok, state, record}}
      else
        result =
          with {:ok, result, local_checksums} <- upload_part(context, source, part, record, opts),
               tag when is_binary(tag) and tag != "" <- result["ETag"] do
            updated =
              Enum.map(record["parts"], fn current ->
                if current["number"] == part["number"],
                  do:
                    current
                    |> Map.put("status", "completed")
                    |> Map.put("etag", tag)
                    |> Map.put("checksums", local_checksums),
                  else: current
              end)

            advance(
              state,
              {:part_completed, part["number"], tag},
              Map.put(record, "parts", updated),
              claim
            )
          else
            {:error, _} = error -> error
            _ -> failure(:invalid_part_result, "Multipart part response lacks an ETag")
          end

        case result do
          {:ok, state, record} -> {:cont, {:ok, state, record}}
          {:error, error, state, record} -> {:halt, {:error, error, state, record}}
          {:error, error} -> {:halt, {:error, error, state, record}}
        end
      end
    end)
  end

  defp upload_part(context, source, part, record, opts) do
    if opts[:copy] == true and is_struct(source, RemoteSource) do
      request =
        input("UploadPartCopy", record, opts)
        |> Map.merge(RemoteSource.copy_input(source))
        |> Map.put("PartNumber", part["number"])
        |> Map.put(
          "CopySourceRange",
          "bytes=#{part["offset"]}-#{part["offset"] + part["length"] - 1}"
        )

      with :ok <- Source.validate(source),
           {:ok, %{"CopyPartResult" => result}} <-
             Client.call(context, "UploadPartCopy", request, opts),
           {:ok, checksums} <- UploadChecksum.copied_part(opts[:checksum_contract], result) do
        {:ok, result, checksums}
      end
    else
      with {:ok, range} <- Source.range(source, part["offset"], part["length"], opts) do
        try do
          with {:ok, result} <-
                 Client.call(
                   context,
                   "UploadPart",
                   Map.put(input("UploadPart", record, opts), "PartNumber", part["number"]),
                   Keyword.merge(opts,
                     body_source: Engine.upload_chunks(range, opts, part["offset"]),
                     checksum_source: range,
                     payload_hash: range.payload_hash,
                     content_length: range.length
                   )
                 ),
               :ok <- Source.validate(range),
               {:ok, checksums} <- UploadChecksum.part(opts[:checksum_contract], range, result),
               do: {:ok, result, checksums}
        after
          Source.cleanup(range)
        end
      end
    end
  end

  defp select_checksum(context, opts),
    do:
      UploadChecksum.select(
        context,
        opts[:input] || %{},
        if(opts[:copy] == true, do: :copy, else: :multipart)
      )

  defp complete(context, source, state, record, claim, opts) do
    with :ok <- Source.validate(source),
         {:ok, fields, expected_checksums} <-
           UploadChecksum.completion(opts[:checksum_contract], record["parts"]),
         {:ok, record} <- persist(record, claim, %{"phase" => "completing"}) do
      request =
        Map.merge(input("CompleteMultipartUpload", record, opts), fields)

      case Client.call(context, "CompleteMultipartUpload", request, opts) do
        {:ok, result} ->
          completion = %{"etag" => result["ETag"]}

          completion =
            if result["VersionId"],
              do: Map.put(completion, "version_id", result["VersionId"]),
              else: completion

          case advance(
                 state,
                 :completion_confirmed,
                 Map.put(record, "completion", completion),
                 claim
               ) do
            {:ok, state, record} ->
              case UploadChecksum.verify(expected_checksums, result) do
                {:ok, _} -> verify(context, state, record, claim, opts)
                {:error, error} -> failed(context, error, state, record, claim, opts)
              end

            {:error, error, _state, record} ->
              {:error, details(error, record)}
          end

        {:error, %Error{category: :service} = error} ->
          # A parsed service rejection is definitive. A lost/invalid reply is not.
          {:ok, uncertain, _} = Reducer.step(state, :completion_unknown)
          {:ok, failed, _} = Reducer.step(uncertain, :completion_rejected)
          failed(context, error, failed, record, claim, opts)

        {:error, _} ->
          indeterminate(state, record, claim)
      end
    else
      {:error, error} ->
        failed(context, error, %{state | phase: :transferring}, record, claim, opts)
    end
  end

  defp verify(context, state, record, claim, opts) do
    head = input("HeadObject", record, opts)

    head =
      if record["completion"]["version_id"],
        do: Map.put(head, "VersionId", record["completion"]["version_id"]),
        else: head

    with {:ok, _fields, expected_checksums} <-
           UploadChecksum.completion(opts[:checksum_contract], record["parts"]),
         {:ok, result} <- Client.call(context, "HeadObject", head, opts),
         true <-
           result["ContentLength"] == record["source"]["length"] and
             tag(result["ETag"]) == tag(record["completion"]["etag"]),
         {:ok, _} <- UploadChecksum.verify(expected_checksums, result),
         {:ok, _state, record} <- advance(state, :verified, record, claim) do
      success(record, result)
    else
      {:error, error, _state, record} ->
        {:error, details(error, record)}

      {:error, error} ->
        failed(context, error, state, record, claim, opts)

      false ->
        failed(
          context,
          Error.new(:protocol, "Completed object identity does not match the upload",
            code: :completion_verification_failed
          ),
          state,
          record,
          claim,
          opts
        )
    end
  end

  defp resume_record(context, _source, %{"completion" => completion} = record, plan, claim, opts)
       when is_map(completion) do
    state = %{
      Reducer.new(plan, opts)
      | phase: :verifying,
        upload_id: record["upload_id"],
        upload_active: false
    }

    verify(context, state, record, claim, opts)
  end

  defp resume_record(_context, _source, %{"phase" => phase} = record, _plan, _claim, _opts)
       when phase in ["completing", "indeterminate"] do
    {:error,
     details(
       Error.new(
         :protocol,
         "Multipart completion has an unknown outcome; inspect the destination before further action",
         code: :completion_indeterminate
       ),
       record
     )}
  end

  defp resume_record(context, source, record, plan, claim, opts) do
    cond do
      record["cleanup"] == "aborted" ->
        {:error,
         details(
           Error.new(:client, "The recorded multipart upload was aborted", code: :upload_aborted),
           record
         )}

      not is_binary(record["upload_id"]) ->
        {:error,
         details(
           Error.new(:client, "The journal has no acknowledged upload ID",
             code: :upload_id_unknown
           ),
           record
         )}

      true ->
        with {:ok, remote} <-
               Paginator.collect("ListParts", input("ListParts", record, opts), fn operation,
                                                                                   input ->
                 Client.call(context, operation, input, opts)
               end),
             remote =
               Enum.map(remote["Parts"] || [], fn part ->
                 %{
                   "number" => part["PartNumber"],
                   "size" => part["Size"],
                   "etag" => part["ETag"],
                   "checksums" => checksums(part)
                 }
               end),
             {:ok, record} <- Journal.reconcile(record, remote, complete: true),
             {:ok, record} <-
               persist(record, claim, %{"phase" => "transferring", "cleanup" => "none"}) do
          completed =
            for part <- record["parts"],
                part["status"] == "completed",
                into: %{},
                do: {part["number"], part["etag"]}

          phase = if map_size(completed) == plan.part_count, do: :completing, else: :transferring

          state = %{
            Reducer.new(plan, opts)
            | phase: phase,
              upload_active: true,
              upload_id: record["upload_id"],
              completed_parts: completed
          }

          run(context, source, state, record, claim, opts)
        end
    end
  end

  defp failed(context, error, state, record, claim, opts) do
    cond do
      not state.upload_active ->
        retain_error(error, record, claim, %{"phase" => "failed"})

      opts[:keep_incomplete] == true ->
        retain_error(error, record, claim, %{"phase" => "failed", "cleanup" => "retained"})

      true ->
        case cleanup(context, record, claim, :failed, opts) do
          {:ok, record} ->
            {:error, details(error, record)}

          {:error, cleanup_error, record} ->
            {:error, details(error, record, %{cleanup_error: cleanup_error.code})}
        end
    end
  end

  defp cleanup(context, record, claim, phase, opts) do
    # A failed checkpoint must never prevent aborting the upload ID we created.
    # Reload the revision while retaining that owned ID for a final checkpoint.
    record = latest(record, claim)
    checkpoint = persist(record, claim, %{"phase" => "aborting", "cleanup" => "abort_requested"})

    record =
      case checkpoint do
        {:ok, record} ->
          record

        {:error, _error} ->
          record
      end

    case Client.call(
           context,
           "AbortMultipartUpload",
           input("AbortMultipartUpload", record, opts),
           opts
         ) do
      {:ok, _} ->
        case persist(record, claim, %{
               "phase" => Atom.to_string(phase),
               "cleanup" => "aborted"
             }) do
          {:ok, record} -> {:ok, record}
          {:error, error} -> {:error, error, record}
        end

      {:error, error} ->
        case persist(record, claim, %{"phase" => "abort_failed", "cleanup" => "abort_failed"}) do
          {:ok, record} -> {:error, error, record}
          {:error, checkpoint_error} -> {:error, checkpoint_error, record}
        end
    end
  end

  defp latest(record, claim) do
    case Journal.get(claim.root, record["id"]) do
      {:ok, current} -> Map.put(current, "upload_id", record["upload_id"])
      {:error, _} -> record
    end
  end

  defp indeterminate(state, record, claim) do
    {:ok, _state, _effects} = Reducer.step(state, :completion_unknown)

    retain_error(
      Error.new(:protocol, "Multipart completion has an unknown outcome",
        code: :completion_indeterminate
      ),
      record,
      claim,
      %{"phase" => "indeterminate"}
    )
  end

  defp retain_error(error, record, claim, changes) do
    case persist(record, claim, changes) do
      {:ok, record} -> {:error, details(error, record)}
      {:error, checkpoint_error} -> {:error, details(checkpoint_error, record)}
    end
  end

  defp advance(state, event, record, claim) do
    with {:ok, state, _effects} <- Reducer.step(state, event) do
      case persist(record, claim, %{"phase" => Atom.to_string(state.phase)}) do
        {:ok, record} -> {:ok, state, record}
        {:error, error} -> {:error, error, state, record}
      end
    end
  end

  defp persist(record, claim, changes) do
    updated = Map.merge(record, changes)

    with :ok <- Journal.checkpoint(claim, updated),
         do: {:ok, Map.update!(updated, "revision", &(&1 + 1))}
  end

  defp record(context, bucket, key, source, plan, opts) do
    options =
      %{
        "part_size" => plan.part_size,
        "keep_incomplete" => opts[:keep_incomplete] == true,
        "copy" => opts[:copy] == true,
        "checksum_algorithm" => get_in(opts, [:checksum_contract, "algorithm"]),
        "checksum_type" => get_in(opts, [:checksum_contract, "type"]),
        "input_digest" =>
          :crypto.hash(:sha256, :erlang.term_to_binary(opts[:input] || %{}, [:deterministic]))
          |> Base.encode16(case: :lower)
      }
      |> Map.reject(fn {_key, value} -> is_nil(value) end)

    %{
      "version" => 1,
      "endpoint" => endpoint(context),
      "credential_provider" => provider(context),
      "target" => %{"bucket" => bucket, "key" => key},
      "source" => Journal.source_record(source),
      "options" => options,
      "phase" => "planned",
      "cleanup" => "none",
      "parts" =>
        Enum.map(plan.parts, fn part ->
          %{
            "number" => part.number,
            "offset" => source.offset + part.offset,
            "length" => part.length,
            "status" => "pending",
            "etag" => nil,
            "checksums" => %{}
          }
        end)
    }
  end

  defp endpoint(context),
    do: %{
      "url" => context.endpoint,
      "region" => context.region,
      "addressing" => Atom.to_string(context.addressing),
      "service" => "s3"
    }

  defp provider(%{unsigned: true}), do: %{"kind" => "anonymous", "name" => "anonymous"}

  defp provider(%{credentials: %{provenance: %{provider: :explicit}}, access_key_id: key}),
    do: %{
      "kind" => "explicit",
      "name" => :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
    }

  defp provider(context), do: %{"kind" => "profile", "name" => context.profile || "default"}

  defp plan(record) do
    parts =
      Enum.map(record["parts"], fn part ->
        %{
          number: part["number"],
          offset: part["offset"] - record["source"]["offset"],
          length: part["length"]
        }
      end)

    %Planner{
      mode: :multipart,
      size: record["source"]["length"],
      part_size: record["options"]["part_size"],
      part_count: length(parts),
      parts: parts
    }
  end

  defp input(operation, record, opts) do
    {:ok, model} = OperationCatalog.operation(operation)
    members = OperationCatalog.shape(model["input"]["shape"])["members"] |> Map.keys()

    configured =
      if operation == "CreateMultipartUpload",
        do: UploadChecksum.initiation(opts[:checksum_contract], opts[:input] || %{}),
        else: opts[:input] || %{}

    configured =
      if operation == "UploadPart" and opts[:checksum_contract],
        do: Map.put(configured, "ChecksumAlgorithm", opts[:checksum_contract]["algorithm"]),
        else: configured

    configured
    |> Map.merge(%{
      "Bucket" => record["target"]["bucket"],
      "Key" => record["target"]["key"],
      "UploadId" => record["upload_id"]
    })
    |> Map.take(members)
    |> Map.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp checksums(result) do
    for algorithm <- ~w(SHA256 SHA1 CRC32 CRC32C CRC64NVME),
        value = result["Checksum" <> algorithm],
        is_binary(value),
        into: %{},
        do: {algorithm, value}
  end

  defp tag(value) when is_binary(value), do: String.trim(value, "\"")
  defp tag(_value), do: nil
  defp root(opts), do: opts[:state_root] || State.transfers()

  defp success(record, result),
    do:
      {:ok,
       %{
         bytes: record["source"]["length"],
         etag: tag(record["completion"]["etag"]),
         journal_id: record["id"],
         result: result
       }}

  defp details(error, record, extra \\ %{}),
    do: %{
      error
      | details:
          Map.merge(
            error.details,
            Map.merge(%{journal_id: record["id"], cleanup: record["cleanup"]}, extra)
          )
    }

  defp failure(code, message), do: {:error, Error.new(:protocol, message, code: code)}
end
