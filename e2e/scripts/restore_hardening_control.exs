# Test-only local harness. No credentials are emitted to stdout or evidence.
alias ExStorageService.{Context, InstanceConfig}
alias ExStorageService.Operations.Backup

[action | arguments] = System.argv()

result =
  case {action, arguments} do
    {"boot", []} ->
      configured = Application.fetch_env!(:ex_storage_service, :instance_config)

      workers =
        [
          :multipart_gc,
          :content_gc,
          :cas_gc,
          :packer,
          :lifecycle,
          :cross_cluster_replication,
          :repair,
          :scrub
        ]
        |> Map.new(&{&1, false})

      configured = if is_map(configured), do: Map.to_list(configured), else: configured

      Application.put_env(
        :ex_storage_service,
        :instance_config,
        Keyword.put(configured, :workers, workers)
      )

      {:ok, _} = Application.ensure_all_started(:ex_storage_service)
      {:ok, context} = Context.default()

      unless System.get_env("ESS_RESTORE_BOOT") == "quarantine" do
        {:ok, _} = Application.ensure_all_started(:ex_storage_service_cluster)
        {:ok, _} = Application.ensure_all_started(:ex_storage_service_s3)
      end

      path = System.fetch_env!("ESS_RESTORE_READY_FILE")

      File.write!(
        path,
        JSON.encode!(%{node: to_string(node()), generation: context.config.node_generation}),
        [:sync]
      )

      File.chmod!(path, 0o600)
      :boot_ready

    {"prepare", [bundle]} ->
      {:ok, config} = InstanceConfig.from_application_env()
      {:ok, value} = Backup.prepare(Context.new(config), bundle)
      value

    {"verify-bundle", [bundle, expected]} ->
      outcome = Backup.verify(bundle)

      case {expected, outcome} do
        {"valid", {:ok, report}} -> report
        {"invalid", {:error, reason}} -> %{rejected: inspect(reason)}
        _ -> raise "backup verification did not produce the required result"
      end

    {"rpc", [operation, target_string, input_path, output_path]} ->
      true = Regex.match?(~r/^ess_restore_[a-z0-9_]+@127\.0\.0\.1$/, target_string)
      target = String.to_atom(target_string)
      true = Node.connect(target)
      input = JSON.decode!(File.read!(input_path))
      {:ok, context} = :rpc.call(target, Context, :default, [], 30_000)
      root = context.data_root

      true =
        String.starts_with?(root, "/tmp/ess-hardening-restore-") or
          String.starts_with?(root, "/tmp/recovery/targets/")

      call = fn module, function, args ->
        case :rpc.call(target, module, function, args, 120_000) do
          {:badrpc, {:EXIT, {reason, stack}}} when is_list(stack) ->
            kind = if is_exception(reason), do: reason.__struct__, else: :non_exception_exit

            frames =
              stack
              |> Enum.take(8)
              |> Enum.flat_map(fn
                {mod, fun, arity, _location} when is_integer(arity) ->
                  [{mod, fun, arity}]

                {mod, fun, arguments, _location} when is_list(arguments) ->
                  [{mod, fun, length(arguments)}]

                _ ->
                  []
              end)

            # Exception values and frame arguments can contain backup records or
            # credentials. Retain only the exception module and bounded MFAs.
            raise "disposable restore RPC failed: #{inspect(kind)} #{inspect(frames)}"

          {:badrpc, _} ->
            raise "disposable restore RPC failed"

          value ->
            value
        end
      end

      value =
        case operation do
          "seed" ->
            source = """
            alias ExStorageService.IAM.{User, AccessKey, Policy}
            {:ok, full_user} = User.create_user("restore-admin")
            deny = %{effect: :deny, actions: ["s3:PutObject"],
              resources: ["arn:aws:s3:::#{input["bucket"]}/denied"]}
            {:ok, full_policy} = Policy.create_policy("restore-admin-policy", Policy.full_access_statements() ++ [deny])
            :ok = Policy.attach_policy(full_user.id, full_policy.id)
            {:ok, full_key} = AccessKey.create_access_key(full_user.id)
            {:ok, reader_user} = User.create_user("restore-reader")
            {:ok, reader_policy} = Policy.create_policy("restore-reader-policy", [
              %{effect: :allow, actions: ["s3:GetObject", "s3:GetObjectVersion", "s3:GetObjectTagging", "s3:GetObjectVersionTagging"],
                resources: ["arn:aws:s3:::#{input["bucket"]}/*"]}
            ])
            :ok = Policy.attach_policy(reader_user.id, reader_policy.id)
            {:ok, reader_key} = AccessKey.create_access_key(reader_user.id)
            %{admin: %{access_key: full_key.access_key_id, secret_key: full_key.secret_access_key},
              reader: %{access_key: reader_key.access_key_id, secret_key: reader_key.secret_access_key}}
            """

            {credentials, _} = call.(Code, :eval_string, [source])
            File.write!(output_path, JSON.encode!(credentials), [:sync])
            File.chmod!(output_path, 0o600)
            :credentials_written_privately

          "pack" ->
            true = context.config.mode == :standalone
            hash = input["hash"]
            true = Regex.match?(~r/^[a-f0-9]{64}$/, hash)

            {:ok, %{packed: 1, pack_hash: pack_hash}} =
              call.(ExStorageService.Storage.Pack, :pack_blobs, [[hash]])

            :ok =
              call.(ExStorageService.BlobStore.LocalCAS, :delete, [
                hash,
                Context.direct_blob_store_options(context)
              ])

            %{packed: true, loose_fallback_removed: true, pack_hash: pack_hash}

          "quiesce" ->
            {:ok, report} = call.(Backup, :quiesce, [context])
            report

          "boundary" ->
            source = """
            alias ExStorageService.Metadata.{Keys, JobStore, OperationIntents}
            alias ExStorageService.Metadata.Models.Job
            alias ExStorageService.Context
            {:ok, context} = Context.default()
            input = #{inspect(input)}
            now = System.system_time(:millisecond)
            replica = %{endpoint: input["canary"], bucket: input["bucket"], access_key: nil, secret_key_enc: nil}
            for name <- ["pending", "running", "completed", "failed"] do
              event = %{id: "restore-" <> name, kind: :cross_cluster_delete,
                payload: %{bucket: input["bucket"], key: "absent-canary", replica: replica}}
              {:ok, job} = Job.new("restore-boundary", event, now)
              :ok = Concord.put(Keys.job(job.job_id), Job.to_map(job))
              if name != "pending" do
                {:ok, claimed} = JobStore.claim(job.job_id, context.config.node_id, now, 3_600_000,
                  owner_generation: context.config.node_generation)
                case name do
                  "completed" -> {:ok, _} = JobStore.complete(job.job_id, context.config.node_id,
                    claimed.fencing_token, now + 1, owner_generation: context.config.node_generation)
                  "failed" ->
                    {:ok, value} = JobStore.get(job.job_id)
                    :ok = Concord.put(Keys.job(job.job_id), Map.merge(Job.to_map(value),
                      %{state: :failed, lease_until_ms: 0}))
                  _ -> :ok
                end
              end
            end
            {:ok, _} = OperationIntents.open("restore-unknown", input["hash"], input["size"],
              context.config.node_id, context.config.node_generation, protection_ms: 3_600_000)
            {:ok, _} = OperationIntents.transition("restore-unknown", :unknown)
            %{jobs: 4, unknown_intent: true, old_owner: context.config.node_id, old_generation: context.config.node_generation}
            """

            {report, _} = call.(Code, :eval_string, [source])
            report

          "backup" ->
            contexts =
              Enum.map(input["sources"], fn name ->
                {:ok, source_context} =
                  :rpc.call(String.to_atom(name), Context, :default, [], 30_000)

                source_context
              end)

            {:ok, report} =
              call.(Backup, :create, [context, input["bundle"], [sources: contexts]])

            report

          "restore" ->
            {:ok, report} = call.(Backup, :restore, [context, input["bundle"]])
            report

          "install" ->
            {:ok, report} = call.(Backup, :install, [context, input["bundle"]])
            report

          "verify" ->
            {:ok, report} = call.(Backup, :verify_restore, [context, input["bundle"]])
            report

          "fences" ->
            source = """
            alias ExStorageService.Metadata.{JobStore, BlobLocations, Keys}
            now = System.system_time(:millisecond)
            {:ok, pending} = JobStore.get("restore-pending")
            {:ok, running} = JobStore.get("restore-running")
            {:ok, completed} = JobStore.get("restore-completed")
            {:ok, failed} = JobStore.get("restore-failed")
            true = pending.state == :pending and pending.fencing_token == 0
            true = running.state == :pending and running.fencing_token == 2
            true = is_nil(running.owner_node) and is_nil(running.owner_generation) and running.lease_until_ms == 0
            true = completed.state == :completed and failed.state == :failed
            for function <- [:renew, :complete, :fail] do
              args = case function do
                :renew -> ["restore-running", #{inspect(input["old_owner"])}, 1, now, 30_000, [owner_generation: 1]]
                :complete -> ["restore-running", #{inspect(input["old_owner"])}, 1, now, [owner_generation: 1]]
                :fail -> ["restore-running", #{inspect(input["old_owner"])}, 1, :old_worker, now, [owner_generation: 1]]
              end
              {:error, error} = apply(JobStore, function, args)
              true = error == :stale_lease or match?({:stale_lease, _}, error)
            end
            {:error, :stale_cleanup_fence} = BlobLocations.authorize_cleanup(#{inspect(input["hash"])},
              #{inspect(input["old_owner"])}, 1, "restore-running", #{inspect(input["old_owner"])}, 1, 1)
            {:ok, intent} = Concord.get(Keys.operation_intent("restore-unknown"))
            true = intent.state == :unknown and intent.node_generation == 1
            %{pending_preserved: true, running_fenced: true, terminal_preserved: true,
              old_renew_complete_fail_rejected: true, old_cleanup_rejected: true, unknown_intent_preserved: true}
            """

            {report, _} = call.(Code, :eval_string, [source])
            report

          "reject-workers" ->
            workers = Map.new(context.config.workers, fn {name, _} -> {name, name == :cas_gc} end)

            {:error, :unresolved_recovery_work} =
              call.(Backup, :activate, [context, input["bundle"], [workers: workers]])

            %{destructive_activation_rejected: true}

          "activate" ->
            workers = Map.new(context.config.workers, fn {name, _} -> {name, false} end)

            {:ok, report} =
              call.(Backup, :activate, [context, input["bundle"], [workers: workers]])

            report
        end

      unless operation == "seed" do
        File.write!(output_path, JSON.encode!(value), [:sync])
        File.chmod!(output_path, 0o600)
      end

      %{operation: operation, completed: true}
  end

IO.puts(JSON.encode!(%{restore_control: result}))
