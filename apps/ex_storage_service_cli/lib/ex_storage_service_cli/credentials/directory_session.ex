defmodule ExStorageServiceCli.Credentials.DirectorySession do
  @moduledoc "Invocation-owned, bounded S3 Express session identities, separate from IAM credentials."
  use GenServer
  alias ExStorageServiceCli.Context
  alias ExStorageServiceCli.Credentials.{Owner, Record, Remote}
  alias ExStorageServiceCli.Credentials.Process, as: CredentialProcess
  alias ExStorageServiceCli.S3.Client

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, {self(), opts})

  def resolve(manager, %Context{} = iam, bucket, contract, mode \\ "ReadWrite") do
    with :ok <- validate(iam, bucket, contract, mode),
         {:ok, owner} <-
           GenServer.call(
             manager,
             {:owner, key(iam, bucket, contract, mode), iam, bucket, contract, mode}
           ),
         {:ok, %Record{} = credentials, _settings} <- Owner.resolve(owner) do
      {:ok, credentials}
    end
  catch
    :exit, _ -> Remote.failure(:directory_session_unavailable)
  end

  @impl true
  def init({caller, opts}) do
    {:ok, supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

    {:ok,
     %{
       owner: Process.monitor(opts[:invocation_owner] || caller),
       supervisor: supervisor,
       entries: %{},
       monitors: %{},
       limit: min(max(opts[:max_contexts] || 16, 1), 100),
       opts: opts
     }}
  end

  @impl true
  def handle_call({:owner, key, iam, bucket, contract, mode}, _from, state) do
    cond do
      Map.has_key?(state.entries, key) ->
        {:reply, {:ok, state.entries[key]}, state}

      map_size(state.entries) >= state.limit ->
        {:reply, Remote.failure(:directory_session_cache_full), state}

      true ->
        opts = state.opts
        loader = fn -> fetch(iam, bucket, contract, mode, opts) end

        owner_opts =
          Keyword.take(opts, [:clock, :max_waiters, :invocation_owner])
          |> Keyword.put(:expiry_skew, opts[:expiry_skew] || 30)

        spec = %{
          id: make_ref(),
          start: {Owner, :start_link, [loader, owner_opts]},
          restart: :temporary,
          shutdown: 5000
        }

        case DynamicSupervisor.start_child(state.supervisor, spec) do
          {:ok, owner} ->
            monitor = Process.monitor(owner)

            {:reply, {:ok, owner},
             %{
               state
               | entries: Map.put(state.entries, key, owner),
                 monitors: Map.put(state.monitors, monitor, key)
             }}

          _ ->
            {:reply, Remote.failure(:directory_session_unavailable), state}
        end
    end
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{owner: monitor} = state),
    do: {:stop, :normal, state}

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    {key, monitors} = Map.pop(state.monitors, monitor)
    {:noreply, %{state | entries: Map.delete(state.entries, key), monitors: monitors}}
  end

  @impl true
  def terminate(_reason, state) do
    if Process.alive?(state.supervisor), do: DynamicSupervisor.stop(state.supervisor)
  end

  @impl true
  def format_status(_status), do: %{state: "directory session cache redacted"}

  defp fetch(iam, bucket, contract, mode, opts) do
    iam = %{iam | region: contract.region}
    input = %{"Bucket" => bucket, "SessionMode" => mode}
    client_opts = [service: contract.service, max_control_bytes: 65_536, max_attempts: 1]
    client_opts = if opts[:now], do: Keyword.put(client_opts, :now, opts[:now]), else: client_opts

    with {:ok, %{"Credentials" => fields}} when is_map(fields) <-
           Client.call(iam, "CreateSession", input, client_opts),
         {:ok, record} <-
           Record.new(
             %{
               access_key_id: fields["AccessKeyId"],
               secret_access_key: fields["SecretAccessKey"],
               session_token: fields["SessionToken"],
               expires_at: fields["Expiration"]
             },
             %{provider: :s3express}
           ),
         true <- is_binary(record.session_token) and record.expires_at != nil,
         :ok <- CredentialProcess.expiry(record, opts[:now] || DateTime.utc_now()) do
      {:ok, record, %{bucket: bucket, mode: mode}}
    else
      {:error, _} = error -> error
      _ -> Remote.failure(:invalid_directory_session_response)
    end
  end

  defp validate(iam, bucket, contract, mode) do
    valid =
      not iam.unsigned and is_binary(bucket) and bucket != "" and
        mode in ["ReadOnly", "ReadWrite"] and contract[:session_auth] == :s3express and
        is_binary(contract[:endpoint]) and is_binary(contract[:region]) and
        contract[:service] == "s3express" and
        not match?(%Record{provenance: %{provider: :s3express}}, iam.credentials)

    if valid, do: :ok, else: Remote.failure(:invalid_directory_session_context)
  end

  defp key(iam, bucket, contract, mode) do
    identity =
      iam.credential_owner ||
        :crypto.hash(:sha256, [
          iam.access_key_id || "",
          <<0>>,
          iam.secret_access_key || "",
          <<0>>,
          iam.session_token || ""
        ])

    {contract.endpoint, bucket, contract.region, mode, identity}
  end
end
