defmodule ExStorageService.InstanceConfig do
  @moduledoc """
  Validated configuration for one storage instance.

  Standalone remains the default. Cluster metadata and private transport values
  are typed and validated while the public cluster writer stays disabled until
  replica quorum semantics are complete.
  """

  @enforce_keys [
    :instance,
    :auto_start,
    :data_root,
    :blob_root,
    :tmp_root,
    :ra_root,
    :metadata_root,
    :web_enabled,
    :workers,
    :mode,
    :node_role,
    :node_id,
    :cluster_name,
    :cluster_topology,
    :cluster_members,
    :cluster_seeds,
    :cluster_bootstrap,
    :erlang_node,
    :erlang_cookie,
    :internal_transport_enabled,
    :internal_bind,
    :internal_port,
    :internal_advertised_url,
    :internal_secret,
    :internal_tls_certfile,
    :internal_tls_keyfile,
    :internal_auth_skew_seconds,
    :replication_factor,
    :write_quorum,
    :allow_degraded_writes,
    :cluster_data_plane_enabled,
    :public_s3_enabled,
    :metadata_schema
  ]
  @derive {Inspect, except: [:internal_secret]}
  defstruct @enforce_keys

  @worker_defaults %{
    multipart_gc: true,
    content_gc: true,
    cas_gc: true,
    packer: true,
    lifecycle: true,
    cross_cluster_replication: true,
    repair: false,
    scrub: false
  }

  @type mode :: :standalone | :cluster
  @type node_role :: :data | :metadata
  @type cluster_topology :: :none | :static | :dns
  @type cluster_member :: %{required(:id) => String.t(), required(:endpoint) => node()}
  @type metadata_schema :: :v1 | :v2

  @type t :: %__MODULE__{
          instance: atom() | String.t(),
          auto_start: boolean(),
          data_root: String.t(),
          blob_root: String.t(),
          tmp_root: String.t(),
          ra_root: String.t(),
          metadata_root: String.t(),
          web_enabled: boolean(),
          workers: %{required(atom()) => boolean()},
          mode: mode(),
          node_role: node_role(),
          node_id: String.t(),
          cluster_name: String.t(),
          cluster_topology: cluster_topology(),
          cluster_members: [cluster_member()],
          cluster_seeds: [node() | String.t()],
          cluster_bootstrap: boolean(),
          erlang_node: node(),
          erlang_cookie: atom(),
          internal_transport_enabled: boolean(),
          internal_bind: :inet.ip_address(),
          internal_port: :inet.port_number(),
          internal_advertised_url: String.t() | nil,
          internal_secret: String.t() | nil,
          internal_tls_certfile: String.t() | nil,
          internal_tls_keyfile: String.t() | nil,
          internal_auth_skew_seconds: pos_integer(),
          replication_factor: pos_integer(),
          write_quorum: pos_integer(),
          allow_degraded_writes: boolean(),
          cluster_data_plane_enabled: boolean(),
          public_s3_enabled: boolean(),
          metadata_schema: metadata_schema()
        }

  @spec from_application_env() :: {:ok, t()} | {:error, String.t()}
  def from_application_env do
    configured = Application.get_env(:ex_storage_service, :instance_config, [])

    application_roots =
      [
        data_root: Application.get_env(:ex_storage_service, :data_root),
        blob_root: Application.get_env(:ex_storage_service, :blob_root),
        tmp_root: Application.get_env(:ex_storage_service, :tmp_root),
        ra_root: Application.get_env(:ex_storage_service, :ra_root),
        metadata_root: Application.get_env(:ex_storage_service, :metadata_root)
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    application_roots
    |> Keyword.merge(configured)
    |> new()
  end

  @spec new(keyword() | map()) :: {:ok, t()} | {:error, String.t()}
  def new(%__MODULE__{} = config), do: validate(config)
  def new(opts) when is_map(opts), do: opts |> Map.to_list() |> new()

  def new(opts) when is_list(opts) do
    mode = Keyword.get(opts, :mode, :standalone)
    node_role = Keyword.get(opts, :node_role, :data)

    data_root =
      Keyword.get(
        opts,
        :data_root,
        Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")
      )

    use_application_roots? = not Keyword.has_key?(opts, :data_root)

    blob_root =
      Keyword.get(
        opts,
        :blob_root,
        application_root(:blob_root, Path.join(data_root, "cas"), use_application_roots?)
      )

    use_application_tmp_root? =
      not Keyword.has_key?(opts, :data_root) and not Keyword.has_key?(opts, :blob_root)

    with {:ok, worker_overrides} <- normalize_workers(Keyword.get(opts, :workers, %{})) do
      worker_defaults =
        if node_role == :metadata,
          do: Map.new(@worker_defaults, fn {worker, _enabled} -> {worker, false} end),
          else: @worker_defaults

      workers = Map.merge(worker_defaults, worker_overrides)

      config = %__MODULE__{
        instance: Keyword.get(opts, :instance, :default),
        auto_start: Keyword.get(opts, :auto_start, true),
        data_root: data_root,
        blob_root: blob_root,
        tmp_root:
          Keyword.get(
            opts,
            :tmp_root,
            application_root(:tmp_root, Path.join(blob_root, "tmp"), use_application_tmp_root?)
          ),
        ra_root:
          Keyword.get(
            opts,
            :ra_root,
            application_root(:ra_root, Path.join(data_root, "ra"), use_application_roots?)
          ),
        metadata_root:
          Keyword.get(
            opts,
            :metadata_root,
            application_root(
              :metadata_root,
              Path.join(data_root, "concord"),
              use_application_roots?
            )
          ),
        web_enabled: Keyword.get(opts, :web_enabled, true),
        workers: workers,
        mode: mode,
        node_role: node_role,
        node_id: Keyword.get(opts, :node_id, "default"),
        cluster_name: Keyword.get(opts, :cluster_name, "ex_storage_service"),
        cluster_topology: Keyword.get(opts, :cluster_topology, :none),
        cluster_members: Keyword.get(opts, :cluster_members, []),
        cluster_seeds: Keyword.get(opts, :cluster_seeds, []),
        cluster_bootstrap: Keyword.get(opts, :cluster_bootstrap, false),
        erlang_node: Keyword.get(opts, :erlang_node, node()),
        erlang_cookie: Keyword.get(opts, :erlang_cookie, Node.get_cookie()),
        internal_transport_enabled: mode == :cluster and node_role == :data,
        internal_bind: Keyword.get(opts, :internal_bind, {127, 0, 0, 1}),
        internal_port: Keyword.get(opts, :internal_port, 9100),
        internal_advertised_url: Keyword.get(opts, :internal_advertised_url),
        internal_secret: Keyword.get(opts, :internal_secret),
        internal_tls_certfile: Keyword.get(opts, :internal_tls_certfile),
        internal_tls_keyfile: Keyword.get(opts, :internal_tls_keyfile),
        internal_auth_skew_seconds: Keyword.get(opts, :internal_auth_skew_seconds, 300),
        replication_factor: Keyword.get(opts, :replication_factor, cluster_default(mode, 2, 1)),
        write_quorum: Keyword.get(opts, :write_quorum, cluster_default(mode, 2, 1)),
        allow_degraded_writes: Keyword.get(opts, :allow_degraded_writes, false),
        cluster_data_plane_enabled: Keyword.get(opts, :cluster_data_plane_enabled, false),
        public_s3_enabled: Keyword.get(opts, :public_s3_enabled, true),
        metadata_schema: Keyword.get(opts, :metadata_schema, :v2)
      }

      validate(config)
    end
  end

  @spec worker_enabled?(t(), atom()) :: boolean()
  def worker_enabled?(%__MODULE__{workers: workers}, worker),
    do: Map.get(workers, worker, false)

  @spec worker_defaults() :: map()
  def worker_defaults, do: @worker_defaults

  defp validate(%__MODULE__{instance: instance})
       when not is_atom(instance) and not is_binary(instance),
       do: {:error, "instance must be an atom or non-empty string"}

  defp validate(%__MODULE__{instance: ""}),
    do: {:error, "instance must be an atom or non-empty string"}

  defp validate(%__MODULE__{auto_start: value}) when not is_boolean(value),
    do: {:error, "auto start must be a boolean"}

  defp validate(%__MODULE__{web_enabled: value}) when not is_boolean(value),
    do: {:error, "web enabled must be a boolean"}

  defp validate(%__MODULE__{} = config) do
    with :ok <- validate_paths(config),
         :ok <- validate_workers(config.workers) do
      validate_storage(config)
    end
  end

  defp validate_paths(config) do
    [:data_root, :blob_root, :tmp_root, :ra_root, :metadata_root]
    |> Enum.find(fn key ->
      value = Map.fetch!(config, key)
      not is_binary(value) or value == ""
    end)
    |> case do
      nil -> :ok
      key -> {:error, "#{key} must be a non-empty path"}
    end
  end

  defp validate_workers(workers) when is_map(workers) do
    unknown = Map.keys(workers) -- Map.keys(@worker_defaults)

    cond do
      unknown != [] ->
        {:error, "unknown workers: #{inspect(Enum.sort(unknown))}"}

      Enum.any?(workers, fn {_worker, enabled} -> not is_boolean(enabled) end) ->
        {:error, "worker values must be booleans"}

      true ->
        :ok
    end
  end

  defp validate_workers(_workers), do: {:error, "workers must be a map or keyword list"}

  defp normalize_workers(workers) when is_map(workers), do: {:ok, workers}

  defp normalize_workers(workers) when is_list(workers) do
    if Keyword.keyword?(workers),
      do: {:ok, Map.new(workers)},
      else: {:error, "workers must be a map or keyword list"}
  end

  defp normalize_workers(_workers), do: {:error, "workers must be a map or keyword list"}

  defp application_root(key, fallback, true),
    do: Application.get_env(:ex_storage_service, key, fallback)

  defp application_root(_key, fallback, false), do: fallback

  defp cluster_default(:cluster, cluster_value, _standalone_value), do: cluster_value
  defp cluster_default(_mode, _cluster_value, standalone_value), do: standalone_value

  defp validate_storage(%__MODULE__{mode: mode}) when mode not in [:standalone, :cluster],
    do: {:error, "mode must be :standalone or :cluster, got: #{inspect(mode)}"}

  defp validate_storage(%__MODULE__{node_role: role}) when role not in [:data, :metadata],
    do: {:error, "node role must be :data or :metadata, got: #{inspect(role)}"}

  defp validate_storage(%__MODULE__{cluster_topology: topology})
       when topology not in [:none, :static, :dns],
       do: {:error, "cluster topology must be :none, :static, or :dns, got: #{inspect(topology)}"}

  defp validate_storage(%__MODULE__{cluster_bootstrap: value}) when not is_boolean(value),
    do: {:error, "cluster bootstrap must be a boolean"}

  defp validate_storage(%__MODULE__{metadata_schema: schema}) when schema not in [:v1, :v2],
    do: {:error, "metadata schema must be :v1 or :v2, got: #{inspect(schema)}"}

  defp validate_storage(%__MODULE__{replication_factor: rf}) when not is_integer(rf) or rf < 1,
    do: {:error, "replication factor must be an integer greater than or equal to 1"}

  defp validate_storage(%__MODULE__{write_quorum: quorum})
       when not is_integer(quorum) or quorum < 1,
       do: {:error, "write quorum must be an integer greater than or equal to 1"}

  defp validate_storage(%__MODULE__{allow_degraded_writes: value}) when not is_boolean(value),
    do: {:error, "allow degraded writes must be a boolean"}

  defp validate_storage(%__MODULE__{cluster_data_plane_enabled: value})
       when not is_boolean(value),
       do: {:error, "cluster data plane enabled must be a boolean"}

  defp validate_storage(%__MODULE__{public_s3_enabled: value}) when not is_boolean(value),
    do: {:error, "public S3 enabled must be a boolean"}

  defp validate_storage(%__MODULE__{internal_transport_enabled: value})
       when not is_boolean(value),
       do: {:error, "internal transport enabled must be a boolean"}

  defp validate_storage(%__MODULE__{internal_port: port})
       when not is_integer(port) or port < 1 or port > 65_535,
       do: {:error, "internal port must be an integer between 1 and 65535"}

  defp validate_storage(%__MODULE__{internal_secret: secret})
       when not is_nil(secret) and (not is_binary(secret) or secret == ""),
       do: {:error, "internal secret must be nil or a non-empty binary"}

  defp validate_storage(%__MODULE__{internal_auth_skew_seconds: skew})
       when not is_integer(skew) or skew < 1,
       do: {:error, "internal auth skew must be an integer greater than or equal to 1"}

  defp validate_storage(%__MODULE__{} = config) do
    with :ok <- validate_internal_bind(config.internal_bind),
         :ok <- validate_tls_pair(config),
         :ok <- validate_internal_transport_derivation(config) do
      validate_storage_mode(config)
    end
  end

  defp validate_storage_mode(%__MODULE__{replication_factor: rf, write_quorum: quorum})
       when quorum > rf,
       do: {:error, "write quorum must satisfy 1 <= W <= RF (got W=#{quorum}, RF=#{rf})"}

  defp validate_storage_mode(%__MODULE__{mode: :standalone, node_role: :metadata}),
    do: {:error, "metadata role requires cluster mode"}

  defp validate_storage_mode(%__MODULE__{mode: :cluster} = config), do: validate_cluster(config)

  defp validate_storage_mode(config) do
    with :ok <- validate_internal_advertised_url(config), do: {:ok, config}
  end

  defp validate_cluster(config) do
    with :ok <- require_disabled(config.public_s3_enabled, "public S3 listener"),
         :ok <- require_disabled(config.web_enabled, "web listener"),
         :ok <- require_disabled(config.cluster_data_plane_enabled, "cluster data plane"),
         :ok <- validate_cluster_identity(config),
         :ok <- validate_cluster_members(config),
         :ok <- validate_cluster_seeds(config),
         :ok <- validate_internal_advertised_url(config),
         :ok <- validate_metadata_role(config) do
      {:ok, config}
    end
  end

  defp require_disabled(false, _feature), do: :ok

  defp require_disabled(true, feature),
    do: {:error, "cluster mode keeps the #{feature} disabled until the data plane is complete"}

  defp validate_cluster_identity(config) do
    cond do
      not is_binary(config.node_id) or config.node_id == "" ->
        {:error, "cluster mode requires a non-empty stable node id"}

      not is_binary(config.cluster_name) or config.cluster_name == "" ->
        {:error, "cluster mode requires a non-empty cluster name"}

      config.cluster_topology not in [:static, :dns] ->
        {:error, "cluster mode requires :static or :dns topology"}

      not is_atom(config.erlang_node) or config.erlang_node == :nonode@nohost ->
        {:error, "cluster mode requires a distributed Erlang node name"}

      not is_atom(config.erlang_cookie) or config.erlang_cookie == :nocookie ->
        {:error, "cluster mode requires a non-default Erlang cookie"}

      true ->
        :ok
    end
  end

  defp validate_cluster_members(%__MODULE__{cluster_members: members} = config)
       when is_list(members) do
    ids = Enum.map(members, fn member -> if is_map(member), do: Map.get(member, :id) end)

    endpoints =
      Enum.map(members, fn member -> if is_map(member), do: Map.get(member, :endpoint) end)

    cond do
      length(members) != 3 ->
        {:error, "cluster mode requires exactly three ordered metadata voters"}

      not Enum.all?(members, &valid_cluster_member?/1) ->
        {:error,
         "cluster members must contain non-empty string ids and distributed node endpoints"}

      MapSet.size(MapSet.new(ids)) != length(ids) ->
        {:error, "cluster member ids must be unique"}

      MapSet.size(MapSet.new(endpoints)) != length(endpoints) ->
        {:error, "cluster member endpoints must be unique"}

      not Enum.any?(members, fn member ->
        member.id == config.node_id and member.endpoint == config.erlang_node
      end) ->
        {:error, "local node id and Erlang endpoint must match one configured cluster member"}

      true ->
        :ok
    end
  end

  defp validate_cluster_members(_config), do: {:error, "cluster members must be a list"}

  defp valid_cluster_member?(%{id: id, endpoint: endpoint}) do
    is_binary(id) and id != "" and is_atom(endpoint) and endpoint != :nonode@nohost and
      String.contains?(Atom.to_string(endpoint), "@")
  end

  defp valid_cluster_member?(_member), do: false

  defp validate_cluster_seeds(%__MODULE__{cluster_topology: :static, cluster_seeds: seeds}) do
    if is_list(seeds) and seeds != [] and Enum.all?(seeds, &is_atom/1),
      do: :ok,
      else: {:error, "static cluster topology requires Erlang node seeds"}
  end

  defp validate_cluster_seeds(%__MODULE__{cluster_topology: :dns, cluster_seeds: seeds}) do
    if is_list(seeds) and seeds != [] and Enum.all?(seeds, &(is_binary(&1) and &1 != "")),
      do: :ok,
      else: {:error, "DNS cluster topology requires one or more DNS queries"}
  end

  defp validate_metadata_role(%__MODULE__{node_role: :metadata, workers: workers}) do
    if Enum.any?(workers, fn {_worker, enabled} -> enabled end),
      do: {:error, "metadata role cannot enable data-plane workers"},
      else: :ok
  end

  defp validate_metadata_role(_config), do: :ok

  defp validate_internal_bind(bind) do
    case :inet.ntoa(bind) do
      address when is_list(address) -> :ok
      {:error, :einval} -> {:error, "internal bind must be a parsed IPv4 or IPv6 address"}
    end
  rescue
    _error in [ArgumentError, FunctionClauseError] ->
      {:error, "internal bind must be a parsed IPv4 or IPv6 address"}
  end

  defp validate_tls_pair(%__MODULE__{
         internal_tls_certfile: certfile,
         internal_tls_keyfile: keyfile
       }) do
    case {certfile, keyfile} do
      {nil, nil} ->
        :ok

      {certfile, keyfile}
      when is_binary(certfile) and certfile != "" and is_binary(keyfile) and keyfile != "" ->
        :ok

      _other ->
        {:error, "internal TLS certificate and key paths must be configured together"}
    end
  end

  defp validate_internal_transport_derivation(%__MODULE__{} = config) do
    expected = config.mode == :cluster and config.node_role == :data

    if config.internal_transport_enabled == expected,
      do: :ok,
      else: {:error, "internal transport enablement must be derived from cluster data-node role"}
  end

  defp validate_internal_advertised_url(%__MODULE__{mode: :standalone}), do: :ok
  defp validate_internal_advertised_url(%__MODULE__{node_role: :metadata}), do: :ok

  defp validate_internal_advertised_url(%__MODULE__{internal_advertised_url: url}) do
    with true <- is_binary(url),
         {:ok,
          %URI{
            scheme: scheme,
            host: host,
            port: port,
            userinfo: nil,
            query: nil,
            fragment: nil,
            path: path
          }} <- URI.new(url),
         true <- scheme in ["http", "https"],
         true <- is_binary(host) and host != "",
         true <- is_nil(port) or port in 1..65_535,
         true <- path in [nil, "", "/"] do
      :ok
    else
      _invalid ->
        {:error, "cluster data nodes require a valid HTTP(S) internal advertised URL"}
    end
  end
end
