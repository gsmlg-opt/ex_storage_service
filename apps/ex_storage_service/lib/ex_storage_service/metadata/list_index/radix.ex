defmodule ExStorageService.Metadata.ListIndex.Radix do
  @moduledoc """
  Bounded exact-read compressed byte-radix transaction planner.

  The caller injects linearizable current-record reads and commits `operations/1`
  with its metadata and descriptor changes in one transaction. This module never
  commits, scans, or owns a second metadata authority. A seek retains only its
  path and successor frontier; callers bracket page reads with their descriptor
  revision barrier.

  Terminals contain bounded listing summaries, not arbitrary object metadata.
  A planner may accumulate a bounded mutation batch; create a fresh planner for
  each transaction attempt rather than retaining it between requests.
  """

  @max_key_bytes 16_384
  @max_node_bytes 32_768
  @max_terminal_bytes 8_192

  @enforce_keys [:read, :namespace]
  defstruct [:read, :namespace, :root_id, originals: %{}, dirty: %{}, count_delta: 0]

  def max_key_bytes, do: @max_key_bytes
  def max_node_bytes, do: @max_node_bytes
  def max_terminal_bytes, do: @max_terminal_bytes

  def new(read_fun, namespace, root_id)
      when is_function(read_fun, 1) and is_binary(namespace) and
             (is_nil(root_id) or (is_binary(root_id) and byte_size(root_id) == 16)),
      do: %__MODULE__{read: read_fun, namespace: namespace, root_id: root_id}

  def put(%__MODULE__{} = state, key, summary) do
    with :ok <- valid_key(key),
         :ok <- valid_terminal(summary),
         {:ok, state, root_id, added} <- insert(state, state.root_id, key, summary, 0, []) do
      {:ok, %{state | root_id: root_id, count_delta: state.count_delta + added}}
    end
  end

  def delete(%__MODULE__{} = state, key) do
    with :ok <- valid_key(key),
         {:ok, state, root_id, removed} <- remove(state, state.root_id, key, 0, []) do
      {:ok, %{state | root_id: root_id, count_delta: state.count_delta - removed}}
    end
  end

  @doc "One lower-bound terminal; exclusive seeks also provide structural successors."
  def seek(state, lower_bound, exclusive? \\ false)

  def seek(%__MODULE__{} = state, lower_bound, exclusive?) when is_boolean(exclusive?) do
    with :ok <- valid_key(lower_bound) do
      stats = %{reads: 0, bytes: 0, visited: 0, depth: 0}

      case seek_node(state, state.root_id, lower_bound, exclusive?, [], 0, [], nil, stats) do
        {:ok, result, stats} -> {:ok, result, stats}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Deduplicated backend transaction operations, plus descriptor/count changes."
  def operations(%__MODULE__{} = state) do
    compare =
      state.originals
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.flat_map(fn {id, record} ->
        key = storage_key(state, id)

        case record do
          nil ->
            if Map.get(state.dirty, id) == :deleted,
              do: [],
              else: [{:exists, key, :==, false}]

          %{mod_revision: revision} ->
            [{:mod_revision, key, :==, revision}]
        end
      end)

    success =
      state.dirty
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.flat_map(fn {id, node} ->
        key = storage_key(state, id)

        case {Map.fetch!(state.originals, id), node} do
          {nil, :deleted} -> []
          {_, :deleted} -> [{:delete, {:key, key}, %{}}]
          {%{value: ^node}, _} -> []
          _ -> [{:put, key, node, %{}}]
        end
      end)

    %{
      compare: compare,
      success: success,
      failure: [],
      root_id: state.root_id,
      count_delta: state.count_delta
    }
  end

  defp insert(state, nil, key, summary, _depth, _ancestors) do
    {state, id} = allocate(state, node(key, summary, []))
    {:ok, state, id, 1}
  end

  defp insert(state, id, key, summary, depth, ancestors) do
    with :ok <- path_guard(id, depth, ancestors),
         {:ok, current, state} <- observed(state, id),
         :ok <- valid_node(current, ancestors == [], incoming_edge(key, ancestors)),
         :ok <- valid_depth(depth + byte_size(current.label)) do
      shared = common_length(current.label, key)

      if shared == byte_size(current.label) do
        rest = suffix(key, shared)
        insert_matched(state, id, current, rest, summary, depth, [id | ancestors])
      else
        split_insert(state, id, current, key, summary, shared)
      end
    end
  end

  defp insert_matched(state, id, current, <<>>, summary, _depth, _ancestors) do
    added = if is_nil(current.terminal), do: 1, else: 0

    with {:ok, state} <- store(state, id, %{current | terminal: summary}),
         do: {:ok, state, id, added}
  end

  defp insert_matched(state, id, current, <<byte, _::binary>> = rest, summary, depth, ancestors) do
    child = child_id(current.children, byte)

    with {:ok, state, child, added} <-
           insert(state, child, rest, summary, depth + byte_size(current.label), ancestors),
         {:ok, state} <-
           store(state, id, %{current | children: replace_child(current.children, byte, child)}) do
      {:ok, state, id, added}
    end
  end

  defp split_insert(state, id, current, key, summary, shared) do
    old_suffix = suffix(current.label, shared)
    {state, old_id} = allocate(state, %{current | label: old_suffix})
    old_edge = {:binary.first(old_suffix), old_id}
    new_suffix = suffix(key, shared)

    {state, terminal, children} =
      case new_suffix do
        <<>> ->
          {state, summary, [old_edge]}

        <<byte, _::binary>> ->
          {state, new_id} = allocate(state, node(new_suffix, summary, []))
          {state, nil, Enum.sort([old_edge, {byte, new_id}])}
      end

    with {:ok, state} <- store(state, id, node(binary_part(key, 0, shared), terminal, children)),
         do: {:ok, state, id, 1}
  end

  defp remove(state, nil, _key, _depth, _ancestors), do: {:ok, state, nil, 0}

  defp remove(state, id, key, depth, ancestors) do
    with :ok <- path_guard(id, depth, ancestors),
         {:ok, current, state} <- observed(state, id),
         :ok <- valid_node(current, ancestors == [], incoming_edge(key, ancestors)),
         :ok <- valid_depth(depth + byte_size(current.label)) do
      if starts_with?(key, current.label) do
        remove_matched(state, id, current, suffix(key, byte_size(current.label)), depth, [
          id | ancestors
        ])
      else
        {:ok, state, id, 0}
      end
    end
  end

  defp remove_matched(state, id, %{terminal: nil}, <<>>, _depth, _ancestors),
    do: {:ok, state, id, 0}

  defp remove_matched(state, id, current, <<>>, depth, ancestors),
    do: normalize(state, id, %{current | terminal: nil}, 1, depth, ancestors)

  defp remove_matched(state, id, current, <<byte, _::binary>> = rest, depth, ancestors) do
    with {:ok, state, child, removed} <-
           remove(
             state,
             child_id(current.children, byte),
             rest,
             depth + byte_size(current.label),
             ancestors
           ) do
      if removed == 0 do
        {:ok, state, id, 0}
      else
        normalize(
          state,
          id,
          %{current | children: replace_child(current.children, byte, child)},
          removed,
          depth,
          ancestors
        )
      end
    end
  end

  defp normalize(state, id, %{terminal: nil, children: []}, removed, _depth, _ancestors),
    do: {:ok, mark_deleted(state, id), nil, removed}

  defp normalize(
         state,
         id,
         %{terminal: nil, children: [{edge, child}]} = current,
         removed,
         depth,
         ancestors
       ) do
    with :ok <- path_guard(child, depth + byte_size(current.label), ancestors),
         {:ok, descendant, state} <- observed(state, child),
         :ok <- valid_node(descendant, false, edge),
         :ok <- valid_depth(depth + byte_size(current.label) + byte_size(descendant.label)),
         {:ok, state} <-
           store(state, id, %{descendant | label: current.label <> descendant.label}) do
      {:ok, mark_deleted(state, child), id, removed}
    end
  end

  defp normalize(state, id, current, removed, _depth, _ancestors) do
    with {:ok, state} <- store(state, id, current), do: {:ok, state, id, removed}
  end

  defp seek_node(_state, nil, _bound, _exclusive, _labels, _depth, _ancestors, _edge, stats),
    do: {:ok, nil, stats}

  defp seek_node(state, id, bound, exclusive, labels, depth, ancestors, edge, stats) do
    with :ok <- path_guard(id, depth, ancestors),
         {:ok, current, read_bytes, reads} <- read_node(state, id),
         :ok <- valid_node(current, ancestors == [], edge),
         :ok <- valid_depth(depth + byte_size(current.label)) do
      depth = depth + byte_size(current.label)
      labels = [current.label | labels]

      stats = %{
        stats
        | reads: stats.reads + reads,
          bytes: stats.bytes + read_bytes,
          visited: stats.visited + 1,
          depth: max(stats.depth, depth)
      }

      case relative_bound(current.label, bound) do
        :before ->
          {:ok, nil, stats}

        next_bound ->
          if current.terminal != nil and
               (next_bound == :minimum or (next_bound == <<>> and not exclusive)) do
            key = labels |> Enum.reverse() |> IO.iodata_to_binary()
            {:ok, {key, current.terminal}, stats}
          else
            seek_children(
              state,
              current.children,
              next_bound,
              exclusive,
              labels,
              depth,
              [id | ancestors],
              stats
            )
          end
      end
    end
  end

  defp seek_children(_state, [], _bound, _exclusive, _labels, _depth, _ancestors, stats),
    do: {:ok, nil, stats}

  defp seek_children(
         state,
         [{edge, child} | rest],
         bound,
         exclusive,
         labels,
         depth,
         ancestors,
         stats
       ) do
    next_bound = child_bound(edge, bound)

    if next_bound == :before do
      seek_children(state, rest, bound, exclusive, labels, depth, ancestors, stats)
    else
      case seek_node(state, child, next_bound, exclusive, labels, depth, ancestors, edge, stats) do
        {:ok, nil, stats} ->
          seek_children(state, rest, bound, exclusive, labels, depth, ancestors, stats)

        result ->
          result
      end
    end
  end

  defp relative_bound(_label, :minimum), do: :minimum

  defp relative_bound(label, bound) do
    shared = common_length(label, bound)

    cond do
      shared == byte_size(label) -> suffix(bound, shared)
      label > bound -> :minimum
      true -> :before
    end
  end

  defp child_bound(_edge, :minimum), do: :minimum
  defp child_bound(_edge, <<>>), do: :minimum
  defp child_bound(edge, <<first, _::binary>>) when edge < first, do: :before
  defp child_bound(edge, <<first, _::binary>>) when edge > first, do: :minimum
  defp child_bound(_edge, bound), do: bound

  defp observed(state, id) do
    case Map.fetch(state.dirty, id) do
      {:ok, :deleted} ->
        {:error, :corrupt_index}

      {:ok, value} ->
        {:ok, value, state}

      :error ->
        case Map.fetch(state.originals, id) do
          {:ok, %{value: value}} ->
            {:ok, value, state}

          _ ->
            with {:ok, %{value: value, mod_revision: revision} = record} <-
                   state.read.(storage_key(state, id)),
                 true <- is_integer(revision) and revision > 0 do
              {:ok, value, %{state | originals: Map.put(state.originals, id, record)}}
            else
              {:error, _} = error -> error
              _ -> {:error, :corrupt_index}
            end
        end
    end
  end

  defp read_node(state, id) do
    case Map.fetch(state.dirty, id) do
      {:ok, :deleted} ->
        {:error, :corrupt_index}

      {:ok, value} ->
        {:ok, value, :erlang.external_size(value), 0}

      :error ->
        with {:ok, %{value: value, mod_revision: revision}} <-
               state.read.(storage_key(state, id)),
             true <- is_integer(revision) and revision > 0 do
          {:ok, value, :erlang.external_size(value), 1}
        else
          {:error, _} = error -> error
          _ -> {:error, :corrupt_index}
        end
    end
  end

  defp allocate(state, value) do
    id = :crypto.strong_rand_bytes(16)

    if Map.has_key?(state.originals, id) do
      allocate(state, value)
    else
      state = %{
        state
        | originals: Map.put(state.originals, id, nil),
          dirty: Map.put(state.dirty, id, value)
      }

      {state, id}
    end
  end

  defp store(state, id, value) do
    with :ok <- valid_node(value, true, nil),
         do: {:ok, %{state | dirty: Map.put(state.dirty, id, value)}}
  end

  defp mark_deleted(state, id), do: %{state | dirty: Map.put(state.dirty, id, :deleted)}

  defp node(label, terminal, children),
    do: %{schema: 1, label: label, terminal: terminal, children: children}

  defp storage_key(state, id), do: state.namespace <> Base.url_encode64(id, padding: false)

  defp child_id(children, byte) do
    case List.keyfind(children, byte, 0) do
      nil -> nil
      {_, id} -> id
    end
  end

  defp replace_child(children, byte, nil), do: List.keydelete(children, byte, 0)

  defp replace_child(children, byte, id),
    do: Enum.sort(List.keystore(children, byte, 0, {byte, id}))

  defp valid_key(key) when is_binary(key) and byte_size(key) <= @max_key_bytes, do: :ok
  defp valid_key(_), do: {:error, :invalid_index_key}
  defp valid_depth(depth) when depth <= @max_key_bytes, do: :ok
  defp valid_depth(_), do: {:error, :corrupt_index}

  defp valid_terminal(nil), do: {:error, :invalid_index_summary}

  defp valid_terminal(value) when is_map(value) do
    if :erlang.external_size(value) <= @max_terminal_bytes,
      do: :ok,
      else: {:error, :invalid_index_summary}
  end

  defp valid_terminal(_), do: {:error, :invalid_index_summary}

  defp valid_node(
         %{schema: 1, label: label, terminal: terminal, children: children} = value,
         root?,
         edge
       )
       when is_binary(label) and is_list(children) do
    valid? =
      map_size(value) == 4 and (root? or label != <<>>) and
        (is_nil(edge) or (label != <<>> and :binary.first(label) == edge)) and
        byte_size(label) <= @max_key_bytes and length(children) <= 256 and
        valid_children?(children, -1) and
        (terminal != nil or length(children) >= 2) and
        (is_nil(terminal) or valid_terminal(terminal) == :ok) and
        :erlang.external_size(value) <= @max_node_bytes

    if valid?, do: :ok, else: {:error, :corrupt_index}
  end

  defp valid_node(_, _, _), do: {:error, :corrupt_index}

  defp valid_children?([], _previous), do: true

  defp valid_children?([{byte, id} | rest], previous)
       when is_integer(byte) and byte > previous and byte >= 0 and byte <= 255 and
              is_binary(id) and byte_size(id) == 16,
       do: valid_children?(rest, byte)

  defp valid_children?(_, _), do: false

  defp path_guard(id, depth, ancestors) do
    if is_binary(id) and byte_size(id) == 16 and depth <= @max_key_bytes and id not in ancestors,
      do: :ok,
      else: {:error, :corrupt_index}
  end

  defp common_length(left, right), do: common_length(left, right, 0)

  defp common_length(<<byte, left::binary>>, <<byte, right::binary>>, size),
    do: common_length(left, right, size + 1)

  defp common_length(_, _, size), do: size
  defp incoming_edge(_key, []), do: nil
  defp incoming_edge(<<byte, _::binary>>, _ancestors), do: byte
  defp suffix(value, offset), do: binary_part(value, offset, byte_size(value) - offset)
  defp starts_with?(value, prefix), do: common_length(value, prefix) == byte_size(prefix)
end
