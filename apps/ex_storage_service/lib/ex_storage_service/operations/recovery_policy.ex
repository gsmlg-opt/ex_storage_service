defmodule ExStorageService.Operations.RecoveryPolicy do
  @moduledoc false

  alias ExStorageService.Metadata.Keys

  # A reviewed resolution changes only the terminal state and its timestamp.
  # The source content identity, owner/generation, deadlines and history remain
  # part of verification. This never performs or infers a reconciliation.
  def metadata_matches?(key, source, current, resolutions) do
    source == current or resolved_intent?(key, source, current, resolutions)
  end

  def unresolved?(key, value) do
    cond do
      String.starts_with?(key, Keys.operation_intent_prefix()) ->
        value[:state] not in [:committed, :aborted]

      String.starts_with?(key, ["ess:gateway-write:", "ess:gateway-delete:", "ess:v2:gc_lock:"]) ->
        true

      true ->
        # Publication guards persist after committed writes. They protect bytes;
        # their presence is not an unresolved collector or operation lease.
        false
    end
  end

  defp resolved_intent?(key, source, current, resolutions)
       when is_map(source) and is_map(current) and is_map(resolutions) do
    source[:state] in [:pending, :unknown] and
      current[:state] in [:committed, :aborted] and
      is_binary(source[:operation_id]) and
      key == Keys.operation_intent(source.operation_id) and
      Map.get(resolutions, source.operation_id) == current.state and
      is_integer(source[:updated_at_ms]) and is_integer(current[:updated_at_ms]) and
      current.updated_at_ms >= source.updated_at_ms and
      Map.drop(source, [:state, :updated_at_ms]) == Map.drop(current, [:state, :updated_at_ms])
  end

  defp resolved_intent?(_, _, _, _), do: false
end
