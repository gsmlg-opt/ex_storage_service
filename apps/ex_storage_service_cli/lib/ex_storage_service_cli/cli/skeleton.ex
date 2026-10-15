defmodule ExStorageServiceCli.CLI.Skeleton do
  @moduledoc "Pure model skeletons matching the pinned AWS argument generator."
  alias ExStorageServiceCli.{Error, S3.OperationCatalog}

  def generate(operation, kind \\ :input)

  def generate(operation, kind) when kind in [:input, :output] do
    with {:ok, operation} <- OperationCatalog.operation(operation) do
      binding = operation[Atom.to_string(kind)]
      {:ok, if(binding, do: value(binding["shape"], kind, [], ""), else: %{})}
    end
  end

  def generate(_operation, _kind),
    do: {:error, Error.new(:syntax, "Unsupported skeleton format", code: :unsupported_skeleton)}

  defp value(name, kind, stack, member_name) do
    shape = OperationCatalog.shape(name)
    stack = [name | stack]

    case shape["type"] do
      "structure" ->
        if Enum.count(stack, &(&1 == name)) > 1 do
          %{}
        else
          Map.new(shape["members"] || %{}, fn {member, binding} ->
            {member, value(binding["shape"], kind, stack, member)}
          end)
        end

      "list" ->
        [
          value(
            shape["member"]["shape"],
            kind,
            stack,
            if(kind == :output, do: shape["member"]["shape"], else: "")
          )
        ]

      "map" ->
        %{"KeyName" => value(shape["value"]["shape"], kind, stack, "")}

      "string" ->
        if(kind == :output, do: member_name, else: List.first(shape["enum"] || []) || "")

      type when type in ~w(integer long) ->
        0

      type when type in ~w(float double) ->
        0.0

      "boolean" ->
        true

      "timestamp" ->
        "1970-01-01T00:00:00"

      "blob" ->
        nil
    end
  end
end
