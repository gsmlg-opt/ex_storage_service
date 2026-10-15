defmodule ExStorageServiceCli.CLI.NativeEvents do
  @moduledoc "Versioned native NDJSON item events and one invocation summary."
  alias ExStorageServiceCli.{Error, Presenter}

  def with_invocation(invocation, function) do
    state = :atomics.new(2, signed: false)

    options =
      Map.merge(invocation.options, %{
        event_state: state,
        event_operation: Enum.join(invocation.command, " "),
        event_target: List.first(invocation.args)
      })

    invocation = %{invocation | options: options}
    result = function.(invocation)

    with :ok <- error_event(result, options), :ok <- summary(result, options), do: result
  end

  def item(data, options) do
    bytes = value(data, :bytes) || value(data, :size) || value(data, "Size")

    event =
      base(options, "item", "ok")
      |> Map.merge(%{data: data, bytes: bytes, error: nil, request_id: nil})

    with :ok <- Presenter.write(event, options) do
      if options[:event_state] do
        :atomics.add(options.event_state, 1, 1)
        if is_integer(bytes) and bytes >= 0, do: :atomics.add(options.event_state, 2, bytes)
      end

      :ok
    end
  end

  defp error_event({:error, %Error{code: :broken_pipe}}, _options), do: :ok

  defp error_event({:error, %Error{} = error}, options) do
    event =
      base(options, "error", "error")
      |> Map.merge(%{
        bytes: nil,
        request_id: error.request_id,
        error: %{category: error.category, code: error.code, message: error.message}
      })

    Presenter.write(event, options)
  end

  defp error_event(_, _options), do: :ok

  defp summary({:error, %Error{code: :broken_pipe}}, _options), do: :ok

  defp summary(result, options) do
    event = base(options, "summary", if(result == :ok, do: "ok", else: "error"))

    event =
      Map.merge(event, %{
        items: :atomics.get(options.event_state, 1),
        bytes: :atomics.get(options.event_state, 2),
        error: nil,
        request_id: nil
      })

    Presenter.write(event, options)
  end

  defp base(options, type, status),
    do: %{
      schema_version: 1,
      type: type,
      status: status,
      operation: options[:event_operation],
      target: options[:event_target]
    }

  defp value(data, key) when is_map(data), do: Map.get(data, key)
  defp value(_, _), do: nil
end
