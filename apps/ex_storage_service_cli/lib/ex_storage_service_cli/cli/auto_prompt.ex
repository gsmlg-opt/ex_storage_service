defmodule ExStorageServiceCli.CLI.AutoPrompt do
  @moduledoc "Opt-in bounded model/registry questions on stderr, with terminal-safe sensitive input."
  alias ExStorageServiceCli.{
    Error,
    CLI.AWSInput,
    CLI.Registry,
    CLI.Signals.Native,
    S3.OperationCatalog
  }

  def mode(options) do
    cond do
      options[:help] || options[:generate_cli_skeleton] ->
        {:ok, "off"}

      options[:cli_auto_prompt] && options[:no_cli_auto_prompt] ->
        failure(:conflicting_auto_prompt)

      options[:no_cli_auto_prompt] ->
        {:ok, "off"}

      options[:cli_auto_prompt] ->
        {:ok, "on"}

      true ->
        value = String.downcase(options[:cli_auto_prompt_mode] || "off")
        if value in ~w(on on-partial off), do: {:ok, value}, else: failure(:invalid_auto_prompt)
    end
  end

  def run(operation, input, options, opts \\ []) do
    with {:ok, mode} <- mode(options),
         {:ok, operation_model} <- OperationCatalog.operation(operation) do
      shape =
        if operation_model["input"],
          do: OperationCatalog.shape(operation_model["input"]["shape"]),
          else: %{}

      missing = Enum.reject(shape["required"] || [], &Map.has_key?(input, &1))

      if mode == "off" or (mode == "on-partial" and missing == []) do
        {:ok, input}
      else
        with :ok <- terminal(opts),
             {:ok, input} <- parameters(missing, shape["members"] || %{}, input, options, opts),
             :ok <- confirm(operation, opts),
             do: {:ok, input}
      end
    end
  end

  def arguments(frontend, command, args, options, opts \\ []) do
    with {:ok, mode} <- mode(options) do
      {arity, _, _} = Registry.spec(frontend, command)
      count = max(arity.first - length(args), 0)

      if mode == "off" or count == 0 do
        {:ok, args}
      else
        with :ok <- terminal(opts) do
          Enum.reduce_while(1..count, {:ok, args}, fn _, {:ok, args} ->
            label = argument_label(command, length(args))

            case question(label, false, opts) do
              {:ok, value} when value != "" -> {:cont, {:ok, args ++ [value]}}
              {:ok, _} -> {:halt, failure(:auto_prompt_empty)}
              error -> {:halt, error}
            end
          end)
        end
      end
    end
  end

  def confirm(command, opts \\ []) do
    with {:ok, answer} <- question("Run #{safe(command)}? [y/N]", false, opts) do
      if String.downcase(answer) in ["y", "yes"], do: :ok, else: failure(:auto_prompt_declined)
    end
  end

  defp parameters(names, bindings, input, options, opts) do
    Enum.reduce_while(names, {:ok, input}, fn name, {:ok, input} ->
      binding = bindings[name]
      shape = OperationCatalog.shape(binding["shape"]) || %{}

      sensitive =
        binding["sensitive"] == true or shape["sensitive"] == true or
          String.contains?(String.downcase(name), [
            "secret",
            "password",
            "token",
            "ssecustomerkey"
          ])

      with {:ok, value} <- question(name, sensitive, opts),
           {:ok, decoded} <- AWSInput.prompt_value(binding, value, options, opts) do
        {:cont, {:ok, Map.put(input, name, decoded)}}
      else
        error -> {:halt, error}
      end
    end)
  end

  defp question(label, sensitive, opts) do
    writer = opts[:prompt_write] || fn bytes -> IO.write(:stderr, bytes) end

    ask = fn ->
      writer.(safe(label) <> ": ")

      if opts[:prompt_read],
        do: opts[:prompt_read].(%{label: label, sensitive: sensitive}),
        else: read_line()
    end

    result =
      if sensitive && !opts[:prompt_read] do
        with {:ok, terminal} <- Native.suppress_stdin_echo() do
          try do
            ask.()
          after
            Native.restore_terminal(terminal)
            writer.("\n")
          end
        else
          _ -> failure(:auto_prompt_terminal)
        end
      else
        ask.()
      end

    case result do
      {:ok, bytes} when is_binary(bytes) and byte_size(bytes) <= 65_536 ->
        {:ok, String.trim_trailing(bytes, "\n") |> String.trim_trailing("\r")}

      :eof ->
        failure(:auto_prompt_eof)

      {:error, %Error{}} = error ->
        error

      _ ->
        failure(:auto_prompt_input)
    end
  rescue
    _ -> failure(:auto_prompt_input)
  catch
    :exit, _ -> failure(:auto_prompt_input)
  end

  defp read_line, do: read_line([], 0)

  defp read_line(chunks, size) do
    if size >= 65_536 do
      failure(:auto_prompt_input_limit)
    else
      case IO.binread(:stdio, 1) do
        "\n" -> {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}
        :eof -> :eof
        {:error, _} -> failure(:auto_prompt_input)
        byte -> read_line([byte | chunks], size + byte_size(byte))
      end
    end
  end

  defp terminal(opts) do
    if Keyword.has_key?(opts, :tty) do
      if opts[:tty] == true, do: :ok, else: failure(:auto_prompt_requires_tty)
    else
      case Native.suppress_stdin_echo() do
        {:ok, terminal} ->
          Native.restore_terminal(terminal)
          :ok

        _ ->
          failure(:auto_prompt_requires_tty)
      end
    end
  rescue
    _ -> failure(:auto_prompt_requires_tty)
  end

  defp argument_label([name], 0) when name in ~w(cp mv sync mirror diff get put), do: "Source"

  defp argument_label([name], 1) when name in ~w(cp mv sync mirror diff get put),
    do: "Destination"

  defp argument_label(_, index), do: "Argument #{index + 1}"
  defp safe(value), do: value |> to_string() |> String.replace(~r/[\x00-\x1F\x7F]/u, "?")

  defp failure(code),
    do: {:error, Error.new(:client, "CLI auto-prompt could not complete", code: code)}
end
