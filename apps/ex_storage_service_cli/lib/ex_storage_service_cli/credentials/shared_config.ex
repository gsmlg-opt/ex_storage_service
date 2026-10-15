defmodule ExStorageServiceCli.Credentials.SharedConfig do
  @moduledoc "Read-only AWS INI profiles and indented service settings with binary keys."
  alias ExStorageServiceCli.Error
  @limit 1_048_576

  def load(opts \\ []) do
    env = Keyword.get(opts, :env, System.get_env())
    home = opts[:home] || env["HOME"] || System.user_home!()
    config = opts[:config_file] || env["AWS_CONFIG_FILE"] || Path.join(home, ".aws/config")

    credentials =
      opts[:credentials_file] || env["AWS_SHARED_CREDENTIALS_FILE"] ||
        Path.join(home, ".aws/credentials")

    with {:ok, sections} <- read(config),
         {:ok, credentials} <-
           if(Keyword.get(opts, :load_credentials, true), do: read(credentials), else: {:ok, %{}}) do
      profiles =
        Map.new(
          Enum.flat_map(sections, fn
            {"default", values} -> [{"default", values}]
            {"profile " <> name, values} -> [{unquote_name(name), values}]
            _ -> []
          end)
        )

      {:ok, %{config: profiles, credentials: credentials, sections: sections}}
    end
  end

  def parse(data) when is_binary(data) do
    if byte_size(data) > @limit or not String.valid?(data) or String.contains?(data, <<0>>) do
      failure(:invalid_shared_config)
    else
      data
      |> String.replace_prefix(<<239, 187, 191>>, "")
      |> String.split(~r/\r\n|\n|\r/)
      |> Enum.reduce_while({:ok, %{sections: %{}, section: nil, parent: nil}}, fn line,
                                                                                  {:ok, state} ->
        case line(line, state) do
          {:ok, state} -> {:cont, {:ok, state}}
          error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, state} -> {:ok, state.sections}
        error -> error
      end
    end
  end

  defp read(path) do
    case File.open(Path.expand(path), [:read, :binary]) do
      {:ok, file} ->
        result = IO.binread(file, @limit + 1)
        File.close(file)

        case result do
          :eof -> {:ok, %{}}
          {:error, _} -> failure(:shared_config_read_failed)
          data -> parse(data)
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, _} ->
        failure(:shared_config_read_failed)
    end
  end

  defp line(data, state) do
    trimmed = String.trim(data)

    cond do
      trimmed == "" or String.starts_with?(trimmed, ["#", ";"]) -> {:ok, state}
      String.starts_with?(trimmed, "[") -> section(trimmed, state)
      state.section == nil -> failure(:invalid_shared_config)
      true -> property(data, state)
    end
  end

  defp section(data, state) do
    case Regex.run(~r/^\[([^\[\]]+)\]\s*(?:[#;].*)?$/, data) do
      [_, name] ->
        name = String.trim(name)

        if name == "" or Map.has_key?(state.sections, name),
          do: failure(:invalid_shared_config),
          else:
            {:ok,
             %{state | sections: Map.put(state.sections, name, %{}), section: name, parent: nil}}

      _ ->
        failure(:invalid_shared_config)
    end
  end

  defp property(data, state) do
    indent = byte_size(data) - byte_size(String.trim_leading(data))

    case Regex.run(~r/^\s*([A-Za-z0-9_.-]+)\s*[:=]\s*(.*)$/, data) do
      [_, key, value] ->
        key = String.downcase(key)
        value = String.trim(value)

        case state.parent do
          {parent, parent_indent} when indent > parent_indent ->
            existing = get_in(state.sections, [state.section, parent])
            existing = if is_map(existing), do: existing, else: %{}

            if Map.has_key?(existing, key),
              do: failure(:invalid_shared_config),
              else:
                {:ok,
                 %{
                   state
                   | sections:
                       put_in(
                         state.sections,
                         [state.section, parent],
                         Map.put(existing, key, value)
                       )
                 }}

          _ ->
            if Map.has_key?(state.sections[state.section], key),
              do: failure(:invalid_shared_config),
              else:
                {:ok,
                 %{
                   state
                   | sections: put_in(state.sections, [state.section, key], value),
                     parent: if(value == "", do: {key, indent}, else: nil)
                 }}
        end

      _ ->
        failure(:invalid_shared_config)
    end
  end

  defp unquote_name(name) do
    name = String.trim(name)

    if String.starts_with?(name, "\"") and String.ends_with?(name, "\""),
      do: String.slice(name, 1, String.length(name) - 2),
      else: name
  end

  defp failure(code),
    do:
      {:error,
       Error.new(:configuration, "Cannot read or parse AWS shared configuration", code: code)}
end
