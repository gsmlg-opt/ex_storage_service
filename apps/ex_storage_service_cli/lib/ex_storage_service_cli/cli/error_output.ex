defmodule ExStorageServiceCli.CLI.ErrorOutput do
  @moduledoc "Frontend-specific stderr rendering of already-redacted failures."
  alias ExStorageServiceCli.{Error, Output}

  def write(%Error{} = error, :native), do: Output.error(error.message)
  def write(%Error{} = error, frontend), do: IO.puts(:stderr, message(error, frontend))

  def message(%Error{} = error, _frontend) do
    description =
      if error.operation && error.http_status do
        code =
          if error.code == "HTTP#{error.http_status}",
            do: to_string(error.http_status),
            else: to_string(error.code)

        retry =
          if error.details[:retry_limit_reached],
            do: " (reached max retries: #{max((error.attempt || 1) - 1, 0)})",
            else: ""

        "An error occurred (#{code}) when calling the #{error.operation} operation#{retry}: " <>
          (error.details[:service_message] || error.message)
      else
        error.message
      end

    (error.details[:high_level_prefix] || "") <> description
  end
end
