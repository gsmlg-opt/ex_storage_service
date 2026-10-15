defmodule ExStorageServiceCli.S3.Waiter do
  @moduledoc "Pinned S3 waiter acceptors, bounded attempts, and interruptible delays."
  alias ExStorageServiceCli.{Error, S3.OperationCatalog}

  def wait(name, input, fetch, opts \\ []) do
    case OperationCatalog.waiters()[name] do
      nil -> {:error, Error.new(:syntax, "Unknown S3 waiter")}
      model -> loop(model, name, input, fetch, opts, 1, nil)
    end
  end

  defp loop(model, name, input, fetch, opts, attempt, previous_acceptor) do
    result = fetch.(model["operation"], input)

    status =
      case result do
        {:ok, _data, %{status: status}} -> status
        {:error, %Error{http_status: status}} -> status
        _ -> nil
      end

    acceptor =
      Enum.find(model["acceptors"], &(&1["matcher"] == "status" and &1["expected"] == status))

    cond do
      acceptor && acceptor["state"] == "success" ->
        :ok

      acceptor && acceptor["state"] == "failure" ->
        failure(
          name,
          :waiter_failed,
          attempt,
          "Waiter encountered a terminal failure state: #{explanation(acceptor)}"
        )

      is_nil(acceptor) and match?({:error, %Error{category: :service}}, result) ->
        {:error, error} = result

        code =
          if error.code == "HTTP#{error.http_status}",
            do: to_string(error.http_status),
            else: error.code || "Unknown"

        failure(
          name,
          :waiter_failed,
          attempt,
          "An error occurred (#{code}): #{error.details[:service_message] || error.message}"
        )

      is_nil(acceptor) and match?({:error, _}, result) ->
        result

      attempt >= (opts[:max_attempts] || model["maxAttempts"]) ->
        last_acceptor = acceptor || previous_acceptor

        reason =
          if last_acceptor,
            do: "Max attempts exceeded. Previously accepted state: #{explanation(last_acceptor)}",
            else: "Max attempts exceeded"

        failure(name, :waiter_exhausted, attempt, reason)

      true ->
        delay = Keyword.get(opts, :delay, model["delay"] * 1000)

        with :ok <- (opts[:sleep] || (&sleep/1)).(delay),
             do: loop(model, name, input, fetch, opts, attempt + 1, acceptor || previous_acceptor)
    end
  end

  defp sleep(delay) do
    receive do
      {:ess_cancel, _signal} -> {:error, Error.new(:interrupted, "Waiter interrupted")}
    after
      delay -> :ok
    end
  end

  defp explanation(%{"matcher" => "status", "expected" => status}),
    do: "Matched expected HTTP status code: #{status}"

  defp failure(name, code, attempt, reason),
    do:
      {:error,
       Error.new(:waiter, "Waiter #{name} failed: #{reason}", code: code, attempt: attempt)}
end
