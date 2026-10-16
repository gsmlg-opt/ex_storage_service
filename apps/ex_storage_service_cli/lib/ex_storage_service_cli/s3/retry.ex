defmodule ExStorageServiceCli.S3.Retry do
  @moduledoc "Bounded, replay-aware request retry and overall-deadline policy."
  alias ExStorageServiceCli.{Error, S3.Retry.State}
  alias ExStorageServiceCli.Transfer.Coordinator
  @reads ~w(GetObject HeadObject GetObjectAttributes)
  @repeatable ~w(UploadPart ListParts AbortMultipartUpload)
  @transient ~w(RequestTimeout RequestTimeoutException PriorRequestNotComplete)
  @throttled ~w(Throttling ThrottlingException ThrottledException RequestThrottledException TooManyRequestsException ProvisionedThroughputExceededException TransactionInProgressException RequestLimitExceeded BandwidthLimitExceeded LimitExceededException RequestThrottled SlowDown PriorRequestNotComplete EC2ThrottledException)
  @denied ~w(AccessDenied AccessDeniedException InvalidAccessKeyId SignatureDoesNotMatch ExpiredToken InvalidToken TokenRefreshRequired)
  @uncertain [
    :request_timeout,
    :read_timeout,
    :read_idle_timeout,
    :write_idle_timeout,
    :timeout,
    :closed,
    :transport_failure
  ]

  @doc "Merges resolved profile/environment settings with explicit per-call options. Attempts include the first request."
  def options(settings, opts) do
    inherited =
      for key <- [:retry_mode, :max_attempts, :retry_state, :coordinator, :task_supervisor],
          value = settings[key],
          value != nil,
          do: {key, value}

    Keyword.merge(inherited, opts)
  end

  def run(operation, opts, request) when is_function(request, 2) or is_function(request, 3) do
    attempts = opts[:max_attempts] || 3
    budget = opts[:overall_timeout] || :infinity
    mode = opts[:retry_mode] || "standard"

    if is_integer(attempts) and attempts >= 1 and mode in ["standard", "adaptive"] and
         (budget == :infinity or (is_integer(budget) and budget > 0)) do
      clock = opts[:clock] || fn -> System.monotonic_time(:millisecond) end
      deadline = if budget == :infinity, do: :infinity, else: clock.() + budget

      opts =
        opts
        |> Keyword.put(:retry_mode, mode)
        |> Keyword.put_new(:cleanup, operation == "AbortMultipartUpload")

      with_state(opts, clock, fn opts ->
        attempt(operation, request, 1, attempts, deadline, clock, opts, nil)
      end)
    else
      {:error,
       Error.new(:configuration, "Retry mode, attempts or deadline are outside their bounds")}
    end
  end

  defp with_state(opts, clock, function) do
    if opts[:retry_state] do
      function.(opts)
    else
      {:ok, state} = State.start_link(owner: self(), now: clock.())

      try do
        function.(Keyword.put(opts, :retry_state, state))
      after
        if Process.alive?(state), do: GenServer.stop(state)
      end
    end
  end

  defp attempt(operation, request, number, maximum, deadline, clock, opts, acquired) do
    with :ok <- cancelled(opts),
         :ok <- permit(deadline, clock, opts),
         remaining when remaining != 0 <- remaining(deadline, clock) do
      result =
        if is_function(request, 3),
          do: request.(number, remaining, opts),
          else: request.(number, remaining)

      if opts[:retry_mode] == "adaptive",
        do: State.response(opts[:retry_state], throttled?(result), clock.())

      case result do
        {:error, %Error{} = error} ->
          error = %{
            error
            | attempt: number,
              details: Map.put(error.details, :retry_limit_reached, number >= maximum)
          }

          case disposition(operation, error, opts) do
            :indeterminate ->
              {:error,
               %{
                 error
                 | code: :outcome_indeterminate,
                   retryable: false,
                   message: "The operation has an unknown outcome and cannot be replayed safely"
               }}

            :retry when number < maximum ->
              cost =
                if error.code in [
                     :connect_timeout,
                     :request_timeout,
                     :read_timeout,
                     :read_idle_timeout,
                     :write_idle_timeout,
                     :timeout
                   ],
                   do: 10,
                   else: 5

              if State.acquire(opts[:retry_state], cost) do
                with :ok <- wait(delay(number, opts), deadline, clock, opts) do
                  attempt(operation, request, number + 1, maximum, deadline, clock, opts, cost)
                end
              else
                {:error,
                 %{
                   error
                   | retryable: false,
                     details: Map.put(error.details, :retry_quota_reached, true)
                 }}
              end

            _ ->
              {:error, error}
          end

        result ->
          if success?(result),
            do: State.release(opts[:retry_state], acquired || 1)

          result
      end
    else
      0 -> deadline_error(number)
      {:error, _} = error -> error
    end
  end

  @doc "Unknown mutating-request outcomes are never ordinary transient retries."
  def disposition(operation, %Error{category: :transport, code: code}, opts) do
    cond do
      code in @uncertain and not repeatable?(operation) -> :indeterminate
      opts[:replayable] == false -> :stop
      code in [:econnrefused, :connect_timeout, :nxdomain] -> :retry
      repeatable?(operation) and code in @uncertain -> :retry
      true -> :stop
    end
  end

  def disposition(operation, %Error{category: :service} = error, opts) do
    transient =
      error.code in (@transient ++ @throttled) or error.http_status in [500, 502, 503, 504]

    if opts[:replayable] != false and repeatable?(operation) and error.code not in @denied and
         transient,
       do: :retry,
       else: :stop
  end

  def disposition(_operation, _error, _opts), do: :stop

  defp repeatable?(operation),
    do:
      operation in (@reads ++ @repeatable) or
        String.starts_with?(operation, ["List", "Get", "Head"])

  defp success?({:ok, _}), do: true
  defp success?({:ok, _, _}), do: true
  defp success?(_), do: false
  defp throttled?({:error, %Error{category: :service, code: code}}), do: code in @throttled
  defp throttled?(_), do: false
  defp remaining(:infinity, _clock), do: :infinity
  defp remaining(deadline, clock), do: max(deadline - clock.(), 0)

  defp delay(number, opts) do
    # The reference caps after randomization, rather than randomizing a capped range.
    range = (opts[:base_delay] || 1000) * Integer.pow(2, number - 1)

    min(
      (opts[:jitter] || fn cap -> :rand.uniform(cap + 1) - 1 end).(range),
      opts[:max_delay] || 20_000
    )
  end

  defp permit(deadline, clock, opts) do
    with :ok <- cancelled(opts) do
      if opts[:retry_mode] == "adaptive" do
        case State.token(opts[:retry_state], clock.()) do
          :ok ->
            :ok

          {:wait, delay} ->
            with :ok <- wait(delay, deadline, clock, opts), do: permit(deadline, clock, opts)
        end
      else
        :ok
      end
    end
  end

  defp wait(delay, deadline, clock, opts) do
    with :ok <- cancelled(opts) do
      cond do
        deadline != :infinity and delay >= remaining(deadline, clock) -> deadline_error(nil)
        opts[:sleep] -> with :ok <- opts[:sleep].(delay), do: cancelled(opts)
        true -> sleep(delay, opts)
      end
    end
  end

  defp deadline_error(number),
    do:
      {:error,
       Error.new(:transport, "Operation exceeded its overall deadline",
         code: :operation_deadline,
         attempt: number
       )}

  defp cancelled(opts) do
    if opts[:cleanup] do
      :ok
    else
      with :ok <- Coordinator.check(opts[:coordinator]), do: mailbox_cancelled()
    end
  catch
    :exit, _ -> {:error, Error.new(:interrupted, "Invocation ownership ended")}
  end

  defp mailbox_cancelled() do
    receive do
      {:ess_cancel, _signal} -> {:error, Error.new(:interrupted, "Operation interrupted")}
    after
      0 -> :ok
    end
  end

  defp sleep(milliseconds, opts) do
    with :ok <- cancelled(opts) do
      interval = min(milliseconds, 50)
      cleanup = opts[:cleanup] == true

      receive do
        {:ess_cancel, _signal} when not cleanup ->
          {:error, Error.new(:interrupted, "Operation interrupted")}
      after
        interval ->
          if milliseconds > interval,
            do: sleep(milliseconds - interval, opts),
            else: cancelled(opts)
      end
    end
  end
end
