defmodule ExStorageServiceCli.S3.RetryTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, S3.Retry}
  alias ExStorageServiceCli.S3.Retry.State
  alias ExStorageServiceCli.Transfer.Coordinator

  test "validated region cache is bounded and isolates endpoint, addressing, region and bucket" do
    {:ok, state} = State.start_link()
    key = {"https://storage.example", :path, "us-east-1", "bucket"}
    assert :error = State.region(state, key)
    assert :ok = State.put_region(state, key, "us-west-2")
    assert {:ok, "us-west-2"} = State.region(state, key)
    assert :error = State.region(state, {"https://other.example", :path, "us-east-1", "bucket"})

    assert :error =
             State.region(state, {"https://storage.example", :virtual, "us-east-1", "bucket"})

    assert :error = State.region(state, {"https://storage.example", :path, "eu-west-1", "bucket"})

    assert {:error, :invalid_region_cache_entry} =
             State.put_region(state, key, "us-west-2\r\nInjected")

    assert {:error, :invalid_region_cache_entry} =
             State.put_region(
               state,
               {"https://secret@storage.example", :path, "us-east-1", "bucket"},
               "us-west-2"
             )

    for number <- 1..128 do
      assert :ok =
               State.put_region(state, {nil, :path, "us-east-1", "bucket-#{number}"}, "us-west-2")
    end

    assert :error = State.region(state, key)
    assert map_size(State.snapshot(state).regions) == 128
  end

  test "retry counts match the captured pinned executable wire cases" do
    fixtures =
      File.read!(Path.expand("../../fixtures/s3/retry-wire.json", __DIR__)) |> JSON.decode!()

    for fixture <- fixtures, fixture["case"] != "custom-endpoint-region" do
      environment = fixture["environment"]
      opts = [retry_mode: environment["AWS_RETRY_MODE"] || "standard", sleep: fn _ -> :ok end]

      opts =
        if environment["AWS_MAX_ATTEMPTS"],
          do:
            Keyword.put(opts, :max_attempts, String.to_integer(environment["AWS_MAX_ATTEMPTS"])),
          else: opts

      owner = self()

      assert {:error, %Error{}} =
               Retry.run("ListBuckets", opts, fn _, _ ->
                 send(owner, :wire_request)

                 {:error,
                  Error.new(:service, "fixture",
                    code: fixture["code"],
                    http_status: fixture["status"]
                  )}
               end)

      for _ <- fixture["requests"], do: assert_receive(:wire_request)
      refute_receive :wire_request
    end
  end

  test "worker backoff observes invocation cancellation without receiving a signal itself" do
    {:ok, coordinator} = Coordinator.start_link()
    owner = self()

    worker =
      Task.async(fn ->
        Retry.run("GetObject", [coordinator: coordinator, jitter: fn _ -> 5000 end], fn _, _ ->
          send(owner, :requested)
          {:error, Error.new(:service, "slow", code: "SlowDown")}
        end)
      end)

    assert_receive :requested
    :ok = Coordinator.cancel(coordinator)
    assert {:error, %Error{category: :interrupted}} = Task.await(worker, 500)
    refute_receive :requested

    assert {:ok, :aborted} =
             Retry.run("AbortMultipartUpload", [coordinator: coordinator], fn _, _ ->
               {:ok, :aborted}
             end)
  end

  test "pinned standard defaults are three total attempts and resolved settings are effective" do
    owner = self()
    opts = Retry.options(%{retry_mode: "standard", max_attempts: 4}, sleep: fn _ -> :ok end)

    assert {:error, %Error{attempt: 4}} =
             Retry.run("HeadBucket", opts, fn number, _ ->
               send(owner, {:number, number})
               {:error, Error.new(:service, "slow", code: "SlowDown")}
             end)

    for number <- 1..4, do: assert_receive({:number, ^number})

    assert {:error, %Error{attempt: 3}} =
             Retry.run("HeadBucket", [sleep: fn _ -> :ok end], fn _, _ ->
               {:error, Error.new(:service, "slow", code: "SlowDown")}
             end)

    assert [max_attempts: 1] = Retry.options(%{max_attempts: 4}, max_attempts: 1)
  end

  test "pinned standard classifier excludes bare 429 and authorization failures" do
    assert :retry =
             Retry.disposition(
               "GetObject",
               Error.new(:service, "pending", code: "PriorRequestNotComplete"),
               []
             )

    assert :stop =
             Retry.disposition("GetObject", Error.new(:service, "rate", http_status: 429), [])

    assert :stop =
             Retry.disposition(
               "GetObject",
               Error.new(:service, "denied", code: "AccessDenied", http_status: 500),
               []
             )

    for mode <- ["unknown", "legacy"] do
      assert {:error, %Error{category: :configuration}} =
               Retry.run("GetObject", [retry_mode: mode], fn _, _ ->
                 flunk("invalid mode sent a request")
               end)
    end
  end

  test "quota is shared across requests and success replenishes only the final retry cost" do
    {:ok, state} = State.start_link(quota: 5)
    opts = [retry_state: state, sleep: fn _ -> :ok end]
    error = {:error, Error.new(:service, "slow", code: "SlowDown")}

    assert {:error, %Error{attempt: 2, details: %{retry_quota_reached: true}}} =
             Retry.run("GetObject", opts, fn _, _ -> error end)

    assert State.snapshot(state).quota == 0
    assert {:ok, :done} = Retry.run("GetObject", opts, fn _, _ -> {:ok, :done} end)
    assert State.snapshot(state).quota == 1
  end

  test "backoff caps after jitter and timeout retry consumes ten quota tokens" do
    owner = self()
    {:ok, state} = State.start_link(quota: 20)

    assert {:ok, :done} =
             Retry.run(
               "GetObject",
               [
                 max_attempts: 2,
                 retry_state: state,
                 jitter: fn range ->
                   send(owner, {:range, range})
                   range
                 end,
                 sleep: fn delay ->
                   send(owner, {:delay, delay})
                   :ok
                 end
               ],
               fn
                 1, _ ->
                   {:error, Error.new(:transport, "read", code: :read_timeout)}

                 2, _ ->
                   assert State.snapshot(state).quota == 10
                   {:ok, :done}
               end
             )

    assert_receive {:range, 1000}
    assert_receive {:delay, 1000}
    assert State.snapshot(state).quota == 20
  end

  test "cancellation is checked before sending and during adaptive waiting" do
    send(self(), {:ess_cancel, :sigint})

    assert {:error, %Error{category: :interrupted}} =
             Retry.run("GetObject", [], fn _, _ -> flunk("cancelled") end)

    {:ok, state} = State.start_link(now: 0)
    :ok = State.response(state, true, 0)

    assert {:error, %Error{code: :operation_deadline}} =
             Retry.run(
               "GetObject",
               [
                 retry_mode: "adaptive",
                 retry_state: state,
                 clock: fn -> 0 end,
                 overall_timeout: 10
               ],
               fn _, _ -> flunk("rate limited") end
             )

    caller = self()

    spawn(fn ->
      Process.sleep(10)
      send(caller, {:ess_cancel, :sigterm})
    end)

    assert {:error, %Error{category: :interrupted}} =
             Retry.run("GetObject", [retry_mode: "adaptive", retry_state: state], fn _, _ ->
               flunk("rate limited")
             end)
  end

  test "adaptive bucket enables only after throttling and follows pinned cubic rate" do
    {:ok, state} = State.start_link(now: 0)
    assert :ok = State.token(state, 0)
    :ok = State.response(state, true, 500)
    snapshot = State.snapshot(state)
    assert snapshot.enabled
    assert_in_delta snapshot.measured, 1.6, 0.00001
    assert_in_delta snapshot.rate, 1.12, 0.00001
    assert {:wait, 447} = State.token(state, 500)
    assert :ok = State.token(state, 947)
    :ok = State.response(state, false, 1500)
    assert State.snapshot(state).rate > snapshot.rate
  end

  test "shared retry state exits when its invocation owner exits" do
    caller = self()

    owner =
      spawn(fn ->
        {:ok, state} = State.start_link()
        send(caller, {:state, state})

        receive do
          :finish -> :ok
        end
      end)

    assert_receive {:state, state}, 2_000
    ref = Process.monitor(state)
    send(owner, :finish)
    assert_receive {:DOWN, ^ref, :process, ^state, :normal}
  end

  test "reads retry transient failures within the attempt bound" do
    owner = self()

    request = fn attempt, _remaining ->
      send(owner, {:attempt, attempt})

      if attempt < 3,
        do: {:error, Error.new(:service, "slow", code: "SlowDown")},
        else: {:ok, :done}
    end

    assert {:ok, :done} =
             Retry.run("GetObject", [max_attempts: 3, sleep: fn _ -> :ok end], request)

    for number <- 1..3, do: assert_receive({:attempt, ^number})
    refute_receive {:attempt, _}
  end

  test "nonreplayable and potentially version-creating writes are not repeated" do
    owner = self()

    request = fn _, _ ->
      send(owner, :called)
      {:error, Error.new(:transport, "lost", code: :closed)}
    end

    assert {:error, %Error{code: :outcome_indeterminate}} =
             Retry.run("PutObject", [max_attempts: 3], request)

    assert_receive :called
    refute_receive :called

    assert {:error, %Error{code: :closed}} =
             Retry.run("UploadPart", [max_attempts: 3, replayable: false], request)

    assert_receive :called
    refute_receive :called
  end

  test "authentication errors stop and overall deadline bounds backoff" do
    owner = self()

    request = fn _, _ ->
      send(owner, :called)
      {:error, Error.new(:service, "denied", code: "AccessDenied", http_status: 403)}
    end

    assert {:error, %Error{code: "AccessDenied"}} =
             Retry.run("HeadObject", [max_attempts: 3], request)

    assert_receive :called
    refute_receive :called

    assert {:error, %Error{code: :operation_deadline}} =
             Retry.run(
               "HeadObject",
               [max_attempts: 3, overall_timeout: 10, jitter: fn _ -> 100 end],
               fn _, _ -> {:error, Error.new(:service, "slow", code: "SlowDown")} end
             )
  end
end
