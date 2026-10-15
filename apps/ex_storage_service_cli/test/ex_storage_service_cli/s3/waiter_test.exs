defmodule ExStorageServiceCli.S3.WaiterTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, S3.OperationCatalog, S3.Waiter}

  test "every pinned success acceptor works, including forbidden and redirected buckets" do
    for {name, model} <- OperationCatalog.waiters(),
        acceptor <- model["acceptors"],
        acceptor["state"] == "success" do
      expected_operation = model["operation"]

      fetch = fn operation, _ ->
        assert operation == expected_operation

        if acceptor["expected"] == 200,
          do: {:ok, %{}, %{status: 200}},
          else: {:error, Error.new(:service, "status", http_status: acceptor["expected"])}
      end

      assert :ok = Waiter.wait(name, %{}, fetch)
    end
  end

  test "retry acceptors exhaust at the exact attempt count and authorization failures stop" do
    owner = self()

    fetch = fn _, _ ->
      send(owner, :attempt)
      {:error, Error.new(:service, "absent", http_status: 404)}
    end

    assert {:error, %Error{code: :waiter_exhausted, attempt: 3, message: message}} =
             Waiter.wait("ObjectExists", %{}, fetch, max_attempts: 3, delay: 0)

    assert message ==
             "Waiter ObjectExists failed: Max attempts exceeded. Previously accepted state: " <>
               "Matched expected HTTP status code: 404"

    for _ <- 1..3, do: assert_receive(:attempt)
    refute_receive :attempt

    assert {:error, %Error{code: :waiter_failed, attempt: 1}} =
             Waiter.wait("ObjectExists", %{}, fn _, _ ->
               {:error, Error.new(:service, "denied", http_status: 403)}
             end)
  end

  test "unmatched success exhausts without an acceptor and transport errors keep their type" do
    assert {:error,
            %Error{
              code: :waiter_exhausted,
              message: "Waiter BucketNotExists failed: Max attempts exceeded"
            }} =
             Waiter.wait("BucketNotExists", %{}, fn _, _ -> {:ok, %{}, %{status: 200}} end,
               max_attempts: 1
             )

    error = Error.new(:transport, "Cannot connect", code: :connection_refused)

    assert {:error, ^error} =
             Waiter.wait("ObjectExists", %{}, fn _, _ -> {:error, error} end)
  end

  test "cancellation during delay stops additional network calls" do
    owner = self()

    fetch = fn _, _ ->
      send(owner, :attempt)
      {:error, Error.new(:service, "absent", http_status: 404)}
    end

    send(self(), {:ess_cancel, :sigint})
    assert {:error, %Error{category: :interrupted}} = Waiter.wait("BucketExists", %{}, fetch)
    assert_receive :attempt
    refute_receive :attempt
  end
end
