defmodule ExStorageServiceCli.CLI.SignalsTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Error, ExitStatus}
  alias ExStorageServiceCli.CLI.Runtime
  alias ExStorageServiceCli.Transfer.{Coordinator, Source}
  @moduletag :tmp_dir

  test "runtime signal owner cancels work, permits abort, and reports its signal" do
    for {signal, category, status} <- [{:sigint, :interrupted, 130}, {:sigterm, :terminated, 143}] do
      result =
        Runtime.with_invocation(
          fn runtime ->
            # Exercise the same callback used by the platform adapter without sending a signal to ExUnit.
            send(runtime.signals, {:ess_signal, signal})

            await_cancel(runtime.coordinator)

            assert {:error, %Error{category: ^category}} =
                     Coordinator.acquire(runtime.coordinator, 1)

            assert {:ok, lease} = Coordinator.acquire(runtime.coordinator, 1, cleanup: true)
            assert :ok = Coordinator.release(lease)
            :ok
          end,
          handle_signals: true
        )

      assert {:error, %Error{category: ^category} = error} = result
      assert ExitStatus.code(error, :s3api) == status
    end
  end

  test "source hashing stops on cancellation and owned staging is still removable", %{
    tmp_dir: root
  } do
    path = Path.join(root, "source")
    File.write!(path, :binary.copy(<<0, 255>>, 100_000))
    {:ok, coordinator} = Coordinator.start_link([])
    {:ok, source} = Source.file(path, hash: false, coordinator: coordinator)
    assert :ok = Coordinator.cancel(coordinator)
    assert {:error, %Error{category: :interrupted}} = Source.hash(source)
    assert :ok = Source.cleanup(%{source | temporary: true})
    refute File.exists?(path)
  end

  test "library runtime leaves process signal ownership uninstalled" do
    assert :ok =
             Runtime.with_invocation(fn runtime ->
               assert runtime.signals == nil
               :ok
             end)
  end

  defp await_cancel(coordinator, attempts \\ 100)
  defp await_cancel(_coordinator, 0), do: flunk("signal handler did not cancel invocation")

  defp await_cancel(coordinator, attempts) do
    case Coordinator.check(coordinator) do
      :ok ->
        Process.sleep(1)
        await_cancel(coordinator, attempts - 1)

      {:error, _} ->
        :ok
    end
  end
end
