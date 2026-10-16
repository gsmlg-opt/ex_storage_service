defmodule ExStorageServiceCli.NativeDeadlineDurationTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{CLI.Parser, Error}
  @fixture Path.join(__DIR__, "fixtures/native-deadline-duration-go.json")

  test "both native deadline flags follow retained Go grammar within exact transport precision" do
    for record <- JSON.decode!(File.read!(@fixture))["cases"],
        {flag, field} <- [
          {"conn-read-deadline", :read_idle_timeout},
          {"conn-write-deadline", :write_idle_timeout}
        ] do
      result = Parser.parse(["ls", "--" <> flag, record["input"]])
      nanos = record["nanoseconds"]

      cond do
        record["valid_go_duration"] and nanos == 0 ->
          assert {:ok, invocation} = result, inspect(record)
          assert Map.fetch!(invocation.options, field) == nil

        record["valid_go_duration"] and nanos > 0 and rem(nanos, 1_000_000) == 0 and
            div(nanos, 1_000_000) <= 4_294_967_295 ->
          assert {:ok, invocation} = result, inspect(record)
          assert Map.fetch!(invocation.options, field) == div(nanos, 1_000_000)

        true ->
          assert {:error, %Error{category: :syntax}} = result, inspect(record)
      end
    end
  end

  test "invalid UTF-8 and oversized duration inputs fail before dispatch" do
    for value <- [<<255>>, String.duplicate("0", 257) <> "ms"] do
      assert {:error, %Error{category: :syntax}} =
               Parser.parse(["ls", "--conn-read-deadline", value])
    end
  end
end
