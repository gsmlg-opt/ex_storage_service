defmodule ExStorageServiceCli.QueryTest do
  use ExUnit.Case, async: false

  alias ExStorageServiceCli.{Error, Query}

  @fixtures Path.expand("../../fixtures/jmespath", __DIR__)
  @manifest JSON.decode!(File.read!(Path.join(@fixtures, "manifest.json")))

  for {file, hash} <- @manifest["files"] do
    @corpus_file file
    @hash hash

    test "pinned MIT corpus file #{@corpus_file} retains its source bytes" do
      bytes = File.read!(Path.join(@fixtures, @corpus_file))
      assert Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) == @hash
    end

    for {suite, suite_index} <-
          Path.join(@fixtures, file) |> File.read!() |> JSON.decode!() |> Enum.with_index(),
        {test_case, case_index} <- Enum.with_index(suite["cases"]) do
      @data suite["given"]
      @case test_case

      test "MIT corpus #{file} suite #{suite_index} case #{case_index}: #{test_case["expression"]}" do
        result = Query.search(@case["expression"], @data)

        if Map.has_key?(@case, "result") do
          assert {:ok, actual} = result
          assert actual == @case["result"]
        else
          assert {:error, %Error{code: code}} = result
          assert code == @case["error"]
        end
      end
    end
  end

  test "logical operators short circuit while literals are validated before execution" do
    assert {:ok, "yes"} = Query.search("'yes' || unknown()", %{})
    assert {:ok, false} = Query.search("`false` && unknown()", %{})
    assert {:error, %Error{code: "syntax"}} = Query.search("'yes' || `not json`", %{})
  end

  test "validation checks grammar and budgets without evaluating against invented data" do
    assert :ok = Query.validate("length(foo)")
    assert :ok = Query.validate("Contents[?Size > `0`].Key")
    assert {:error, %Error{code: "syntax"}} = Query.validate("foo[")
    assert {:error, %Error{code: "syntax"}} = Query.validate("'yes' || `bad json`")
    assert_budget(Query.validate(String.duplicate("a", 9), max_expression_bytes: 8))
    assert_budget(Query.validate("a.b.c", max_depth: 2))
  end

  test "expression references cannot masquerade as JSON values" do
    assert {:error, %Error{code: "invalid-type"}} = Query.search("map(`null`, `[1]`)", %{})
    assert {:error, %Error{code: "invalid-type"}} = Query.search("type(&foo)", %{})
    assert {:ok, [1, nil]} = Query.search("map(&foo, @)", [%{"foo" => 1}, %{}])
  end

  test "integer conversions and nested equality preserve exact numeric values" do
    assert {:ok, 9_007_199_254_740_993} = Query.search("to_number('9007199254740993')", %{})
    assert {:ok, nil} = Query.search("to_number('1trailing')", %{})
    assert {:ok, true} = Query.search("`1` == `1.0`", %{})
    assert {:ok, true} = Query.search("contains(`[1]`, `1.0`)", %{})
  end

  test "empty projections remain arrays and slice bounds clamp correctly" do
    assert {:ok, []} = Query.search("*", %{})
    assert {:ok, []} = Query.search("foo[*]", %{"foo" => []})
    assert {:ok, [nil]} = Query.search("to_array(`null`)", %{})
    assert {:ok, [0, 1, 2]} = Query.search("[-99999999999999999999:]", [0, 1, 2])
    assert {:ok, []} = Query.search("[:-99999999999999999999]", [0, 1, 2])
    assert {:ok, [2, 1, 0]} = Query.search("[99999999999999999999::-1]", [0, 1, 2])
    assert {:ok, []} = Query.search("[-99999999999999999999::-1]", [0, 1, 2])
  end

  test "Unicode functions count and reverse codepoints rather than grapheme clusters" do
    assert {:ok, 2} = Query.search("length(@)", "e\u0301")
    assert {:ok, "\u0301e"} = Query.search("reverse(@)", "e\u0301")
  end

  test "query identifiers, keys and function names never create user atoms" do
    name = "query_atom_probe_#{System.unique_integer([:positive])}"
    assert {:ok, nil} = Query.search(name, %{})
    assert {:ok, %{^name => nil}} = Query.search("{#{name}: @.#{name}}", %{})
    assert {:error, %Error{code: "unknown-function"}} = Query.search("#{name}()", %{})
    assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    assert {:error, %Error{code: "invalid-type"}} = Query.search("@", %{not_json: 1})
  end

  test "input, expression, depth and output budgets fail explicitly without truncation" do
    assert_budget(Query.search("@", String.duplicate("x", 20), max_bytes: 10))
    assert_budget(Query.search("@", [1, 2, 3], max_nodes: 3))
    assert_budget(Query.search("@", [[[[1]]]], max_depth: 2))
    assert_budget(Query.search(String.duplicate("a", 9), %{}, max_expression_bytes: 8))
    assert_budget(Query.search("[a, a, a]", %{"a" => "1234"}, max_bytes: 10))
    assert_budget(Query.search("a.b.c", %{}, max_depth: 2))
  end

  test "heap exhaustion and timeouts stop owned evaluation without crashing the caller" do
    expression = Enum.join(List.duplicate("[@,@]", 22), " | ")
    assert_budget(Query.search(expression, [1], max_heap_words: 32_768, timeout: 500))
    assert_budget(Query.search(expression, [1], timeout: 1))
    assert {:ok, 1} = Query.search("a", %{"a" => 1})
  end

  test "caller death stops its guardian and evaluator" do
    expression = Enum.join(List.duplicate("[@,@]", 22), " | ")

    owner =
      spawn(fn ->
        receive do
          :start -> Query.search(expression, [1], timeout: 1000)
        end
      end)

    :erlang.trace(owner, true, [:procs, :set_on_spawn])
    send(owner, :start)
    assert_receive {:trace, ^owner, :spawn, guardian, _}, 1000
    assert_receive {:trace, ^guardian, :spawn, worker, _}, 1000
    guardian_monitor = Process.monitor(guardian)
    worker_monitor = Process.monitor(worker)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^guardian_monitor, :process, ^guardian, _}, 1000
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, _}, 1000
  end

  test "failures exclude supplied expressions and object contents" do
    secret = "secret-value-do-not-log"
    assert {:error, error} = Query.search("#{secret}", %{"data" => secret})
    refute inspect(error) =~ secret
  end

  defp assert_budget(result) do
    assert {:error, %Error{code: "query-budget-exceeded"}} = result
  end
end
