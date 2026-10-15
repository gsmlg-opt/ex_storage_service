defmodule ExStorageServiceCli.YAMLTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Error, YAML}

  @vectors Path.expand("../../fixtures/yaml/vectors.json", __DIR__)
           |> File.read!()
           |> JSON.decode!()

  for vector <- @vectors["input"] do
    @vector vector

    test "pinned AWS CLI YAML oracle #{@vector["name"]}" do
      result = YAML.decode(@vector["text"])

      if @vector["error"] do
        assert {:error, %Error{code: "invalid-yaml"}} = result
      else
        assert {:ok, value} = result
        assert normalized(value) == @vector["value"]
      end
    end
  end

  for vector <- @vectors["output"] do
    @vector vector

    test "pinned AWS CLI YAML output #{@vector["name"]}" do
      assert YAML.encode(@vector["value"]) == {:ok, @vector["text"]}
      assert YAML.decode(@vector["text"]) == {:ok, @vector["value"]}
    end
  end

  test "duplicate merges and recursive aliases fail without implicit coercion" do
    assert_invalid(YAML.decode("base: &base {x: 1}\nvalue: {<<: *base, <<: *base}\n"))
    assert_invalid(YAML.decode("a: &a [*a]\n"))
    assert_invalid(YAML.decode("value: {<<: 1}\n"))
    assert {:ok, %{"value" => %{"<<" => "literal"}}} = YAML.decode("value: {'<<': literal}\n")
    assert {:ok, %{"value" => "<<"}} = YAML.decode("value: <<\n")
  end

  test "unknown and Erlang extension tags never execute or create atoms" do
    name = "yaml_atom_probe_#{System.unique_integer([:positive])}"
    assert_invalid(YAML.decode("value: !<tag:yamerl,2012:atom> #{name}\n"))
    assert_invalid(YAML.decode("value: !<tag:yamerl,2012:fun> 'fun() -> halt() end.'\n"))
    assert {:ok, %{^name => ^name}} = YAML.decode("#{name}: #{name}\n")
    assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    refute Code.ensure_loaded?(:ess_cli_yaml_node_erlang_atom)
    refute Code.ensure_loaded?(:ess_cli_yaml_node_erlang_fun)
  end

  test "non-JSON keys and non-finite values cannot reach modeled requests" do
    assert_invalid(YAML.decode("1: value\n"))
    assert_invalid(YAML.decode("? [a, b]\n: value\n"))
    for value <- [".nan", ".inf", "-.inf"], do: assert_invalid(YAML.decode("value: #{value}\n"))
    assert_invalid(YAML.encode(%{not_json: 1}))
  end

  test "text, node, depth and output budgets fail without silent truncation" do
    assert_budget(YAML.decode("value: data\n", max_bytes: 5))
    assert_budget(YAML.decode("value: [1, 2, 3]\n", max_nodes: 3))
    assert_budget(YAML.decode("value: [[[1]]]\n", max_depth: 2))
    assert_budget(YAML.encode(%{"value" => String.duplicate("x", 10)}, max_bytes: 5))
    assert_budget(YAML.encode(%{"value" => "\0\0"}, max_bytes: 8))
  end

  test "alias counts and expanded values are bounded independently of source size" do
    aliases = Enum.join(List.duplicate("*base", 129), ", ")
    assert_budget(YAML.decode("base: &base [1]\nvalue: [#{aliases}]\n"))

    assert_budget(
      YAML.decode("base: &base [1,2,3,4,5]\nvalue: [*base,*base,*base]\n", max_nodes: 10)
    )

    assert {:ok, %{"a" => 1}} = YAML.decode("a: 1\n")
  end

  test "deep syntax is rejected by the constructor before reaching output normalization" do
    text = "value: " <> String.duplicate("[", 130) <> "1" <> String.duplicate("]", 130)
    assert_budget(YAML.decode(text))
  end

  test "control characters, multiline strings, Unicode and large keys roundtrip" do
    strings = [
      "\n",
      "\n\n",
      "first\n",
      "one\n\ntwo",
      "x \ny",
      "x\n y",
      "\t\n",
      "a'b\nc",
      "\x00\x01\u0085\u2028\u2029",
      "雪☃",
      "~",
      "true",
      "2024-01-01",
      "1_000",
      "1:2",
      "---",
      "..."
    ]

    for string <- strings do
      input = %{"value" => string}
      assert {:ok, output} = YAML.encode(input)
      assert YAML.decode(output) == {:ok, input}
    end

    key = String.duplicate("k", 1025)
    assert {:ok, text} = YAML.encode(%{key => [1, nil]})
    assert YAML.decode(text) == {:ok, %{key => [1, nil]}}
  end

  test "timeout and heap exhaustion preserve caller health and redact input" do
    text = "value: [" <> Enum.join(List.duplicate("1", 20_000), ",") <> "]"
    assert_budget(YAML.decode(text, max_heap_words: 32_768, timeout: 500))
    assert_budget(YAML.decode(text, timeout: 1))
    secret = "secret-yaml-value-do-not-log"
    assert {:error, error} = YAML.decode("value: !unsupported #{secret}\n")
    refute inspect(error) =~ secret
    assert {:ok, 1} = YAML.decode("1")
  end

  @tag timeout: 10_000
  test "cold trusted dependency loading does not consume the parsing deadline" do
    # Delay only a disposable VM's loader, keeping the test runner's code server
    # and all concurrent callers untouched. Scalar remains cold until decode.
    source = ~S"""
    for module <- [ExStorageServiceCli.YAML, ExStorageServiceCli.Error, System, IO, Process, Regex, Date, DateTime] do
      {:module, ^module} = :code.ensure_loaded(module)
    end
    for {name, _, _} <- :code.all_available(),
        String.starts_with?(List.to_string(name), "ess_cli_yaml_") do
      module = List.to_atom(name)
      {:module, ^module} = :code.ensure_loaded(module)
    end
    false = :code.is_loaded(ExStorageServiceCli.YAML.Scalar)
    owner = self()
    loader = Process.whereis(:code_server)
    true = :erlang.suspend_process(loader)
    try do
      spawn(fn ->
        send(owner, {:decoded, ExStorageServiceCli.YAML.decode("Bucket: bucket\nKey: key\n")})
      end)
      receive do after 1200 -> :ok end
    after
      true = :erlang.resume_process(loader)
    end
    receive do
      {:decoded, {:ok, %{"Bucket" => "bucket", "Key" => "key"}}} -> IO.puts("cold-load-ok")
      {:decoded, _} -> System.halt(1)
    after
      3000 -> System.halt(2)
    end
    """

    ebin = YAML |> :code.which() |> Path.dirname()
    executable = System.find_executable("elixir")
    assert executable

    port =
      Port.open({:spawn_executable, String.to_charlist(executable)}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: ["--erl", "+S 2:2", "-pa", ebin, "-e", source]
      ])

    try do
      assert {0, "cold-load-ok\n"} =
               child_result(port, "", System.monotonic_time(:millisecond) + 8000)
    after
      if Port.info(port), do: Port.close(port)
    end
  end

  defp child_result(port, output, deadline) do
    receive do
      {^port, {:data, bytes}} -> child_result(port, output <> bytes, deadline)
      {^port, {:exit_status, status}} -> {status, output}
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        flunk("Cold YAML subprocess exceeded its deadline")
    end
  end

  defp normalized({:yaml_date, value}), do: %{"__yaml_tag__" => "date", "value" => value}

  defp normalized({:yaml_timestamp, value}) do
    {:ok, date, _} = DateTime.from_iso8601(value)
    {microsecond, _} = date.microsecond
    date = %{date | microsecond: {microsecond, if(microsecond == 0, do: 0, else: 6)}}
    %{"__yaml_tag__" => "timestamp", "value" => DateTime.to_iso8601(date)}
  end

  defp normalized({:binary_file, value}),
    do: %{"__yaml_tag__" => "binary", "value" => Base.encode64(value)}

  defp normalized(map) when is_map(map),
    do: Map.new(map, fn {key, value} -> {key, normalized(value)} end)

  defp normalized(list) when is_list(list), do: Enum.map(list, &normalized/1)
  defp normalized(value), do: value

  defp assert_invalid(result), do: assert({:error, %Error{code: "invalid-yaml"}} = result)
  defp assert_budget(result), do: assert({:error, %Error{code: "yaml-budget-exceeded"}} = result)
end
