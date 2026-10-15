# Run from the CLI application with:
# mix run --no-start test/fixtures/jmespath/validate_external.exs /path/to/jmespath.test/tests
# The official corpus is external; its benchmark-only cases have no expected result.
[directory] = System.argv()

results =
  for file <- Path.wildcard(Path.join(directory, "*.json")),
      suite <- JSON.decode!(File.read!(file)),
      test <- suite["cases"],
      not Map.has_key?(test, "bench") do
    result = ExStorageServiceCli.Query.search(test["expression"], suite["given"])

    passed =
      case result do
        {:ok, actual} -> Map.has_key?(test, "result") and actual == test["result"]
        {:error, error} -> error.code == test["error"]
      end

    {passed, {Path.basename(file), test["expression"], result}}
  end

failures = for {false, failure} <- results, do: failure
Enum.each(failures, &IO.inspect/1)
IO.puts("#{length(results)} semantic/error cases, #{length(failures)} failures")
System.halt(if failures == [], do: 0, else: 1)
