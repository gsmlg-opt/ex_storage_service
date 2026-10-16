Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeSQLTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{Invocation, NativeUtility, Parser, Runner}
  import ExUnit.CaptureIO
  @moduletag :tmp_dir

  defp target(key \\ "data.csv"), do: %Target{kind: :remote, bucket: "bucket", key: key}
  defp context(endpoint), do: %Context{endpoint: endpoint, unsigned: true}

  defp invocation(args \\ ["s3://bucket/data.csv"], options \\ %{}),
    do: %Invocation{frontend: :native, command: ["sql"], args: args, options: options}

  defp opts(root) do
    owner = self()

    [
      tmp_dir: root,
      max_attempts: 1,
      write: fn bytes ->
        send(owner, {:records, bytes})
        :ok
      end
    ]
  end

  defp event(type, payload) do
    headers =
      Enum.map_join([{":message-type", "event"}, {":event-type", type}], fn {key, value} ->
        <<byte_size(key), key::binary, 7, byte_size(value)::16, value::binary>>
      end)

    prelude = <<byte_size(headers) + byte_size(payload) + 16::32, byte_size(headers)::32>>
    bytes = prelude <> <<:erlang.crc32(prelude)::32>> <> headers <> payload
    bytes <> <<:erlang.crc32(bytes)::32>>
  end

  defp response(payload \\ "one\ntwo\n"),
    do:
      {200, [{"Content-Type", "application/vnd.amazon.eventstream"}],
       event("Records", payload) <> event("End", "")}

  defp listing(keys, truncated \\ false) do
    objects =
      Enum.map_join(keys, "", fn key ->
        "<Contents><Key>#{key}</Key><Size>3</Size><ETag>&quot;e&quot;</ETag></Contents>"
      end)

    "<ListBucketResult><IsTruncated>#{truncated}</IsTruncated>#{if truncated, do: "<NextContinuationToken>next</NextContinuationToken>", else: ""}#{objects}</ListBucketResult>"
  end

  test "native SQL owns query and serialization flags while AWS query remains JMESPath" do
    assert {:ok,
            %{
              options: %{
                sql_query: "SELECT * FROM s3object",
                recursive: true,
                csv_input: "fh=USE",
                compression: "GZIP",
                csv_output: "fd=;",
                csv_output_header: "name,value",
                limit: 2
              }
            }} =
             Parser.parse([
               "sql",
               "s3://bucket/prefix/",
               "--query",
               "SELECT * FROM s3object",
               "--recursive",
               "--csv-input",
               "fh=USE",
               "--compression",
               "GZIP",
               "--csv-output",
               "fd=;",
               "--csv-output-header",
               "name,value",
               "--limit",
               "2"
             ])

    assert {:ok, %{options: %{json_input: "type=lines", json_output: "rd=\\n"}}} =
             Parser.parse([
               "sql",
               "s3://bucket/data",
               "--json-input",
               "type=lines",
               "--json-output",
               "rd=\\n"
             ])

    assert {:ok, %{options: %{query: "Contents[].Key"}}} =
             Parser.parse(["s3", "ls", "s3://bucket", "--query", "Contents[].Key"])

    assert {:error, %Error{category: :syntax}} =
             Parser.parse(["cp", "a", "b", "--csv-input", "fh=USE"])
  end

  test "default CSV query sends actual Select XML and preserves split raw Records", %{
    tmp_dir: root
  } do
    bytes =
      event("Records", <<0, 255, 10>>) <>
        event("Cont", "") <> event("Records", "part\n") <> event("End", "")

    {endpoint, _} =
      WireServer.fixture([{200, [{"Content-Type", "application/vnd.amazon.eventstream"}], bytes}])

    assert {:ok,
            %{
              "QueriedObjects" => 1,
              "MatchedObjects" => 1,
              "RecordBytes" => 8,
              "Partial" => false
            }} =
             NativeUtility.run(invocation(), target(), context(endpoint), opts(root))

    assert_receive {:records, <<0, 255, 10>>}
    assert_receive {:records, "part\n"}
    assert_receive {:wire_request, %{method: "POST", target: path, body: body}}
    assert path == "/bucket/data.csv?select=&select-type=2"
    assert body =~ "<Expression>select * from s3object</Expression>"
    assert body =~ "<FileHeaderInfo>USE</FileHeaderInfo>"
    assert body =~ "<CompressionType>NONE</CompressionType>"
    assert body =~ "<FieldDelimiter>,</FieldDelimiter>"
    assert File.ls!(root) == []
  end

  test "CSV aliases long names booleans escapes and explicit compression reach the wire", %{
    tmp_dir: root
  } do
    {endpoint, _} = WireServer.fixture([response("value\n")])

    options = %{
      sql_query: "SELECT s.name FROM s3object s",
      csv_input: "fh=use,fd=;,rd=\\n,qc=\",qec=',cc=#,qrd=true",
      compression: "bzip2",
      csv_output: "FieldDelimiter=|,recordDelimiter=\\n\\n,QuoteChar=\",qec=',qf=always",
      csv_output_header: "name"
    }

    assert {:ok, %{"QueriedObjects" => 1}} =
             NativeUtility.run(
               invocation(["s3://bucket/data"], options),
               target("data"),
               context(endpoint),
               opts(root)
             )

    assert_receive {:records, "name\n"}
    assert_receive {:records, "value\n"}
    assert_receive {:wire_request, %{body: body}}
    assert body =~ "<Expression>SELECT s.name FROM s3object s</Expression>"
    assert body =~ "<CompressionType>BZIP2</CompressionType>"
    assert body =~ "<FileHeaderInfo>USE</FileHeaderInfo>"
    assert body =~ "<AllowQuotedRecordDelimiter>true</AllowQuotedRecordDelimiter>"
    assert body =~ "<QuoteFields>ALWAYS</QuoteFields>"
    assert body =~ "<QuoteEscapeCharacter>&apos;</QuoteEscapeCharacter>"
    assert body =~ "<FieldDelimiter>;</FieldDelimiter>"
    assert body =~ "<FieldDelimiter>|</FieldDelimiter>"
    assert body =~ "<RecordDelimiter>\n\n</RecordDelimiter>"
    refute_receive {:records, _}
  end

  test "explicit CSV header is emitted for an empty result and recursive keeps one object exact",
       %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], event("End", "")}])

    assert {:ok, %{"QueriedObjects" => 1, "RecordBytes" => 0}} =
             NativeUtility.run(
               invocation(["s3://bucket/data.csv"], %{recursive: true, csv_output_header: "name"}),
               target(),
               context(endpoint),
               opts(root)
             )

    assert_receive {:records, "name\n"}

    assert_receive {:wire_request,
                    %{method: "POST", target: "/bucket/data.csv?select=&select-type=2"}}

    refute_receive {:wire_request, _}
  end

  test "JSON serialization and inferred compression have real object effects", %{tmp_dir: root} do
    for {key, options, assertions} <- [
          {"data.json.gz", %{},
           ["<Type>LINES</Type>", "<CompressionType>GZIP</CompressionType>"]},
          {"data", %{json_input: "type=document", json_output: "rd=\\t", compression: "NONE"},
           ["<Type>DOCUMENT</Type>", "<RecordDelimiter>\t</RecordDelimiter>"]},
          {"data.csv.bz2", %{json_output: "rd=\\n"},
           ["<FileHeaderInfo>USE</FileHeaderInfo>", "<CompressionType>BZIP2</CompressionType>"]},
          {"data.parquet", %{}, ["<Parquet>", "<CompressionType>NONE</CompressionType>"]}
        ] do
      {endpoint, _} = WireServer.fixture([response("{}\n")])

      assert {:ok, %{"QueriedObjects" => 1}} =
               NativeUtility.run(
                 invocation(["s3://bucket/" <> key], options),
                 target(key),
                 context(endpoint),
                 opts(root)
               )

      assert_receive {:wire_request, %{method: "POST", body: body}}
      Enum.each(assertions, &assert(body =~ &1))
      assert_receive {:records, "{}\n"}
    end
  end

  test "legacy JSON documents retain atomic outfile and original Select summary fields", %{
    tmp_dir: root
  } do
    file = Path.join(root, "result")
    File.write!(file, "original")
    {endpoint, _} = WireServer.fixture([response("selected\n")])

    options = %{
      input_serialization: ~s({"CSV":{"FileHeaderInfo":"USE","FieldDelimiter":";"}}),
      output_serialization: ~s({"JSON":{"RecordDelimiter":"\\n"}})
    }

    assert {:ok, %{"QueriedObjects" => 1, record_bytes: 9, events: 2}} =
             NativeUtility.run(
               invocation(["s3://bucket/data.csv", "SELECT * FROM s3object", file], options),
               target(),
               context(endpoint),
               opts(root)
             )

    assert File.read!(file) == "selected\n"
    assert_receive {:wire_request, %{body: body}}
    assert body =~ "<JSON>"
    refute_receive {:records, _}
    assert File.ls!(root) == ["result"]
  end

  test "incomplete Select preserves legacy outfile and removes staging", %{tmp_dir: root} do
    file = Path.join(root, "result")
    File.write!(file, "original")
    {endpoint, _} = WireServer.fixture([{200, [], event("Records", "partial")}])

    assert {:error, %Error{code: :missing_select_end}} =
             NativeUtility.run(
               invocation(["s3://bucket/data.csv", "select * from s3object", file]),
               target(),
               context(endpoint),
               opts(root)
             )

    assert File.read!(file) == "original"
    assert File.ls!(root) == ["result"]
  end

  test "recursive SQL completes inventory before queries and emits one header with partial counts",
       %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing(["prefix/b.csv", "prefix/a.csv", "prefix/sub/c.csv"])},
        response("a\n"),
        response("b\n")
      ])

    options = %{recursive: true, limit: 2, csv_output_header: "name"}

    assert {:ok,
            %{"MatchedObjects" => 3, "QueriedObjects" => 2, "Partial" => true, "RecordBytes" => 4}} =
             NativeUtility.run(
               invocation(["s3://bucket/prefix/"], options),
               target("prefix/"),
               context(endpoint),
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET"}}

    assert_receive {:wire_request,
                    %{method: "POST", target: "/bucket/prefix/a.csv?select=&select-type=2"}}

    assert_receive {:wire_request,
                    %{method: "POST", target: "/bucket/prefix/b.csv?select=&select-type=2"}}

    assert_receive {:records, "name\n"}
    assert_receive {:records, "a\n"}
    assert_receive {:records, "b\n"}
    refute_receive {:records, _}
    refute_receive {:wire_request, _}
    assert File.ls!(root) == []
  end

  test "nonrecursive bucket selection skips descendants and zero limit is explicitly partial", %{
    tmp_dir: root
  } do
    for {limit, responses, queried} <- [{1000, [response()], 1}, {0, [], 0}] do
      {endpoint, _} = WireServer.fixture([{200, [], listing(["a.csv", "sub/b.csv"])} | responses])

      assert {:ok, %{"MatchedObjects" => 1, "QueriedObjects" => ^queried, "Partial" => partial}} =
               NativeUtility.run(
                 invocation(["s3://bucket/"], %{limit: limit}),
                 target(""),
                 context(endpoint),
                 opts(root)
               )

      assert partial == (limit == 0)
      assert_receive {:wire_request, %{method: "GET"}}

      if queried == 1 do
        assert_receive {:wire_request,
                        %{method: "POST", target: "/bucket/a.csv?select=&select-type=2"}}

        assert_receive {:records, _}
      end

      refute_receive {:wire_request, _}
    end
  end

  test "later inventory failure prevents Select and all stdout output", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing(["prefix/a.csv"], true)},
        {403, [], "<Error><Code>AccessDenied</Code></Error>"}
      ])

    assert {:error, %Error{code: "AccessDenied"}} =
             NativeUtility.run(
               invocation(["s3://bucket/prefix/"], %{recursive: true}),
               target("prefix/"),
               context(endpoint),
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, _}
    refute_receive {:records, _}
    assert File.ls!(root) == []
  end

  test "a later incompatible format prevents every recursive Select", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], listing(["prefix/a.csv", "prefix/z.parquet"])}])

    assert {:error, %Error{code: :sql_options}} =
             NativeUtility.run(
               invocation(["s3://bucket/prefix/"], %{recursive: true, csv_input: "fh=USE"}),
               target("prefix/"),
               context(endpoint),
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, _}
    refute_receive {:records, _}
    assert File.ls!(root) == []
  end

  test "raw stdout failures identify emitted bytes even in the first object", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], event("Records", "partial")}])

    assert {:error,
            %Error{
              code: :missing_select_end,
              details: %{partial_output: true, output_bytes: 7, queried_objects: 0}
            }} = NativeUtility.run(invocation(), target(), context(endpoint), opts(root))

    assert_receive {:records, "partial"}
  end

  test "malformed conflicting and unsupported forms fail before all network requests", %{
    tmp_dir: root
  } do
    cases = [
      %{csv_input: "fh=USE", json_input: "type=LINES"},
      %{csv_output: "", json_output: ""},
      %{input_serialization: "{}"},
      %{input_serialization: ~s({"CSV":null})},
      %{input_serialization: ~s({"CSV":{}}), csv_input: "fh=USE"},
      %{csv_input: "no-key"},
      %{csv_input: "unknown=x"},
      %{csv_input: "fh=USE,FileHeader=IGNORE"},
      %{csv_input: "qrd=;"},
      %{csv_input: "fd=long"},
      %{json_input: "type=bad"},
      %{compression: "ZIP"},
      %{csv_output_header: ""},
      %{csv_output_header: "a\nb"},
      %{csv_output_header: "a", json_output: "rd=\\n"},
      %{limit: -1},
      %{limit: 10001},
      %{sql_query: ""},
      %{output: "json"}
    ]

    for options <- cases do
      assert {:error, %Error{category: :syntax}} =
               NativeUtility.run(
                 invocation(["s3://bucket/data.csv"], options),
                 target(),
                 %Context{},
                 opts(root)
               )
    end

    assert {:error, %Error{code: :sql_options}} =
             NativeUtility.run(invocation(["one", "two"]), target(), %Context{}, opts(root))

    assert {:error, %Error{code: :sql_options}} =
             NativeUtility.run(
               invocation(["one", "two", "three"], %{sql_query: "select 1"}),
               target(),
               %Context{},
               opts(root)
             )

    refute_receive {:wire_request, _}
    assert File.ls!(root) == []
  end

  test "provider capability exclusion happens before inventory or output", %{tmp_dir: root} do
    assert {:error, %Error{code: :provider_unsupported}} =
             NativeUtility.run(
               invocation(["s3://bucket/"], %{recursive: true}),
               target(""),
               %Context{capabilities: %{provider: "ess"}},
               opts(root)
             )

    refute_receive {:wire_request, _}
    assert File.ls!(root) == []
  end

  test "Runner streams query Records without rendering a summary or applying JMESPath", %{
    tmp_dir: root
  } do
    {endpoint, _} = WireServer.fixture([response("raw\n")])

    output =
      capture_io(fn ->
        assert :ok =
                 Runner.run([
                   "sql",
                   "s3://bucket/data.csv",
                   "--query",
                   "select s.name from s3object s",
                   "--endpoint",
                   endpoint,
                   "--no-sign-request",
                   "--service",
                   "s3",
                   "--config-dir",
                   root
                 ])
      end)

    assert output == "raw\n"
    assert_receive {:wire_request, %{body: body}}
    assert body =~ "<Expression>select s.name from s3object s</Expression>"
  end

  test "Runner reports truncated object selection on stderr without corrupting Records", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([{200, [], listing(["a.csv", "b.csv"])}, response("raw\n")])

    owner = self()

    error =
      capture_io(:stderr, fn ->
        output =
          capture_io(fn ->
            assert :ok =
                     Runner.run([
                       "sql",
                       "s3://bucket/",
                       "--recursive",
                       "--limit",
                       "1",
                       "--endpoint",
                       endpoint,
                       "--no-sign-request",
                       "--service",
                       "s3",
                       "--config-dir",
                       root
                     ])
          end)

        send(owner, {:stdout, output})
      end)

    assert_receive {:stdout, "raw\n"}
    assert error =~ "SQL output is partial: queried 1 of 2 matching objects"
  end
end
