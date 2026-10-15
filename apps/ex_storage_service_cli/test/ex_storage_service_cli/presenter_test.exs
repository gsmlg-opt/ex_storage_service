defmodule ExStorageServiceCli.PresenterTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Error, Presenter, YAML}

  @fixtures JSON.decode!(
              File.read!(Path.expand("../fixtures/presentation/vectors.json", __DIR__))
            )
  for vector <- @fixtures["vectors"] do
    @vector vector
    test "pinned AWS CLI formatter #{@vector["name"]}" do
      options = %{
        output: @vector["output"],
        color: @vector["color"] || "off",
        terminal_width: @vector["terminal_width"] || 80,
        command_name: "ListObjectsV2",
        query: @vector["query"]
      }

      output =
        capture_io(fn ->
          result = Presenter.write(@vector["data"], options)

          if @vector["error"],
            do: assert(match?({:error, %Error{}}, result)),
            else: assert(result == :ok)
        end)

      assert output == (@vector["text"] || "")
    end
  end

  test "metadata is removed before queries in all structured formats" do
    for format <- ["json", "text", "yaml", "yaml-stream"] do
      output =
        capture_io(fn ->
          assert :ok =
                   Presenter.write(%{"ResponseMetadata" => %{"RequestId" => "private"}}, %{
                     output: format,
                     query: "ResponseMetadata"
                   })
        end)

      refute output =~ "private"
      assert output =~ if(format == "text", do: "None", else: "null")
    end
  end

  test "text and table escape untrusted controls including keys and bidi marks" do
    data = %{"a\e[31m" => "line\n\t\e[2J\u202etest"}

    for format <- ["text", "table"] do
      output =
        capture_io(fn ->
          assert :ok =
                   Presenter.write(data, %{
                     output: format,
                     color: "off",
                     command_name: "GetObject"
                   })
        end)

      refute output =~ "\e"
      refute output =~ "\u202e"
      assert output =~ "\\n\\t\\u001b[2J\\u202etest"
    end
  end

  test "machine formats preserve control characters through encoding" do
    data = %{"Key" => "line\n\t\e[2J\u202etest"}
    json = capture_io(fn -> assert :ok = Presenter.write(data, %{output: "json"}) end)
    assert JSON.decode!(json) == data
    yaml = capture_io(fn -> assert :ok = Presenter.write(data, %{output: "yaml"}) end)
    assert YAML.decode(yaml) == {:ok, data}
  end

  test "streamed pages query separately and include empty pages" do
    output =
      capture_io(fn ->
        assert :ok =
                 Presenter.write_stream_page(%{"Contents" => [%{"Key" => "a"}]}, %{
                   query: "Contents[].Key"
                 })

        assert :ok =
                 Presenter.write_stream_page(%{"Contents" => [%{"Key" => "b"}]}, %{
                   query: "Contents[].Key"
                 })

        assert :ok = Presenter.write_stream_page(%{}, %{})
      end)

    assert output == "- - a\n- - b\n- {}\n"
    assert YAML.decode(output) == {:ok, [["a"], ["b"], %{}]}
  end

  test "off emits no bytes and malformed queries are typed failures" do
    assert capture_io(fn -> assert :ok = Presenter.write(%{"Key" => "x"}, %{output: "off"}) end) ==
             ""

    assert capture_io(fn -> assert {:error, %Error{}} = Presenter.write(%{}, %{query: "["}) end) ==
             ""
  end

  test "unknown output and ragged tables fail before printing" do
    for {data, output} <- [{%{}, "csv"}, {[[1, 2], [3]], "table"}, {[%{}, 1], "text"}] do
      assert capture_io(fn ->
               assert {:error, %Error{}} = Presenter.write(data, %{output: output})
             end) == ""
    end
  end

  test "timestamps keep AWS ISO UTC representation" do
    output =
      capture_io(fn -> assert :ok = Presenter.write(%{"Date" => ~U[2024-01-02 03:04:05Z]}) end)

    assert JSON.decode!(output) == %{"Date" => "2024-01-02T03:04:05+00:00"}
  end
end
