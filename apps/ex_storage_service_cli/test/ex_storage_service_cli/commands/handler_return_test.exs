Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Commands.HandlerReturnTest do
  # stderr capture replaces a VM-wide named IO device.
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Commands, Error}
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @context %{
    endpoint: "http://127.0.0.1:1",
    access_key_id: nil,
    secret_access_key: nil,
    unsigned: true,
    region: "us-east-1",
    json: true,
    profile: "default"
  }
  @objects "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>prefix/a</Key><Size>1</Size></Contents><Contents><Key>prefix/b</Key><Size>1</Size></Contents></ListBucketResult>"
  @denied "<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>"

  test "a handler failure returns to a library caller instead of terminating its VM" do
    paths =
      :code.get_path()
      |> Enum.map(&List.to_string/1)
      |> Enum.filter(&String.contains?(&1, "_build/test/lib/"))

    source =
      "Application.ensure_all_started(:http_fetch); result = ExStorageServiceCli.Commands.Cp.run(\"cp\", [\"local\", \"other\"], %{endpoint: \"http://127.0.0.1:1\", json: false}); IO.inspect(result, label: \"HANDLER_RETURNED\", width: :infinity)"

    {output, status} =
      System.cmd("elixir", Enum.flat_map(paths, &["-pa", &1]) ++ ["-e", source],
        stderr_to_stdout: true,
        env: [{"ERL_FLAGS", "+S 2:2"}]
      )

    assert status == 0, output
    assert output =~ "HANDLER_RETURNED: {:error, %ExStorageServiceCli.Error{"
  end

  test "invalid arguments short circuit with typed errors without networking", %{tmp_dir: dir} do
    for {module, command, args} <- [
          {Commands.Bucket, "mb", []},
          {Commands.Bucket, "rb", []},
          {Commands.Ls, "ls", ["one", "two"]},
          {Commands.Tree, "tree", []},
          {Commands.Cp, "cp", []},
          {Commands.Cp, "cp", [Path.join(dir, "missing"), "s3://bucket/key"]},
          {Commands.Cp, "cp", [dir, "s3://bucket/key"]},
          {Commands.Cp, "cp", ["--recursive", Path.join(dir, "missing"), "s3://bucket/key"]},
          {Commands.Rm, "rm", ["s3://bucket"]},
          {Commands.Mv, "mv", ["local", "s3://bucket/key"]},
          {Commands.Mv, "mv", ["s3://bucket", "s3://bucket/key"]},
          {Commands.Presign, "presign", ["s3://bucket/key"]}
        ] do
      capture_io(:stderr, fn ->
        capture_io(fn -> assert {:error, %Error{}} = module.run(command, args, @context) end)
      end)
    end

    signed = Map.merge(@context, %{access_key_id: "key", secret_access_key: "secret"})

    for args <- [["s3://bucket"], ["s3://bucket/key", "--method", "INVALID"]] do
      capture_io(:stderr, fn ->
        assert {:error, %Error{}} = Commands.Presign.run("presign", args, signed)
      end)
    end
  end

  test "recursive uploads stop at first service failure", %{tmp_dir: dir} do
    File.write!(Path.join(dir, "a"), "a")
    File.write!(Path.join(dir, "b"), "b")
    {endpoint, _server} = stop_fixture([])

    capture_io(:stderr, fn ->
      assert {:error, %Error{}} =
               Commands.Cp.run(
                 "cp",
                 ["--recursive", dir, "s3://bucket/prefix"],
                 Map.put(@context, :endpoint, endpoint)
               )
    end)

    assert_receive {:wire_request, %{method: "PUT"}}
    assert_receive :no_more_requests, 1_000
  end

  test "recursive downloads and server copies stop at first failure", %{tmp_dir: dir} do
    for destination <- [dir, "s3://other/prefix"] do
      {endpoint, _server} = stop_fixture([{200, [], @objects}])

      capture_io(:stderr, fn ->
        assert {:error, %Error{}} =
                 Commands.Cp.run(
                   "cp",
                   ["--recursive", "s3://bucket/prefix/", destination],
                   Map.put(@context, :endpoint, endpoint)
                 )
      end)

      assert_receive {:wire_request, %{target: target}}
      assert target =~ "list-type=2"
      assert_receive {:wire_request, %{method: method}}
      assert method == if(String.starts_with?(destination, "s3://"), do: "PUT", else: "GET")
      assert_receive :no_more_requests, 1_000
    end

    refute File.exists?(Path.join(dir, "b"))
  end

  test "move copy failure never deletes source and rb force returns failed delete" do
    for {module, command, args, method} <- [
          {Commands.Mv, "mv", ["s3://bucket/source", "s3://other/destination"], "PUT"},
          {Commands.Bucket, "rb", ["--force", "bucket"], "DELETE"}
        ] do
      {endpoint, _server} = stop_fixture([])

      capture_io(:stderr, fn ->
        assert {:error, %Error{}} =
                 module.run(command, args, Map.put(@context, :endpoint, endpoint))
      end)

      assert_receive {:wire_request, %{method: ^method}}
      assert_receive :no_more_requests, 1_000
    end
  end

  test "service errors return from remaining legacy handlers" do
    for {module, command, args} <- [
          {Commands.Bucket, "mb", ["bucket"]},
          {Commands.Ls, "ls", []},
          {Commands.Tree, "tree", ["bucket"]},
          {Commands.Rm, "rm", ["s3://bucket/key"]},
          {Commands.Info, "info", []}
        ] do
      {endpoint, _server} = WireServer.fixture([{403, [], @denied}])

      capture_io(:stderr, fn ->
        capture_io(fn ->
          assert {:error, %Error{details: %{reported: true}}} =
                   module.run(command, args, Map.put(@context, :endpoint, endpoint))
        end)
      end)

      assert_receive {:wire_request, _request}
    end
  end

  test "local download write failure returns a typed error", %{tmp_dir: dir} do
    destination = Path.join(dir, "existing-file")
    File.write!(destination, "original")
    {endpoint, _server} = WireServer.fixture([{200, [], "new"}])

    assert {:error, %Error{}} =
             Commands.Cp.run(
               "cp",
               ["s3://bucket/key", Path.join(destination, "child")],
               Map.put(@context, :endpoint, endpoint)
             )

    assert File.read!(destination) == "original"
  end

  test "configure rejects corrupt configuration before prompting and leaves it untouched", %{
    tmp_dir: dir
  } do
    path = Path.join(dir, "config.toml")
    contents = "[profiles\nsecret_access_key = \"never-print-this\""
    File.write!(path, contents)

    source =
      "result = ExStorageServiceCli.Commands.Configure.run(\"configure\", [\"--profile\", \"new\"], %{}); IO.inspect(result, label: \"RETURNED\")"

    {output, 0} = library_run(source, [{"ESS_CONFIG_DIR", dir}])
    assert output =~ "category: :config"
    refute output =~ "S3 Endpoint"
    refute output =~ "never-print-this"
    assert File.read!(path) == contents
  end

  test "configure input ending early never creates a profile", %{tmp_dir: dir} do
    source =
      "original = Process.group_leader(); {:ok, input} = StringIO.open(\"\"); Process.group_leader(self(), input); result = ExStorageServiceCli.Commands.Configure.run(\"configure\", [\"--profile\", \"new\"], %{}); Process.group_leader(self(), original); IO.inspect(result, label: \"RETURNED\")"

    {output, 0} = library_run(source, [{"ESS_CONFIG_DIR", dir}])
    assert output =~ "Configuration input ended before the profile was saved"
    refute File.exists?(Path.join(dir, "config.toml"))
  end

  defp library_run(source, environment) do
    paths =
      :code.get_path()
      |> Enum.map(&List.to_string/1)
      |> Enum.filter(&String.contains?(&1, "_build/test/lib/"))

    System.cmd("elixir", Enum.flat_map(paths, &["-pa", &1]) ++ ["-e", source],
      stderr_to_stdout: true,
      env: [{"ERL_FLAGS", "+S 2:2"} | environment]
    )
  end

  defp stop_fixture(responses) do
    WireServer.start(fn listener, owner ->
      Enum.each(responses ++ [{403, [], @denied}], fn {status, headers, body} ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        send(owner, {:wire_request, WireServer.request(socket)})
        WireServer.response(socket, status, headers, body)
        :gen_tcp.close(socket)
      end)

      case :gen_tcp.accept(listener, 250) do
        {:error, :timeout} ->
          send(owner, :no_more_requests)

        {:ok, socket} ->
          send(owner, {:unexpected_request, WireServer.request(socket)})
          :gen_tcp.close(socket)
      end
    end)
  end
end
