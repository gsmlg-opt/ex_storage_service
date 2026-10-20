defmodule ExStorageService.RuntimeConfigTest do
  use ExUnit.Case, async: true

  @runtime Path.expand("../../../../config/runtime.exs", __DIR__)
  @reader """
  [path, encoded_env] = System.argv()

  for {name, _value} <- System.get_env(), String.starts_with?(name, "ESS_") do
    System.delete_env(name)
  end

  System.put_env(:erlang.binary_to_term(Base.decode64!(encoded_env)))

  result =
    try do
      {:ok, Config.Reader.read!(path, env: :test, imports: :disabled)}
    rescue
      error -> {:error, Exception.message(error)}
    end

  IO.write(Base.encode64(:erlang.term_to_binary(result)))
  """

  test "both modes retain command version zero when no override is supplied" do
    for mode <- ["standalone", "cluster"] do
      assert {:ok, config} = read_runtime(mode, nil)

      assert config
             |> Keyword.fetch!(:concord)
             |> Keyword.fetch!(:vsr)
             |> Keyword.fetch!(:command_version) == 0
    end
  end

  test "explicit command versions reach both Concord modes without changing safety defaults" do
    for mode <- ["standalone", "cluster"], version <- ["0", "1"] do
      assert {:ok, config} = read_runtime(mode, version)
      vsr = config |> Keyword.fetch!(:concord) |> Keyword.fetch!(:vsr)
      instance = config |> Keyword.fetch!(:ex_storage_service) |> Keyword.fetch!(:instance_config)

      assert Keyword.fetch!(vsr, :command_version) == String.to_integer(version)
      assert Keyword.fetch!(vsr, :storage) == :file
      refute Keyword.fetch!(vsr, :bootstrap)
      refute Keyword.fetch!(instance, :allow_degraded_writes)
      refute Keyword.fetch!(instance, :public_s3_enabled)
      refute Keyword.fetch!(instance, :cluster_data_plane_enabled)
      expected_quorum = if mode == "cluster", do: 2, else: 1
      assert Keyword.fetch!(instance, :replication_factor) == expected_quorum
      assert Keyword.fetch!(instance, :write_quorum) == expected_quorum
      assert length(Keyword.fetch!(vsr, :members)) == if(mode == "cluster", do: 3, else: 1)
    end
  end

  test "both modes reject every value except the exact strings zero and one" do
    for mode <- ["standalone", "cluster"],
        value <- ["", "-1", "2", "01", "+1", "1.0", " 1", "1 ", "true"] do
      assert {:error, message} = read_runtime(mode, value)
      assert message == "ESS_CONCORD_COMMAND_VERSION must be 0 or 1, got: #{inspect(value)}"
    end
  end

  defp read_runtime(mode, version) do
    env = %{
      "ESS_MODE" => mode,
      "ESS_AUTO_START" => "false",
      "ESS_DATA_ROOT" => "/tmp/ess-runtime-config-reader-only",
      "ESS_WEB_ENABLED" => "false",
      "ESS_PUBLIC_S3_ENABLED" => "false",
      "ESS_CLUSTER_NAME" => "runtime-config-test",
      "ESS_NODE_ID" => "node-a",
      "ESS_CLUSTER_MEMBERS" =>
        "node-a=ess-a@127.0.0.1,node-b=ess-b@127.0.0.1,node-c=ess-c@127.0.0.1"
    }

    env = if is_nil(version), do: env, else: Map.put(env, "ESS_CONCORD_COMMAND_VERSION", version)
    executable = System.find_executable("elixir")
    assert is_binary(executable)

    {output, status} =
      System.cmd(
        executable,
        [
          "--erl",
          "+S 2:2 +SDcpu 1 +SDio 1",
          "-e",
          @reader,
          "--",
          @runtime,
          Base.encode64(:erlang.term_to_binary(env))
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
    :erlang.binary_to_term(Base.decode64!(output))
  end
end
