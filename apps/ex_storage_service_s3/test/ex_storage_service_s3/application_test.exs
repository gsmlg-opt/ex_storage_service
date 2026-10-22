defmodule ExStorageServiceS3.ApplicationTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceS3.Application

  test "includes the Gaoxy.HTTP listener by default" do
    assert [{Gaoxy.HTTP, options}] = Application.children(enabled: true, port: 19_001)
    assert options[:plug] == {ExStorageServiceS3.Router, []}
    assert options[:ip] == {0, 0, 0, 0}
    assert options[:port] == 19_001
    assert options[:protocols] == [:http2, :http1]
    assert options[:h2c] == :prior_knowledge
    assert {:ok, _spec} = Gaoxy.HTTP.service_spec(options)
    assert options[:limits] == %{max_active: 64, max_bytes: 256 * 1024 * 1024}

    assert options[:http_limits] == %{
             max_body_bytes:
               Enum.max([
                 Elixir.Application.get_env(
                   :ex_storage_service,
                   :max_object_size,
                   5 * 1024 * 1024 * 1024
                 ),
                 Elixir.Application.get_env(
                   :ex_storage_service,
                   :max_part_size,
                   5 * 1024 * 1024 * 1024
                 ),
                 1_048_576
               ]) *
                 8_197 + 49_166,
             request_timeout: :infinity,
             header_timeout: 5_000,
             read_timeout: 60_000,
             write_timeout: 60_000
           }
  end

  test "omits the Gaoxy.HTTP listener when public S3 is disabled" do
    assert Application.children(enabled: false) == []
  end

  test "metadata role cannot start the Gaoxy.HTTP listener" do
    assert Application.children(enabled: true, node_role: :metadata) == []
  end
end
