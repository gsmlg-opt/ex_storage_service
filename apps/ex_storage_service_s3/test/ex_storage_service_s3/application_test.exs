defmodule ExStorageServiceS3.ApplicationTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceS3.Application

  test "includes the Bandit listener by default" do
    assert [{Bandit, options}] = Application.children(enabled: true, port: 19_001)
    assert options[:plug] == ExStorageServiceS3.Router
    assert options[:port] == 19_001
    assert options[:scheme] == :http
  end

  test "omits the Bandit listener when public S3 is disabled" do
    assert Application.children(enabled: false) == []
  end
end
