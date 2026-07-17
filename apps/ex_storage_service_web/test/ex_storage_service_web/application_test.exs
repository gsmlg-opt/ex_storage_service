defmodule ExStorageServiceWeb.ApplicationTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceWeb.Application

  test "includes the endpoint by default" do
    assert Application.children(enabled: true) == [ExStorageServiceWeb.Endpoint]
  end

  test "omits the endpoint when the web listener is disabled" do
    assert Application.children(enabled: false) == []
  end
end
