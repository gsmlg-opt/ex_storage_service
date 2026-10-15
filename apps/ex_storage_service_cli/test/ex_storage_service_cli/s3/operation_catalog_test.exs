defmodule ExStorageServiceCli.S3.OperationCatalogTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.S3.OperationCatalog

  test "all pinned operations, shapes, paginators and waiters load offline" do
    assert map_size(OperationCatalog.operations()) == 108
    assert map_size(OperationCatalog.shapes()) == 670
    assert map_size(OperationCatalog.paginators()) == 7
    assert map_size(OperationCatalog.waiters()) == 4
    assert OperationCatalog.shape("Expires")["type"] == "timestamp"

    for {name, model} <- OperationCatalog.operations() do
      command = OperationCatalog.command_name(name)
      assert is_binary(command)
      assert OperationCatalog.operation_name(command) == name
      assert {:ok, ^model} = OperationCatalog.operation(command)
    end
  end

  test "unknown names return a typed error without creating atoms" do
    assert {:error, %Error{category: :syntax, code: :unknown_operation}} =
             OperationCatalog.operation("unknown-operation")

    assert OperationCatalog.shape("UnknownShape") == nil
    assert OperationCatalog.command_name("UnknownOperation") == nil
  end
end
