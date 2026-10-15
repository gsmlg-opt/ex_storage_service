defmodule ExStorageServiceCli.S3.OperationCatalog do
  @moduledoc "Offline, immutable AWS CLI 2.31.18 S3 models and customization overlays."
  alias ExStorageServiceCli.Error

  @root Path.expand("../../../priv/compatibility/aws", __DIR__)
  @external_resource Path.join(@root, "service-2.json")
  @external_resource Path.join(@root, "service-2.sdk-extras.json")
  @external_resource Path.join(@root, "paginators-1.json")
  @external_resource Path.join(@root, "paginators-1.sdk-extras.json")
  @external_resource Path.join(@root, "waiters-2.json")
  @model (fn ->
            merge = fn recur, left, right ->
              Map.merge(left, right, fn _key, a, b ->
                if is_map(a) and is_map(b), do: recur.(recur, a, b), else: b
              end)
            end

            source = @root |> Path.join("service-2.json") |> File.read!() |> JSON.decode!()

            extras =
              @root |> Path.join("service-2.sdk-extras.json") |> File.read!() |> JSON.decode!()

            merge.(merge, source, extras["merge"])
          end).()
  @operations @model["operations"]
  @shapes @model["shapes"]
  @paginators (fn ->
                 base =
                   @root
                   |> Path.join("paginators-1.json")
                   |> File.read!()
                   |> JSON.decode!()
                   |> Map.fetch!("pagination")

                 extras =
                   @root
                   |> Path.join("paginators-1.sdk-extras.json")
                   |> File.read!()
                   |> JSON.decode!()
                   |> get_in(["merge", "pagination"])

                 Map.merge(base, extras, fn _key, left, right -> Map.merge(left, right) end)
               end).()
  @waiters @root
           |> Path.join("waiters-2.json")
           |> File.read!()
           |> JSON.decode!()
           |> Map.fetch!("waiters")
  @commands Map.new(@operations, fn {name, _model} ->
              command =
                name
                |> String.replace(~r/([A-Z]+)([A-Z][a-z])/, "\\1-\\2")
                |> String.replace(~r/([a-z0-9])([A-Z])/, "\\1-\\2")
                |> String.downcase()

              {command, name}
            end)
  @names Map.new(@commands, fn {command, name} -> {name, command} end)

  def operations, do: @operations
  def shapes, do: @shapes
  def shape(name), do: Map.get(@shapes, name)
  def paginators, do: @paginators
  def waiters, do: @waiters
  def command_name(name), do: Map.get(@names, name)

  def operation_name(name),
    do: if(Map.has_key?(@operations, name), do: name, else: Map.get(@commands, name))

  def operation(name) do
    case operation_name(name) do
      nil -> {:error, Error.new(:syntax, "Unknown S3 operation", code: :unknown_operation)}
      name -> {:ok, Map.fetch!(@operations, name)}
    end
  end
end
