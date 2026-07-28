defmodule ExStorageService.BlobStore.SourceTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.Source
  alias ExStorageService.BlobStore.Source.RequestBodyError

  @tag :tmp_dir
  test "reduces only the requested file slice in bounded chunks", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "blob")
    File.write!(path, "prefix-" <> :binary.copy("x", 300_000) <> "-suffix")

    source = Source.file(path, 7, 300_000)

    assert {:ok, {300_000, max_chunk, hash}} =
             Source.reduce(source, {0, 0, :crypto.hash_init(:sha256)}, fn chunk,
                                                                          {bytes, max_chunk, hash} ->
               {:cont,
                {
                  bytes + byte_size(chunk),
                  max(max_chunk, byte_size(chunk)),
                  :crypto.hash_update(hash, chunk)
                }}
             end)

    assert max_chunk <= 262_144
    assert :crypto.hash_final(hash) == :crypto.hash(:sha256, :binary.copy("x", 300_000))
  end

  test "request body forwards a stateful source without joining chunks" do
    chunks = for size <- [31, 127, 509, 2_047], do: :binary.copy("z", size)
    total = Enum.sum(Enum.map(chunks, &byte_size/1))

    source =
      Source.stateful_stream(
        fn initial, reducer ->
          Enum.reduce_while(chunks, {:ok, initial}, fn chunk, {:ok, current} ->
            case reducer.(chunk, current) do
              {:cont, next} -> {:cont, {:ok, next}}
              {:halt, reason, next} -> {:halt, {:error, reason, next}}
            end
          end)
        end,
        total
      )

    assert {^total, 2_047, 4} =
             Enum.reduce(Source.request_body(source), {0, 0, 0}, fn chunk,
                                                                    {bytes, max_chunk, count} ->
               {bytes + byte_size(chunk), max(max_chunk, byte_size(chunk)), count + 1}
             end)
  end

  @tag :tmp_dir
  test "request body supports file, enumerable, callback, and stateful sources", %{
    tmp_dir: tmp_dir
  } do
    path = Path.join(tmp_dir, "all-source-forms")
    File.write!(path, "xxabcdefyy")

    callback =
      Source.stream(
        fn sink ->
          Enum.reduce_while(["ab", "cd", "ef"], :ok, fn chunk, :ok ->
            case sink.(chunk) do
              :ok -> {:cont, :ok}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
        end,
        6
      )

    stateful =
      Source.stateful_stream(
        fn initial, reducer ->
          Enum.reduce_while(["ab", "cd", "ef"], {:ok, initial}, fn chunk, {:ok, current} ->
            case reducer.(chunk, current) do
              {:cont, next} -> {:cont, {:ok, next}}
              {:halt, reason, next} -> {:halt, {:error, reason, next}}
            end
          end)
        end,
        6
      )

    sources = [
      Source.file(path, 2, 6),
      Source.stream(["ab", "cd", "ef"], 6),
      callback,
      stateful
    ]

    Enum.each(sources, fn source ->
      assert source |> Source.request_body() |> Enum.to_list() |> IO.iodata_to_binary() ==
               "abcdef"
    end)
  end

  test "request body raises a typed error when its source fails" do
    source =
      Source.stateful_stream(
        fn initial, reducer ->
          {:cont, next} = reducer.("partial", initial)
          {:error, :upstream_closed, next}
        end,
        20
      )

    assert_raise RequestBodyError, ~r/upstream_closed/, fn ->
      Enum.to_list(Source.request_body(source))
    end
  end
end
