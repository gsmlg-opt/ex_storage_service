defmodule ExStorageService.BlobStore.ReadRepairTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.{LocalCAS, ReadRepair, Source}

  defmodule NoPack do
    def locate(_hash), do: {:error, :not_found}
  end

  test "stateful reduction retains compatibility with unary stream producers" do
    source =
      Source.stream(
        fn sink ->
          with :ok <- sink.("legacy "),
               :ok <- sink.("producer") do
            :ok
          end
        end,
        15
      )

    assert {:ok, "legacy producer"} =
             Source.reduce(source, "", fn chunk, acc -> {:cont, acc <> chunk} end)
  end

  @tag :tmp_dir
  test "tees multiple chunks into a checksum-verified local CAS blob", %{tmp_dir: tmp_dir} do
    body = "bounded-" <> String.duplicate("repair", 100)
    hash = sha256(body)
    parent = self()
    opts = repair_opts(tmp_dir, on_ready: &send(parent, {:ready, &1}))
    source = Source.stream(chunk(body, 37), byte_size(body))
    wrapped = ReadRepair.wrap(source, hash, byte_size(body), opts)

    assert {:ok, ^body} =
             Source.reduce(wrapped, "", fn data, acc -> {:cont, acc <> data} end)

    assert_receive {:ready, %{hash: ^hash, size: size, path: path}}
    assert size == byte_size(body)
    assert File.read!(path) == body
    assert {:ok, %{size: ^size}} = LocalCAS.stat(hash, opts[:blob_store_opts])
    assert staging_files(opts) == []
  end

  @tag :tmp_dir
  test "an incomplete remote stream is served but never published as a repair", %{
    tmp_dir: tmp_dir
  } do
    body = "short"
    expected = body <> "-missing"
    hash = sha256(expected)
    parent = self()
    opts = repair_opts(tmp_dir, on_ready: &send(parent, {:ready, &1}))

    wrapped =
      ReadRepair.wrap(Source.stream([body], byte_size(expected)), hash, byte_size(expected), opts)

    assert {:ok, ^body} =
             Source.reduce(wrapped, "", fn data, acc -> {:cont, acc <> data} end)

    refute_receive {:ready, _ready}
    assert {:error, :not_found} = LocalCAS.stat(hash, opts[:blob_store_opts])
    assert staging_files(opts) == []
  end

  @tag :tmp_dir
  test "verified repair atomically replaces an existing same-size corrupt CAS file", %{
    tmp_dir: tmp_dir
  } do
    body = "valid-repair-bytes"
    corrupt = "corrupt-local-byte"
    assert byte_size(body) == byte_size(corrupt)

    hash = sha256(body)
    opts = repair_opts(tmp_dir, on_ready: fn _ready -> :ok end)
    destination = LocalCAS.blob_path(hash, opts[:blob_store_opts])
    File.mkdir_p!(Path.dirname(destination))
    File.write!(destination, corrupt)

    wrapped =
      ReadRepair.wrap(
        Source.stream(chunk(body, 4), byte_size(body)),
        hash,
        byte_size(body),
        opts
      )

    assert {:ok, ^body} =
             Source.reduce(wrapped, "", fn data, acc -> {:cont, acc <> data} end)

    assert File.read!(destination) == body
    assert :ok = LocalCAS.verify(hash, opts[:blob_store_opts])
    assert staging_files(opts) == []
  end

  @tag :tmp_dir
  test "checksum mismatch and downstream cancellation discard partial repair files", %{
    tmp_dir: tmp_dir
  } do
    parent = self()
    body = "checksum-mismatch"
    expected_hash = sha256("different-content")
    opts = repair_opts(tmp_dir, on_ready: &send(parent, {:ready, &1}))

    mismatch =
      ReadRepair.wrap(
        Source.stream(chunk(body, 3), byte_size(body)),
        expected_hash,
        byte_size(body),
        opts
      )

    assert {:ok, ^body} =
             Source.reduce(mismatch, "", fn data, acc -> {:cont, acc <> data} end)

    refute_receive {:ready, _ready}
    assert {:error, :not_found} = LocalCAS.stat(expected_hash, opts[:blob_store_opts])
    assert staging_files(opts) == []

    cancelled =
      ReadRepair.wrap(
        Source.stream(chunk(body, 3), byte_size(body)),
        sha256(body),
        byte_size(body),
        opts
      )

    assert {:error, :closed, "che"} =
             Source.reduce(cancelled, "", fn data, _acc -> {:halt, :closed, data} end)

    refute_receive {:ready, _ready}
    assert staging_files(opts) == []

    exceptional =
      ReadRepair.wrap(
        Source.stream(chunk(body, 3), byte_size(body)),
        sha256(body),
        byte_size(body),
        opts
      )

    assert_raise RuntimeError, "sink crashed", fn ->
      Source.reduce(exceptional, :consumer, fn _data, _state -> raise "sink crashed" end)
    end

    assert staging_files(opts) == []
  end

  defp repair_opts(tmp_dir, extra) do
    root = Path.join(tmp_dir, "cas")

    [
      blob_store: LocalCAS,
      blob_store_opts: [
        root: root,
        tmp_dir: Path.join(tmp_dir, "staging"),
        pack_module: NoPack
      ],
      finalizer: :inline
    ]
    |> Keyword.merge(extra)
  end

  defp staging_files(opts) do
    opts[:blob_store_opts][:tmp_dir]
    |> Path.join("upload-*")
    |> Path.wildcard()
  end

  defp chunk(body, size),
    do: for(<<chunk::binary-size(size) <- body>>, do: chunk) ++ tail(body, size)

  defp tail(body, size) do
    remainder = rem(byte_size(body), size)
    if remainder == 0, do: [], else: [binary_part(body, byte_size(body) - remainder, remainder)]
  end

  defp sha256(data), do: :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)
end
