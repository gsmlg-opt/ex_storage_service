# Pure, synthetic metadata only: no application startup, credentials or storage roots.
# Run write and verify in SEPARATE VMs; preload atoms in reverse order in verifier.
[operation, path] = System.argv()

case operation do
  "write" ->
    fields = Map.new(1..1000, fn n -> {String.to_atom("ess_repro_field_#{n}"), n} end)
    data = %{version: 2, kv_data: [{"synthetic", {fields, 0}}], indexes: %{}}
    checksum = :crypto.hash(:sha256, :erlang.term_to_binary(data))
    backup = %{metadata: %{checksum: checksum}, data: data}
    :ok = File.write(path, :erlang.term_to_binary(backup, [:compressed]))
    IO.inspect(Concord.Backup.verify(path), label: "same_vm_native_verify")
    IO.puts("synthetic backup SHA256=" <> Base.encode16(:crypto.hash(:sha256, File.read!(path))))

  "verify" ->
    Enum.each(1000..1//-1, &String.to_atom("ess_repro_field_#{&1}"))
    %{metadata: %{checksum: checksum}, data: data} = :erlang.binary_to_term(File.read!(path))
    IO.inspect(checksum == :crypto.hash(:sha256, :erlang.term_to_binary(data)), label: "fresh_vm_checksum_matches")
    IO.inspect(Concord.Backup.verify(path), label: "fresh_vm_native_verify")
    IO.puts("synthetic backup SHA256=" <> Base.encode16(:crypto.hash(:sha256, File.read!(path))))
end
