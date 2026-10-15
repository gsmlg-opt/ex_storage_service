defmodule ExStorageServiceCli.CLI.Registry do
  @moduledoc "Declarative frontend grammar, shared by parsing, help and completion."

  @common %{
    "help" => {:help, :boolean},
    "version" => {:version, :boolean},
    "profile" => {:profile, :string},
    "region" => {:region, :string}
  }
  @native %{
    "config-dir" => {:config_dir, :string},
    "disable-pager" => {:no_cli_pager, :boolean},
    "dp" => {:no_cli_pager, :boolean},
    "insecure" => {:no_verify_ssl, :boolean},
    "custom-header" => {:custom_headers, :repeated_string},
    "resolve" => {:resolve, :repeated_string},
    "limit-upload" => {:limit_upload, :string},
    "limit-download" => {:limit_download, :string},
    "endpoint" => {:endpoint, :string},
    "service" => {:service, {:enum, ~w(ess aws s3)}},
    "access-key" => {:access_key, :string},
    "secret-key" => {:secret_key, :string},
    "session-token" => {:session_token, :string},
    "ca-bundle" => {:ca_bundle, :string},
    "no-verify-ssl" => {:no_verify_ssl, :boolean},
    "client-cert" => {:client_cert, :string},
    "client-key" => {:client_key, :string},
    "output" => {:output, {:enum, ~w(json text table yaml yaml-stream ndjson off)}},
    "quiet" => {:quiet, :boolean},
    "debug" => {:debug, :boolean},
    "max-requests" => {:max_requests, :integer},
    "memory-budget" => {:memory_budget, :integer},
    "max-concurrency" => {:max_concurrency, :integer},
    "tmpdir" => {:tmp_dir, :string},
    "no-sign-request" => {:no_sign_request, :boolean},
    "json" => {:json, :boolean},
    "no-color" => {:no_color, :boolean}
  }
  @aws %{
    "endpoint-url" => {:endpoint_url, :string},
    "no-sign-request" => {:no_sign_request, :boolean},
    "output" => {:output, :string},
    "query" => {:query, :string},
    "debug" => {:debug, :boolean},
    "no-verify-ssl" => {:no_verify_ssl, :boolean},
    "ca-bundle" => {:ca_bundle, :string},
    "cli-connect-timeout" => {:cli_connect_timeout, :integer},
    "cli-read-timeout" => {:cli_read_timeout, :integer},
    "color" => {:color, :string},
    "no-cli-pager" => {:no_cli_pager, :boolean},
    "cli-binary-format" => {:cli_binary_format, :string},
    "no-cli-auto-prompt" => {:no_cli_auto_prompt, :boolean}
  }
  @native_commands %{
    ["configure"] => {0..0, %{}, "Set up access credentials and endpoint"},
    ["mb"] =>
      {1..1,
       %{
         "ignore-existing" => {:ignore_existing, :boolean},
         "with-lock" => {:with_lock, :boolean},
         "with-versioning" => {:with_versioning, :boolean}
       }, "Make (create) a bucket"},
    ["rb"] => {1..1, %{"force" => {:force, :boolean}}, "Remove an empty bucket"},
    ["ls"] =>
      {0..1,
       %{
         "rewind" => {:rewind, :string},
         "prefix" => {:prefix, :string},
         "delimiter" => {:delimiter, :string},
         "max-keys" => {:max_keys, :integer},
         "versions" => {:versions, :boolean},
         "limit" => {:limit, :integer},
         "recursive" => {:recursive, :boolean}
       }, "List buckets or objects"},
    ["tree"] =>
      {1..1,
       %{
         "rewind" => {:rewind, :string},
         "prefix" => {:prefix, :string},
         "max-keys" => {:max_keys, :integer},
         "limit" => {:limit, :integer},
         "depth" => {:depth, :integer},
         "files" => {:files, :boolean}
       }, "Display objects as a directory tree"},
    ["cp"] =>
      {2..2, %{"content-type" => {:content_type, :string}, "recursive" => {:recursive, :boolean}},
       "Copy files or objects"},
    ["rm"] =>
      {1..1,
       %{
         "recursive" => {:recursive, :boolean},
         "version-id" => {:version_id, :string},
         "all-versions" => {:all_versions, :boolean},
         "dry-run" => {:dry_run, :boolean},
         "include" => {:include, :repeated_string},
         "exclude" => {:exclude, :repeated_string}
       }, "Remove objects or explicitly selected versions"},
    ["mv"] => {2..2, %{}, "Move an object (copy and delete)"},
    ["presign"] =>
      {1..1, %{"expires" => {:expires, :integer}, "method" => {:method, :string}},
       "Generate a presigned URL"},
    ["info"] => {0..0, %{}, "Show ESS server health info"},
    ["version"] => {0..0, %{}, "Print CLI version"},
    ["version", "enable"] => {1..1, %{}, "Enable bucket versioning"},
    ["version", "suspend"] => {1..1, %{}, "Suspend bucket versioning"},
    ["version", "info"] => {1..1, %{}, "Inspect bucket versioning"},
    ["alias", "set"] =>
      {2..2,
       %{
         "provider" => {:provider, :string},
         "service-version" => {:service_version, :string},
         "bucket-type" => {:bucket_type, {:enum, ~w(general directory)}}
       }, "Set a named endpoint"},
    ["alias", "list"] => {0..1, %{}, "List aliases with secrets redacted"},
    ["alias", "remove"] => {1..1, %{}, "Remove a named endpoint"},
    ["alias", "import"] =>
      {1..1,
       %{
         "replace" => {:replace, :boolean},
         "allow-secrets" => {:allow_secrets, :boolean},
         "dry-run" => {:dry_run, :boolean}
       }, "Explicitly import alias configuration"},
    ["alias", "export"] =>
      {0..1, %{"include-secrets" => {:include_secrets, :boolean}},
       "Export aliases with secrets redacted"},
    ["completion"] => {1..1, %{}, "Generate shell completion"}
  }
  @s3_commands %{
    ["ls"] => {0..1, "List S3 objects or buckets"},
    ["cp"] => {2..2, "Copy files or S3 objects"},
    ["mv"] => {2..2, "Move files or S3 objects"},
    ["sync"] => {2..2, "Synchronize files and objects"},
    ["rm"] => {1..1, "Remove S3 objects"},
    ["mb"] => {1..1, "Make a bucket"},
    ["rb"] => {1..1, "Remove a bucket"},
    ["presign"] => {1..1, "Presign a GET request"},
    ["website"] => {1..1, "Configure a static website"}
  }

  def commands(:native) do
    transfers =
      for name <- ~w(cp mv sync mirror diff get put), into: %{} do
        reference = if name in ~w(sync mirror diff), do: "sync", else: "cp"
        flags = ExStorageServiceCli.CLI.S3Options.flags([reference]) |> Map.delete("expires")

        flags =
          case name do
            "mirror" -> Map.drop(flags, ~w(size-only exact-timestamps delete))
            "diff" -> Map.delete(flags, "delete")
            _ -> flags
          end

        flags =
          if name in ~w(cp mv get),
            do: Map.put(flags, "version-id", {:version_id, :string}),
            else: flags

        flags =
          if name == "cp",
            do: Map.put(flags, "rewind", {:rewind, :string}),
            else: flags

        flags =
          if name in ~w(cp mv mirror),
            do:
              Map.merge(flags, %{
                "older-than" => {:older_than, :string},
                "newer-than" => {:newer_than, :string}
              }),
            else: flags

        flags =
          if name == "mirror",
            do:
              Map.merge(flags, %{
                "overwrite" => {:overwrite, :boolean},
                "remove" => {:remove, :boolean},
                "exclude-storageclass" => {:exclude_storageclass, :repeated_string},
                "watch" => {:watch, :boolean},
                "adapter" => {:adapter, {:enum, ["polling"]}},
                "interval" => {:interval, :integer},
                "count" => {:count, :integer}
              }),
            else: flags

        {[name],
         {2..2, flags,
          "#{String.capitalize(name)} files and objects through the shared transfer engine"}}
      end

    conveniences = %{
      ["watch"] =>
        {1..1,
         %{
           "adapter" => {:adapter, {:enum, ["polling"]}},
           "interval" => {:interval, :integer},
           "count" => {:count, :integer}
         }, "Compare complete inventories using an explicit polling adapter"},
      ["cat"] =>
        {1..1,
         %{
           "rewind" => {:rewind, :string},
           "version-id" => {:version_id, :string},
           "vid" => {:version_id, :string},
           "offset" => {:offset, :integer},
           "tail" => {:tail, :integer},
           "part-number" => {:part_number, :integer}
         }, "Stream raw object bytes to stdout"},
      ["head"] =>
        {1..1,
         %{
           "rewind" => {:rewind, :string},
           "lines" => {:lines, :integer},
           "n" => {:lines, :integer},
           "bytes" => {:bytes, :integer},
           "version-id" => {:version_id, :string},
           "vid" => {:version_id, :string}
         }, "Print the beginning of an object"},
      ["stat"] =>
        {1..1,
         %{
           "rewind" => {:rewind, :string},
           "version-id" => {:version_id, :string},
           "vid" => {:version_id, :string},
           "recursive" => {:recursive, :boolean},
           "versions" => {:versions, :boolean},
           "limit" => {:limit, :integer}
         }, "Inspect bucket, prefix or object-version metadata"},
      ["du"] =>
        {1..1,
         %{
           "rewind" => {:rewind, :string},
           "limit" => {:limit, :integer},
           "depth" => {:depth, :integer},
           "recursive" => {:recursive, :boolean},
           "versions" => {:versions, :boolean}
         }, "Summarize a bounded object inventory"},
      ["find"] =>
        {1..1,
         %{
           "limit" => {:limit, :integer},
           "name" => {:name, :string},
           "path" => {:path, :string},
           "ignore" => {:ignore, :string},
           "regex" => {:regex, :string},
           "larger" => {:larger, :string},
           "smaller" => {:smaller, :string},
           "older-than" => {:older_than, :string},
           "newer-than" => {:newer_than, :string},
           "maxdepth" => {:maxdepth, :integer},
           "versions" => {:versions, :boolean},
           "include" => {:include, :repeated_string},
           "exclude" => {:exclude, :repeated_string}
         }, "Find keys using a bounded inventory"},
      ["pipe"] =>
        {1..1,
         Map.take(
           ExStorageServiceCli.CLI.S3Options.flags(["cp"]),
           ~w(dryrun quiet only-show-errors expected-size no-progress progress-frequency progress-multiline acl sse sse-c sse-c-key sse-kms-key-id storage-class grants website-redirect content-type cache-control content-disposition content-encoding content-language request-payer checksum-algorithm metadata)
         ), "Stream stdin to an object through a private spool"},
      ["sso", "login"] =>
        {0..0,
         %{
           "use-device-code" => {:use_device_code, :boolean},
           "no-browser" => {:no_browser, :boolean}
         }, "Log in to the selected IAM Identity Center profile"},
      ["sso", "logout"] => {0..0, %{}, "Remove only the selected profile's cached SSO session"}
    }

    @native_commands
    |> Map.merge(transfers)
    |> Map.merge(conveniences)
    |> Map.merge(ExStorageServiceCli.CLI.Management.commands())
    |> Map.merge(ExStorageServiceCli.CLI.NativeUtility.commands())
  end

  def commands(:s3) do
    Map.new(@s3_commands, fn {path, {arity, summary}} ->
      {path, {arity, ExStorageServiceCli.CLI.S3Options.flags(path), summary}}
    end)
  end

  def commands(:s3api) do
    alias ExStorageServiceCli.{CLI.AWSInput, S3.OperationCatalog}

    operations =
      Map.new(OperationCatalog.operations(), fn {name, _} ->
        arity =
          if name in ["GetObject", "GetObjectTorrent", "SelectObjectContent"],
            do: 1..1,
            else: 0..0

        {[OperationCatalog.command_name(name)], {arity, AWSInput.flags(name), "S3 #{name}"}}
      end)

    waiters =
      Map.new(OperationCatalog.waiters(), fn {name, model} ->
        command = name |> String.replace(~r/([a-z0-9])([A-Z])/, "\\1-\\2") |> String.downcase()
        {["wait", command], {0..0, AWSInput.flags(model["operation"]), "Wait for #{name}"}}
      end)

    Map.merge(operations, waiters)
  end

  def globals(:native), do: Map.merge(@common, @native)

  def globals(:s3) do
    Map.merge(@common, @aws)
    |> Map.put("no-paginate", {:no_paginate, {:flag, true}})
    |> Map.put("cli-auto-prompt", {:cli_auto_prompt, {:flag, true}})
    |> Map.put("output", {:output, {:enum, ~w(json text table yaml yaml-stream)}})
    |> Map.put("color", {:color, {:enum, ~w(auto on off)}})
    |> Map.put("cli-binary-format", {:cli_binary_format, {:enum, ~w(base64 raw-in-base64-out)}})
    |> Map.new(fn
      {name, {key, :boolean}} -> {name, {key, {:flag, true}}}
      entry -> entry
    end)
  end

  def globals(:s3api), do: ExStorageServiceCli.CLI.AWSInput.globals()

  def spec(_frontend, ["version"]), do: Map.fetch!(@native_commands, ["version"])

  def spec(frontend, path) do
    Map.get(commands(frontend), path, {0..0, %{}, "Command group"})
  end

  def group?(frontend, path) do
    path != [] and
      Enum.any?(commands(frontend), fn {command, _} ->
        length(command) > length(path) and Enum.take(command, length(path)) == path
      end)
  end

  def options(frontend) do
    Enum.reduce(commands(frontend), globals(frontend), fn {_name, {_arity, flags, _}}, acc ->
      Map.merge(acc, flags)
    end)
  end

  def all_options, do: Map.merge(options(:native), options(:s3))

  def resolve(frontend, words) do
    table = commands(frontend)

    table
    |> Enum.sort_by(fn {path, _spec} -> -length(path) end)
    |> Enum.find_value(fn {path, spec} ->
      count = length(path)
      if Enum.take(words, count) == path, do: {path, Enum.drop(words, count), spec}
    end)
  end

  def short_options,
    do: %{
      "h" => "help",
      "r" => "recursive",
      "f" => "force",
      "e" => "expires",
      "C" => "config-dir",
      "H" => "custom-header",
      "n" => "n"
    }
end
