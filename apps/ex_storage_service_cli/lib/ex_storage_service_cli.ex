defmodule ExStorageServiceCli do
  @moduledoc """
  CLI tool for ExStorageService S3-compatible object storage.

  Provides an `ess` command-line interface for managing buckets and objects.

  ## Usage

      ess <command> [options]

  ## Commands

      configure         Set up access credentials and endpoint
      mb <bucket>       Make (create) a bucket
      rb <bucket>       Remove (delete) a bucket
      ls [bucket[/prefix]]  List buckets or objects
      tree <bucket[/prefix]>  Display objects as a directory tree
      cp <src> <dst>    Copy files (upload/download)
      rm <target>       Remove an object
      mv <src> <dst>    Move an object (copy + delete)
      presign <target>  Generate a presigned URL
      info              Show server health info
      version           Print CLI version

  ## Global Options

      --endpoint <url>     S3 endpoint (default: http://localhost:9000)
      --profile <name>     Use a named profile
      --access-key <id>    Override access key ID
      --secret-key <key>   Override secret access key
      --region <region>    AWS region (default: us-east-1)
      --json               Output in JSON format
      --no-color           Disable colored output
      -h, --help           Show help
  """

  @version Mix.Project.config()[:version]

  alias ExStorageServiceCli.Config
  alias ExStorageServiceCli.Output
  alias ExStorageServiceCli.Commands

  @commands %{
    "configure" => Commands.Configure,
    "mb" => Commands.Bucket,
    "rb" => Commands.Bucket,
    "ls" => Commands.Ls,
    "tree" => Commands.Tree,
    "cp" => Commands.Cp,
    "rm" => Commands.Rm,
    "mv" => Commands.Mv,
    "presign" => Commands.Presign,
    "info" => Commands.Info,
    "version" => Commands.Version
  }

  def main(args) do
    {global_opts, rest, _invalid} =
      OptionParser.parse_head(args,
        strict: [
          endpoint: :string,
          profile: :string,
          access_key: :string,
          secret_key: :string,
          region: :string,
          json: :boolean,
          no_color: :boolean,
          help: :boolean
        ],
        aliases: [h: :help]
      )

    if global_opts[:no_color] do
      Application.put_env(:elixir, :ansi_enabled, false)
    end

    ctx = build_context(global_opts)

    case rest do
      [] ->
        if global_opts[:help] do
          print_help()
        else
          print_help()
        end

      [command | cmd_args] ->
        if global_opts[:help] do
          dispatch_help(command)
        else
          dispatch(command, cmd_args, ctx)
        end
    end
  end

  @doc """
  Returns the current CLI version.
  """
  def version, do: @version

  defp build_context(opts) do
    profile_name = opts[:profile] || "default"
    profile = Config.load_profile(profile_name)

    %{
      endpoint: opts[:endpoint] || profile[:endpoint] || "http://localhost:9000",
      access_key_id: opts[:access_key] || profile[:access_key_id],
      secret_access_key: opts[:secret_key] || profile[:secret_access_key],
      region: opts[:region] || profile[:region] || "us-east-1",
      json: opts[:json] || false,
      profile: profile_name
    }
  end

  defp dispatch(command, args, ctx) do
    case Map.get(@commands, command) do
      nil ->
        Output.error("Unknown command: #{command}")
        print_help()
        System.halt(1)

      module ->
        try do
          module.run(command, args, ctx)
        rescue
          e ->
            Output.error("#{Exception.message(e)}")
            System.halt(1)
        end
    end
  end

  defp dispatch_help(command) do
    case Map.get(@commands, command) do
      nil ->
        Output.error("Unknown command: #{command}")
        print_help()

      module ->
        module.help(command)
    end
  end

  defp print_help do
    IO.puts("""
    #{IO.ANSI.bright()}ess#{IO.ANSI.reset()} — ExStorageService CLI v#{@version}

    #{IO.ANSI.bright()}USAGE#{IO.ANSI.reset()}
        ess <command> [options]

    #{IO.ANSI.bright()}COMMANDS#{IO.ANSI.reset()}
        configure             Set up access credentials and endpoint
        mb <bucket>           Make (create) a bucket
        rb <bucket>           Remove (delete) a bucket
        ls [bucket[/prefix]]  List buckets or objects
        tree <bucket[/prefix]>  Display objects as a directory tree
        cp <src> <dst>        Copy files (upload/download)
        rm s3://<bucket>/<key>  Remove an object
        mv <src> <dst>        Move an object (copy + delete)
        presign s3://<b>/<k>  Generate a presigned URL
        info                  Show server health info
        version               Print CLI version

    #{IO.ANSI.bright()}GLOBAL OPTIONS#{IO.ANSI.reset()}
        --endpoint <url>      S3 endpoint (default: http://localhost:9000)
        --profile <name>      Use a named profile
        --access-key <id>     Override access key ID
        --secret-key <key>    Override secret access key
        --region <region>     AWS region (default: us-east-1)
        --json                Output in JSON format
        --no-color            Disable colored output
        -h, --help            Show help

    #{IO.ANSI.bright()}EXAMPLES#{IO.ANSI.reset()}
        ess configure
        ess mb my-bucket
        ess ls
        ess tree my-bucket
        ess cp ./file.txt s3://my-bucket/file.txt
        ess cp s3://my-bucket/file.txt ./downloaded.txt
        ess ls my-bucket --json
        ess presign s3://my-bucket/file.txt --expires 3600
    """)
  end
end
