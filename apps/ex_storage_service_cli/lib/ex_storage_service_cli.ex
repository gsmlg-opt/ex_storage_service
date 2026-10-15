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

  alias ExStorageServiceCli.{CLI.ErrorOutput, CLI.Parser, CLI.Runner, Error, ExitStatus}

  def main(args) do
    Process.put(:ess_handle_signals, true)

    case Parser.parse(args) do
      {:ok, invocation} ->
        if invocation.options[:no_color], do: Application.put_env(:elixir, :ansi_enabled, false)

        case Runner.execute(invocation) do
          :ok -> :ok
          {:error, %Error{} = error} -> finish_error(error, invocation.frontend)
        end

      {:error, %Error{} = error} ->
        finish_error(error, Map.get(error.details, :frontend, :native))
    end
  rescue
    _error -> finish_error(Error.new(:client, "Client operation failed"), :native)
  end

  @doc """
  Returns the current CLI version.
  """
  def version, do: @version

  defp finish_error(error, frontend) do
    unless error.details[:reported], do: ErrorOutput.write(error, frontend)
    System.halt(ExitStatus.code(error, frontend))
  end
end
