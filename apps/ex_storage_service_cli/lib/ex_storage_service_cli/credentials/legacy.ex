defmodule ExStorageServiceCli.Credentials.Legacy do
  @moduledoc "Pinned legacy EC2 credential-file and Boto provider precedence."
  alias ExStorageServiceCli.Credentials.{Record, Remote, SharedConfig}

  def resolve(env, opts \\ []) do
    with {:ok, record} <- ec2(env, opts) do
      if record, do: {:ok, record}, else: boto(env, opts)
    end
  end

  defp ec2(%{"AWS_CREDENTIAL_FILE" => path}, opts) do
    with {:ok, body} <- Remote.file(expand(path, opts)),
         {:ok, sections} <- SharedConfig.parse("[ec2]\n" <> body) do
      values = sections["ec2"] || %{}

      if values["awsaccesskeyid"],
        do:
          Record.new(
            %{access_key_id: values["awsaccesskeyid"], secret_access_key: values["awssecretkey"]},
            %{provider: :ec2_credentials_file}
          ),
        else: {:ok, nil}
    end
  end

  defp ec2(_env, _opts), do: {:ok, nil}

  defp boto(env, opts) do
    home = opts[:home] || env["HOME"] || System.user_home!()

    paths =
      if env["BOTO_CONFIG"],
        do: [env["BOTO_CONFIG"]],
        else: ["/etc/boto.cfg", Path.join(home, ".boto")]

    Enum.reduce_while(paths, {:ok, nil}, fn path, _acc ->
      path = expand(path, opts)

      if File.exists?(path) do
        result =
          with {:ok, body} <- Remote.file(path), {:ok, sections} <- SharedConfig.parse(body) do
            values = sections["Credentials"] || %{}

            if values["aws_access_key_id"],
              do:
                Record.new(
                  %{
                    access_key_id: values["aws_access_key_id"],
                    secret_access_key: values["aws_secret_access_key"]
                  },
                  %{provider: :boto}
                ),
              else: {:ok, nil}
          end

        case result do
          {:ok, nil} -> {:cont, result}
          _ -> {:halt, result}
        end
      else
        {:cont, {:ok, nil}}
      end
    end)
  end

  defp expand("~/" <> path, opts), do: Path.join(opts[:home] || System.user_home!(), path)
  defp expand(path, _opts), do: Path.expand(path)
end
