defmodule ExStorageServiceCli.CLI.Debug do
  @moduledoc "Opt-in request diagnostics that never serialize authentication, URLs, keys or bodies."

  def request(context, request, url, opts \\ []) do
    uri = URI.parse(url)

    write(
      context,
      %{
        event: "request",
        operation: request.operation,
        method: request.method,
        host: uri.host,
        port: uri.port,
        region: context.region,
        attempt: opts[:attempt] || 1
      },
      opts
    )
  end

  def response(context, request, response, opts \\ []) do
    write(
      context,
      %{event: "response", operation: request.operation, status: response.status},
      opts
    )
  end

  defp write(context, event, opts) do
    if context.options[:debug] == true do
      writer = opts[:debug_write] || fn text -> IO.write(:stderr, text) end
      writer.(JSON.encode!(event) <> "\n")
    end

    :ok
  end
end
