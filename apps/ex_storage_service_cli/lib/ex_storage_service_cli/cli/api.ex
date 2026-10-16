defmodule ExStorageServiceCli.CLI.API do
  @moduledoc "Modeled s3api execution through the shared protocol and transfer effects."
  alias ExStorageServiceCli.{
    CLI.Runtime,
    CLI.Skeleton,
    CLI.OutputSession,
    Context,
    Presenter,
    S3.Client,
    S3.OperationCatalog,
    S3.Paginator,
    S3.Select,
    S3.Waiter,
    S3.WriteResponse,
    Transfer.Engine,
    Transfer.Sink,
    Transfer.Source
  }

  def run(invocation) do
    operation =
      if invocation.options[:waiter],
        do: OperationCatalog.waiters()[invocation.options[:waiter]]["operation"],
        else: OperationCatalog.operation_name(hd(invocation.command))

    options = invocation.options
    input = options[:api_input] || %{}

    if options[:generate_cli_skeleton] do
      skeleton(operation, options)
    else
      with :ok <- presentation_ready(options) do
        Runtime.with_invocation(
          fn runtime ->
            with {:ok, context} <- Runtime.aws_context(runtime, options),
                 :ok <- warning(context),
                 {:ok, options} <- OutputSession.prepare_options(context.options),
                 {:ok, session} <- OutputSession.open(options, coordinator: runtime.coordinator) do
              try do
                with :ok <-
                       execute(
                         context,
                         operation,
                         input,
                         invocation.args,
                         Map.put(options, :output_session, session)
                       ),
                     do: OutputSession.finish(session)
              after
                OutputSession.abort(session)
              end
            end
          end,
          Map.to_list(options)
        )
      end
    end
  end

  defp skeleton(operation, options) do
    kind = if options[:generate_cli_skeleton] == "output", do: :output, else: :input
    format = if options[:generate_cli_skeleton] == "yaml-input", do: "yaml", else: "json"

    with {:ok, data} <- Skeleton.generate(operation, kind),
         do: Presenter.write(data, %{output: format})
  end

  defp presentation_ready(options) do
    cond do
      options[:query] ->
        ExStorageServiceCli.Query.validate(options[:query])

      true ->
        :ok
    end
  end

  defp execute(context, _operation, input, _args, %{waiter: name}) do
    Waiter.wait(name, input, fn operation, input ->
      Client.call(
        context,
        operation,
        input,
        Keyword.put(request_options(context), :return_response, true)
      )
    end)
  end

  defp execute(context, operation, input, args, options) do
    if (options[:output] in ["text", "yaml-stream"] and OperationCatalog.paginators()[operation]) &&
         options[:no_paginate] != true do
      Paginator.cli_pages(
        operation,
        input,
        fn operation, input ->
          Client.call(context, operation, input, request_options(context))
        end,
        pagination_options(options)
      )
      |> Enum.reduce_while(:ok, fn
        {:error, _} = error, _ ->
          {:halt, error}

        {:ok, page, metadata}, :ok ->
          with :ok <- present_page(operation, page, metadata.first, options),
               :ok <- resume_token(metadata.resume_token, options) do
            {:cont, :ok}
          else
            error -> {:halt, error}
          end
      end)
    else
      with {:ok, result} <- operation(context, operation, input, args, options),
           do: Presenter.write(result, Map.put(options, :command_name, operation))
    end
  end

  defp present_page(_operation, page, _first, %{output: "yaml-stream"} = options),
    do: Presenter.write_stream_page(page, options)

  defp present_page(operation, page, first, options) do
    model = OperationCatalog.paginators()[operation]

    page =
      Enum.reduce(List.wrap(model["result_key"]), page, fn key, result ->
        Map.put_new(result, key, nil)
      end)

    Presenter.write(Paginator.output_page(operation, page, first), options)
  end

  defp resume_token(nil, _options), do: :ok
  defp resume_token(_token, %{output: "yaml-stream"}), do: :ok

  defp resume_token(token, options),
    do: Presenter.write(%{"NextToken" => %{"NextToken" => token}}, options)

  defp pagination_options(options),
    do: [
      page_size: options[:page_size],
      no_paginate: options[:no_paginate] || false,
      starting_token: options[:starting_token],
      max_items: options[:max_items]
    ]

  defp operation(context, name, input, [filename], _options)
       when name in ["GetObject", "GetObjectTorrent"] do
    with {:ok, result} <-
           Engine.download_file(context, input["Bucket"], input["Key"], filename,
             input: input,
             operation: name,
             timeout: context.timeout
           ),
         do: {:ok, result.result}
  end

  defp operation(context, "SelectObjectContent", input, [filename], _options) do
    with {:ok, sink} <- Sink.open(filename) do
      try do
        reducer = fn
          %{type: "Records", payload: bytes}, result ->
            with :ok <- Sink.write(sink, bytes), do: {:ok, result}

          _event, result ->
            {:ok, result}
        end

        with {:ok, _result, _summary} <-
               Select.call(context, input, nil, reducer, request_options(context)),
             :ok <- Sink.publish(sink),
             do: {:ok, %{}}
      after
        Sink.abort(sink)
      end
    end
  end

  defp operation(context, "WriteGetObjectResponse", input, _args, %{streaming_body_path: path}) do
    with {:ok, source} <- Source.file(path, coordinator: context.coordinator),
         do:
           WriteResponse.call(
             context,
             Map.delete(input, "Body"),
             Keyword.merge(request_options(context),
               body_source: Source.enumerable(source),
               payload_hash: source.payload_hash,
               checksum_source: source,
               content_length: source.length
             )
           )
  end

  defp operation(context, "WriteGetObjectResponse", input, _args, _options),
    do: WriteResponse.call(context, input, request_options(context))

  defp operation(context, name, input, _args, %{streaming_body_path: path}) do
    with {:ok, source} <- Source.file(path, coordinator: context.coordinator),
         do:
           Client.call(
             context,
             name,
             Map.delete(input, "Body"),
             Keyword.merge(request_options(context),
               body_source: Source.enumerable(source),
               payload_hash: source.payload_hash,
               checksum_source: source,
               content_length: source.length
             )
           )
  end

  defp operation(context, name, input, _args, options) do
    Paginator.collect(
      name,
      input,
      fn operation, input -> Client.call(context, operation, input, request_options(context)) end,
      pagination_options(options)
    )
  end

  defp request_options(context),
    do: [timeout: context.timeout, connect_timeout: context.connect_timeout]

  defp warning(%Context{tls: tls}) do
    if tls[:verify] == :verify_none,
      do: IO.puts(:stderr, "Warning: TLS certificate verification is disabled")

    :ok
  end
end
