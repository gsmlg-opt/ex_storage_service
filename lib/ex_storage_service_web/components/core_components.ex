defmodule ExStorageServiceWeb.CoreComponents do
  @moduledoc """
  Provides core UI components.
  """
  use Phoenix.Component

  alias Phoenix.LiveView.JS

  @doc """
  Renders a header with title.
  """
  attr :class, :string, default: nil

  slot :subtitle
  slot :actions
  slot :inner_block, required: true

  def header(assigns) do
    ~H"""
    <header class={["space-y-1", @class]}>
      <div class="flex items-center justify-between">
        <h1 class="text-2xl font-semibold leading-8 text-zinc-800">
          {render_slot(@inner_block)}
        </h1>
        <div :if={@actions != []} class="flex items-center gap-4">
          {render_slot(@actions)}
        </div>
      </div>
      <p :for={subtitle <- @subtitle} class="text-sm leading-6 text-zinc-600">
        {render_slot(subtitle)}
      </p>
    </header>
    """
  end

  @doc """
  Renders flash notices.
  """
  attr :id, :string, doc: "the optional id of flash container"
  attr :flash, :map, required: true, doc: "the map of flash messages"
  attr :title, :string, default: nil
  attr :kind, :atom, values: [:info, :error], doc: "used for styling and flash lookup"
  attr :rest, :global

  slot :inner_block

  def flash(assigns) do
    assigns = assign_new(assigns, :id, fn -> "flash-#{assigns.kind}" end)

    ~H"""
    <div
      :if={msg = render_slot(@inner_block) || Phoenix.Flash.get(@flash, @kind)}
      id={@id}
      phx-click={JS.push("lv:clear-flash", value: %{key: @kind}) |> hide("##{@id}")}
      role="alert"
      class={[
        "fixed top-2 right-2 mr-2 w-80 sm:w-96 z-50 rounded-lg p-3 ring-1",
        @kind == :info && "bg-emerald-50 text-emerald-800 ring-emerald-500 fill-cyan-900",
        @kind == :error && "bg-rose-50 text-rose-900 ring-rose-500 fill-rose-900"
      ]}
      {@rest}
    >
      <p :if={@title} class="flex items-center gap-1.5 text-sm font-semibold leading-6">
        {@title}
      </p>
      <p class="mt-2 text-sm leading-5">{msg}</p>
      <button type="button" class="absolute top-1 right-1 p-2 group" aria-label="close">
        <span class="text-lg">&times;</span>
      </button>
    </div>
    """
  end

  @doc """
  Shows the flash group with standard titles.
  """
  attr :flash, :map, required: true, doc: "the map of flash messages"
  attr :id, :string, default: "flash-group"

  def flash_group(assigns) do
    ~H"""
    <div id={@id}>
      <.flash kind={:info} title="Success!" flash={@flash} />
      <.flash kind={:error} title="Error!" flash={@flash} />
      <.flash
        id="client-error"
        kind={:error}
        title="Connection lost"
        flash={@flash}
        phx-disconnected={show(".phx-client-error #client-error")}
        phx-connected={hide("#client-error")}
        hidden
      >
        Attempting to reconnect...
      </.flash>
      <.flash
        id="server-error"
        kind={:error}
        title="Something went wrong"
        flash={@flash}
        phx-disconnected={show(".phx-server-error #server-error")}
        phx-connected={hide("#server-error")}
        hidden
      >
        Please reload the page.
      </.flash>
    </div>
    """
  end

  @doc """
  Renders a data table.
  """
  attr :id, :string, required: true
  attr :rows, :list, required: true
  attr :row_id, :any, default: nil, doc: "the function for generating the row id"
  attr :row_click, :any, default: nil, doc: "the function for handling phx-click on each row"

  slot :col, required: true do
    attr :label, :string
  end

  def table(assigns) do
    assigns =
      with %{rows: %Phoenix.LiveView.LiveStream{}} <- assigns do
        assign(assigns, row_id: assigns.row_id || fn {id, _item} -> id end)
      end

    ~H"""
    <div class="overflow-y-auto px-4 sm:overflow-visible sm:px-0">
      <table class="w-full mt-4">
        <thead class="text-sm text-left leading-6 text-zinc-500">
          <tr>
            <th :for={col <- @col} class="p-0 pb-4 pr-6 font-normal">{col[:label]}</th>
          </tr>
        </thead>
        <tbody
          id={@id}
          phx-update={match?(%Phoenix.LiveView.LiveStream{}, @rows) && "stream"}
          class="relative divide-y divide-zinc-100 border-t border-zinc-200 text-sm leading-6 text-zinc-700"
        >
          <tr :for={row <- @rows} id={@row_id && @row_id.(row)} class="group hover:bg-zinc-50">
            <td
              :for={col <- @col}
              phx-click={@row_click && @row_click.(row)}
              class={["relative p-0", @row_click && "hover:cursor-pointer"]}
            >
              <div class="block py-4 pr-6">
                <span class="absolute -inset-y-px right-0 -left-4 group-hover:bg-zinc-50 sm:rounded-l-xl" />
                <span class="relative">
                  {render_slot(col, @row_id && @row_id.(row))}
                </span>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    """
  end

  @doc """
  Renders a back navigation link.
  """
  attr :navigate, :any, required: true

  slot :inner_block

  def back(assigns) do
    ~H"""
    <div class="mt-8">
      <.link navigate={@navigate} class="text-sm font-semibold leading-6 text-zinc-900 hover:text-zinc-700">
        &larr; {render_slot(@inner_block)}
      </.link>
    </div>
    """
  end

  @doc """
  Renders a button.
  """
  attr :type, :string, default: nil
  attr :class, :string, default: nil
  attr :rest, :global, include: ~w(disabled form name value)

  slot :inner_block, required: true

  def button(assigns) do
    ~H"""
    <button
      type={@type}
      class={[
        "phx-submit-loading:opacity-75 rounded-lg bg-zinc-900 hover:bg-zinc-700 py-2 px-3",
        "text-sm font-semibold leading-6 text-white active:text-white/80",
        @class
      ]}
      {@rest}
    >
      {render_slot(@inner_block)}
    </button>
    """
  end

  ## JS Commands

  def show(js \\ %JS{}, selector) do
    JS.show(js,
      to: selector,
      time: 300,
      transition:
        {"transition-all transform ease-out duration-300",
         "opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95",
         "opacity-100 translate-y-0 sm:scale-100"}
    )
  end

  def hide(js \\ %JS{}, selector) do
    JS.hide(js,
      to: selector,
      time: 200,
      transition:
        {"transition-all transform ease-in duration-200",
         "opacity-100 translate-y-0 sm:scale-100",
         "opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"}
    )
  end
end
