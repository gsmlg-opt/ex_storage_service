defmodule ExStorageServiceWeb.CoreComponents do
  @moduledoc """
  Provides core UI components built on duskmoon design system.
  """
  use Phoenix.Component
  use PhoenixDuskmoon.Component

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
        <h1 class="text-2xl font-semibold leading-8 text-on-surface">
          {render_slot(@inner_block)}
        </h1>
        <div :if={@actions != []} class="flex items-center gap-4">
          {render_slot(@actions)}
        </div>
      </div>
      <p :for={subtitle <- @subtitle} class="text-sm leading-6 text-on-surface-variant">
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
        "fixed top-2 right-2 mr-2 w-80 sm:w-96 z-50 rounded-lg p-3",
        @kind == :info && "alert alert-success",
        @kind == :error && "alert alert-error"
      ]}
      {@rest}
    >
      <p :if={@title} class="flex items-center gap-1.5 text-sm font-semibold leading-6">
        {@title}
      </p>
      <p class="mt-2 text-sm leading-5">{msg}</p>
      <button type="button" class="absolute top-1 right-1 p-2" aria-label="close">
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
  attr :row_id, :any, default: nil
  attr :row_click, :any, default: nil

  slot :col, required: true do
    attr :label, :string
  end

  def table(assigns) do
    assigns =
      with %{rows: %Phoenix.LiveView.LiveStream{}} <- assigns do
        assign(assigns, row_id: assigns.row_id || fn {id, _item} -> id end)
      end

    ~H"""
    <div class="overflow-x-auto">
      <table class="table table-hover w-full">
        <thead>
          <tr>
            <th :for={col <- @col} class="text-on-surface-variant">{col[:label]}</th>
          </tr>
        </thead>
        <tbody
          id={@id}
          phx-update={match?(%Phoenix.LiveView.LiveStream{}, @rows) && "stream"}
        >
          <tr :for={row <- @rows} id={@row_id && @row_id.(row)}>
            <td
              :for={col <- @col}
              phx-click={@row_click && @row_click.(row)}
              class={[@row_click && "cursor-pointer"]}
            >
              {render_slot(col, @row_id && @row_id.(row))}
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
      <.dm_link navigate={@navigate} class="text-sm font-semibold text-on-surface">
        &larr; {render_slot(@inner_block)}
      </.dm_link>
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
      class={["btn btn-primary", @class]}
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
