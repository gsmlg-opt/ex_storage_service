defmodule ExStorageServiceWeb.AppComponents do
  @moduledoc """
  Application-specific UI components that complement phoenix_duskmoon.
  """
  use Phoenix.Component

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
end
