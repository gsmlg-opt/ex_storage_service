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

  @doc """
  A reusable confirm dialog modal using pure LiveView (no Shadow DOM).

  ## Assigns

    * `show` – boolean controlling visibility
    * `title` – dialog title (e.g. "Delete Object")
    * `message` – confirmation message (e.g. "Are you sure?")
    * `confirm_event` – the phx-click event name to fire on confirm
    * `confirm_params` – map of phx-value-* params to pass (default `%{}`)
    * `confirm_label` – label for the confirm button (default "Confirm")
    * `on_cancel` – the phx-click event name for cancel (default "close_confirm_modal")
    * `confirm_style` – button class variant: "error", "warning", "primary" (default "error")
  """
  attr :show, :boolean, required: true
  attr :title, :string, default: "Confirm"
  attr :message, :string, default: "Are you sure?"
  attr :confirm_event, :string, required: true
  attr :confirm_params, :map, default: %{}
  attr :confirm_label, :string, default: "Confirm"
  attr :on_cancel, :string, default: "close_confirm_modal"
  attr :confirm_style, :string, default: "error"

  def confirm_modal(assigns) do
    btn_class =
      case assigns.confirm_style do
        "warning" -> "btn btn-warning btn-sm"
        "primary" -> "btn btn-primary btn-sm"
        _ -> "btn btn-error btn-sm"
      end

    assigns = assign(assigns, :btn_class, btn_class)

    ~H"""
    <div
      :if={@show}
      id="confirm-modal-overlay"
      class="fixed inset-0 z-50 flex items-center justify-center"
      phx-key="Escape"
      phx-window-keydown={@on_cancel}
    >
      <%!-- Backdrop --%>
      <div class="absolute inset-0 bg-black/50 backdrop-blur-sm" phx-click={@on_cancel}></div>

      <%!-- Dialog card --%>
      <div class="relative w-full max-w-sm mx-4 card shadow-2xl">
        <div class="card-body p-6 flex flex-col gap-4">
          <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold">{@title}</h2>
            <button
              type="button"
              class="btn btn-ghost btn-sm btn-circle"
              phx-click={@on_cancel}
              aria-label="Close"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                class="w-4 h-4"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
              >
                <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
              </svg>
            </button>
          </div>

          <p class="text-sm text-on-surface-variant">{@message}</p>

          <div class="flex justify-end gap-2">
            <button type="button" class="btn btn-ghost btn-sm" phx-click={@on_cancel}>
              Cancel
            </button>
            <button
              type="button"
              class={@btn_class}
              phx-click={@confirm_event}
              {confirm_value_attrs(@confirm_params)}
            >
              {@confirm_label}
            </button>
          </div>
        </div>
      </div>
    </div>
    """
  end

  defp confirm_value_attrs(params) when params == %{}, do: %{}

  defp confirm_value_attrs(params) do
    Map.new(params, fn {k, v} -> {"phx-value-#{k}", v} end)
  end
end
