import "phoenix_html"
// NOTE: Do NOT import @duskmoon-dev/elements/register here.
// That package overrides window.dispatchEvent which crashes Phoenix LiveSocket.
// See: https://github.com/gsmlg-dev/duskmoon-dev/issues (label: internal request)
import {Socket} from "phoenix"
import {LiveSocket} from "phoenix_live_view"
import * as DuskmoonHooks from "phoenix_duskmoon/hooks"

// CloseModal hook: closes an el-dm-dialog when the server pushes a named event.
// Usage: phx-hook="CloseModal" data-modal-event="my-event" data-modal-id="my-dialog-id"
const CloseModal = {
  mounted() {
    const eventName = this.el.dataset.modalEvent
    const modalId = this.el.dataset.modalId

    if (!eventName || !modalId) return

    this.handleEvent(eventName, () => {
      const dialog = document.getElementById(modalId)
      if (dialog && typeof dialog.close === "function") {
        dialog.close()
      }
    })
  }
}

let csrfToken = document.querySelector("meta[name='csrf-token']").getAttribute("content")
let liveSocket = new LiveSocket("/live", Socket, {
  longPollFallbackMs: 2500,
  params: {_csrf_token: csrfToken},
  hooks: {...DuskmoonHooks, CloseModal}
})

liveSocket.connect()
window.liveSocket = liveSocket
