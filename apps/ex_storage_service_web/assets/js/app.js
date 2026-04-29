import "phoenix_html"
// WORKAROUND(upstream): duskmoon-dev/phoenix-duskmoon-ui#28
// Avoid @duskmoon-dev/elements/register until it stops overwriting window.dispatchEvent.
import {Socket} from "phoenix"
import {LiveSocket} from "phoenix_live_view"
import * as DuskmoonHooks from "phoenix_duskmoon/hooks"

let csrfToken = document.querySelector("meta[name='csrf-token']").getAttribute("content")
let liveSocket = new LiveSocket("/live", Socket, {
  longPollFallbackMs: 2500,
  params: {_csrf_token: csrfToken},
  hooks: DuskmoonHooks
})

liveSocket.connect()
window.liveSocket = liveSocket
