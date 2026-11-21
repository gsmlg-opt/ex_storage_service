import "phoenix_html";
import { Socket } from "phoenix";
import * as DuskmoonHooks from "duskmoon_phoenix/hooks";
import { LiveSocket } from "phoenix_live_view";

let csrfToken = document.querySelector("meta[name='csrf-token']").getAttribute("content")
let liveSocket = new LiveSocket("/live", Socket, {
  longPollFallbackMs: 2500,
  params: { _csrf_token: csrfToken },
  hooks: { ...DuskmoonHooks }
})

liveSocket.connect()
window.liveSocket = liveSocket
