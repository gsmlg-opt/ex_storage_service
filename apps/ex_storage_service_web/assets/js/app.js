import "phoenix_html";
import { Socket } from "phoenix";
import * as DuskmoonHooks from "./duskmoon_hooks.js";
import { LiveSocket } from "phoenix_live_view";

// @duskmoon-dev/elements — pre-bundled by `mix duskmoon.bundle`
import "./duskmoon_elements.js";

let csrfToken = document.querySelector("meta[name='csrf-token']").getAttribute("content")
let liveSocket = new LiveSocket("/live", Socket, {
  longPollFallbackMs: 2500,
  params: { _csrf_token: csrfToken },
  hooks: { ...DuskmoonHooks }
})

liveSocket.connect()
window.liveSocket = liveSocket
