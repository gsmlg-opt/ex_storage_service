import "phoenix_html";
import { Socket } from "phoenix";
import * as DuskmoonHooks from "phoenix_duskmoon/hooks";
import { LiveSocket } from "phoenix_live_view";

// NOTE: Do NOT import @duskmoon-dev/elements/register here.
// When bundled with Bun, it throws:
//   ReferenceError: Cannot access 'ElDmCodeEngine' before initialization
// due to a circular dependency in the package. This crashes ALL element
// registration, not just code-engine.
// Issue filed: https://github.com/gsmlg-dev/duskmoon-dev/issues
//
// This app currently only uses dm_link, dm_mdi, and dm_table components
// which render plain HTML and don't require custom element registration.

let csrfToken = document.querySelector("meta[name='csrf-token']").getAttribute("content")
let liveSocket = new LiveSocket("/live", Socket, {
  longPollFallbackMs: 2500,
  params: { _csrf_token: csrfToken },
  hooks: { ...DuskmoonHooks }
})

liveSocket.connect()
window.liveSocket = liveSocket
