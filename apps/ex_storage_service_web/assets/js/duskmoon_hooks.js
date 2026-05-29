/**
 * Phoenix Duskmoon UI v9 Hooks (local bundle)
 *
 * Bundled locally to work around Volt's inability to vendor-prebundle
 * symlinked Elixir deps in umbrella projects (OXC bundler rejects
 * entries that resolve outside its cwd via symlinks).
 *
 * Source: deps/phoenix_duskmoon/assets/js/hooks/ + phoenix_duskmoon.js
 */

// ── DM Events ────────────────────────────────────────────────────────

const DM_EVENTS = [
  "dm-click", "dm-change", "dm-input", "dm-focus", "dm-blur",
  "dm-submit", "dm-select", "dm-close", "dm-open", "dm-toggle",
];

// ── WebComponentHook ─────────────────────────────────────────────────

export const WebComponentHook = {
  mounted() {
    this._sendListeners = [];
    this._setupEventBridging();
    this._setupAutomaticForwarding();
  },

  updated() {
    this._setupAutomaticForwarding();
  },

  destroyed() {
    if (this._sendListeners) {
      this._sendListeners.forEach(({ event, listener }) => {
        this.el.removeEventListener(event, listener);
      });
      this._sendListeners = null;
    }
    DM_EVENTS.forEach((dmEvent) => {
      const listenerKey = `_dm_listener_${dmEvent}`;
      if (this[listenerKey]) {
        this.el.removeEventListener(dmEvent, this[listenerKey]);
        this[listenerKey] = null;
      }
    });
  },

  _setupEventBridging() {
    const attrs = this.el.attributes;
    const phxTarget = this.el.getAttribute("phx-target");

    const pushEvent = (event, payload, callback) => {
      if (phxTarget) {
        this.pushEventTo(phxTarget, event, payload, callback);
      } else {
        this.pushEvent(event, payload, callback);
      }
    };

    for (let i = 0; i < attrs.length; i++) {
      const attr = attrs[i];

      if (/^duskmoon-send-/.test(attr.name)) {
        const eventName = attr.name.replace(/^duskmoon-send-/, "");
        const [phxEvent, callbackName] = attr.value.split(";");

        const listener = ({ detail }) => {
          pushEvent(phxEvent, detail || {}, (response) => {
            if (callbackName && typeof this[callbackName] === "function") {
              this[callbackName](response, detail, eventName);
            }
          });
        };
        this.el.addEventListener(eventName, listener);
        this._sendListeners.push({ event: eventName, listener });
      }

      if (/^duskmoon-receive-/.test(attr.name)) {
        const eventName = attr.name.replace(/^duskmoon-receive-/, "");
        const handler = attr.value;

        this.handleEvent(eventName, (payload) => {
          if (handler && typeof this.el[handler] === "function") {
            this.el[handler](payload);
          } else {
            this.el.dispatchEvent(
              new CustomEvent(eventName, {
                detail: payload,
                bubbles: true,
                composed: true,
              })
            );
          }
        });
      }

      if (attr.name === "duskmoon-receive") {
        const [phxEvent, callbackName] = attr.value.split(";");
        this.handleEvent(phxEvent, (payload) => {
          if (typeof this.el[callbackName] === "function") {
            this.el[callbackName](payload);
          }
        });
      }
    }
  },

  _setupAutomaticForwarding() {
    const phxTarget = this.el.getAttribute("phx-target");

    DM_EVENTS.forEach((dmEvent) => {
      const phxAttr = "phx-" + dmEvent.replace("dm-", "");
      const phxEvent = this.el.getAttribute(phxAttr);

      if (phxEvent) {
        const listenerKey = `_dm_listener_${dmEvent}`;
        if (this[listenerKey]) {
          this.el.removeEventListener(dmEvent, this[listenerKey]);
        }

        this[listenerKey] = (e) => {
          const detail = e.detail || {};
          const payload = {
            ...detail,
            value: this.el.value,
            name: this.el.name,
            checked: this.el.checked,
          };

          if (phxTarget) {
            this.pushEventTo(phxTarget, phxEvent, payload);
          } else {
            this.pushEvent(phxEvent, payload);
          }
        };

        this.el.addEventListener(dmEvent, this[listenerKey]);
      }
    });
  },
};

// ── FormElementHook ──────────────────────────────────────────────────

export const FormElementHook = {
  ...WebComponentHook,

  mounted() {
    WebComponentHook.mounted.call(this);
    this._setupFormIntegration();
  },

  _setupFormIntegration() {
    const name = this.el.getAttribute("name");
    const feedbackFor = this.el.getAttribute("phx-feedback-for") || name;

    if (feedbackFor) {
      this._setupFeedbackObserver(feedbackFor);
    }
  },

  destroyed() {
    WebComponentHook.destroyed.call(this);
    if (this._feedbackObserver) {
      this._feedbackObserver.disconnect();
      this._feedbackObserver = null;
    }
  },

  _setupFeedbackObserver(feedbackFor) {
    const form = this.el.closest("form");
    if (!form) return;

    this._feedbackObserver = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        if (mutation.attributeName === "class") {
          const hasNoFeedback = form.classList.contains("phx-no-feedback");
          this.el.dispatchEvent(
            new CustomEvent("dm-feedback-change", {
              detail: { showFeedback: !hasNoFeedback, field: feedbackFor },
            })
          );
        }
      });
    });

    this._feedbackObserver.observe(form, { attributes: true, attributeFilter: ["class"] });
  },
};

// ── ThemeSwitcher ────────────────────────────────────────────────────

const darkQuery = window.matchMedia("(prefers-color-scheme: dark)");

function resolveAutoTheme() {
  return darkQuery.matches ? "moonlight" : "sunshine";
}

function applyTheme(theme) {
  const resolved = (!theme || theme === "default") ? resolveAutoTheme() : theme;
  document.documentElement.setAttribute("data-theme", resolved);
}

export const ThemeSwitcher = {
  mounted() {
    const serverTheme = this.el.dataset.theme || "";
    let theme = serverTheme || localStorage.getItem("theme") || "default";

    applyTheme(theme);

    const themeInputs = this.el.querySelectorAll(".theme-controller-item");

    themeInputs.forEach(input => {
      input.checked = theme === input.value;
    });

    this._mediaListener = () => {
      const current = localStorage.getItem("theme") || "default";
      if (current === "default") applyTheme("default");
    };
    darkQuery.addEventListener("change", this._mediaListener);

    this._changeListeners = [];
    themeInputs.forEach(input => {
      const listener = (event) => {
        theme = event.target.value;
        requestAnimationFrame(() => {
          applyTheme(theme);
          localStorage.setItem("theme", theme);
          this.pushEvent("theme_changed", { theme: theme });
          this.el.removeAttribute("open");
        });
      };
      input.addEventListener("change", listener);
      this._changeListeners.push({ element: input, listener });
    });
  },

  updated() {
    const serverTheme = this.el.dataset.theme;
    if (serverTheme) {
      applyTheme(serverTheme);
      const themeInputs = this.el.querySelectorAll(".theme-controller-item");
      themeInputs.forEach(input => {
        input.checked = serverTheme === input.value;
      });
    }
  },

  destroyed() {
    if (this._changeListeners) {
      this._changeListeners.forEach(({ element, listener }) => {
        element.removeEventListener("change", listener);
      });
      this._changeListeners = null;
    }
    if (this._mediaListener) {
      darkQuery.removeEventListener("change", this._mediaListener);
      this._mediaListener = null;
    }
  }
};

// ── Spotlight ────────────────────────────────────────────────────────

export const Spotlight = {
  mounted() {
    this.handleKeyDown = this.handleKeyDown.bind(this);
    this.handleEscape = this.handleEscape.bind(this);
    window.addEventListener('keydown', this.handleKeyDown);
  },

  handleKeyDown(evt) {
    if ((evt.metaKey || evt.ctrlKey) && evt.code === 'KeyK') {
      evt.preventDefault();
      this.el.showModal();
      window.removeEventListener('keydown', this.handleEscape);
      window.addEventListener('keydown', this.handleEscape);
    }
  },

  handleEscape(escEvt) {
    if (escEvt.code === 'Escape') {
      this.el.close();
      window.removeEventListener('keydown', this.handleEscape);
    }
  },

  destroyed() {
    window.removeEventListener('keydown', this.handleKeyDown);
    window.removeEventListener('keydown', this.handleEscape);
  }
};

// ── PageHeader ───────────────────────────────────────────────────────

export const PageHeader = {
  mounted() {
    if (this.el._dmPageHeaderObserved) return;
    this.el._dmPageHeaderObserved = true;

    const navId = this.el.dataset.navId;
    const navEl = document.getElementById(navId);

    if (!navEl) {
      console.warn(`PageHeader hook: Nav element with id "${navId}" not found`);
      return;
    }

    const thresholds = [];
    for (let i = 0; i <= 10; i++) {
      thresholds.push(i / 10);
    }

    const options = {
      root: null,
      rootMargin: "0px",
      threshold: thresholds,
    };

    this.observer = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        const intersectionRatio = entry.intersectionRatio;

        if (intersectionRatio <= 0.5) {
          navEl.classList.remove('hidden');
          navEl.setAttribute('aria-hidden', 'false');
          navEl.style.opacity = 1 - intersectionRatio;
        } else {
          navEl.classList.add('hidden');
          navEl.setAttribute('aria-hidden', 'true');
        }
      });
    }, options);

    this.observer.observe(this.el);
  },

  destroyed() {
    if (this.observer) {
      this.observer.disconnect();
    }
  }
};
