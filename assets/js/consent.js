/* Klaro consent helpers, shared by every page that loads a third-party embed
 * itself instead of letting Klaro do it.
 *
 * Klaro swaps data-src -> src on elements that exist when it initialises. It
 * does NOT pick up elements added later: applyConsents() leaves a freshly
 * inserted iframe untouched even when consent is granted (verified). So pages
 * that render embeds client-side — the route search results, the spec page's
 * lazily loaded route panels — have to set src themselves, and therefore have
 * to check consent themselves. Without that check the embed loads regardless of
 * what the visitor chose, which is exactly what the consent banner promises
 * won't happen.
 *
 * Server-rendered embeds (the dungeon page) need none of this: Klaro sees them
 * at init and manages them on its own.
 */
(function () {
  "use strict";

  var POLL_MS = 500;
  var POLL_ATTEMPTS = 20;

  function manager() {
    try {
      if (!window.klaro || typeof klaro.getManager !== "function") return null;
      return klaro.getManager();
    } catch (e) {
      // Klaro is loaded with defer and initialises asynchronously, so this is
      // "not ready yet" rather than an error.
      return null;
    }
  }

  function granted(name) {
    try {
      var mgr = manager();
      if (mgr && typeof mgr.getConsent === "function") return !!mgr.getConsent(name);
      if (window.klaro && klaro.consents &&
        Object.prototype.hasOwnProperty.call(klaro.consents, name)) {
        return !!klaro.consents[name];
      }
    } catch (e) { /* treat an unreadable manager as "no consent" */ }
    return false;
  }

  // Runs cb() once consent for `name` is granted — immediately if it already is,
  // exactly once otherwise.
  //
  // The watcher and the poll both run, deliberately. Klaro's watcher payload
  // shape varies between versions, so a watcher that silently never matches is a
  // real possibility (it was: granting consent with a route panel already open
  // left the embed unloaded until a reload). And the poll alone isn't enough
  // either — a deep link can open a panel before Klaro has initialised, so there
  // is no manager to watch yet. Whichever notices first wins.
  function whenGranted(name, cb) {
    if (granted(name)) {
      cb();
      return;
    }

    var done = false;
    function fire() {
      if (done) return;
      done = true;
      cb();
    }

    var mgr = manager();
    if (mgr && typeof mgr.watch === "function") {
      try {
        mgr.watch({
          update: function (obj) {
            var states = obj && (obj.states || obj);
            if ((states && states[name]) || granted(name)) fire();
          }
        });
      } catch (e) { /* the poll below still covers us */ }
    }

    var attempts = 0;
    var id = setInterval(function () {
      attempts++;
      if (done || attempts > POLL_ATTEMPTS) {
        clearInterval(id);
        return;
      }
      if (granted(name)) {
        clearInterval(id);
        fire();
      }
    }, POLL_MS);
  }

  function serviceTitle(name) {
    try {
      var services = manager().config.services || [];
      for (var i = 0; i < services.length; i++) {
        if (services[i].name === name) return services[i].title || name;
      }
    } catch (e) { /* fall back to the raw name */ }
    return name;
  }

  // Klaro renders its own "do you want to load external content" notice for the
  // embeds it knows about. It can't for client-rendered ones, and an embed that
  // silently stays blank just reads as broken — so stand in with an equivalent
  // notice, and only when Klaro hasn't already provided one.
  //
  // Positioned with the same utilities as .iframe-spinner so it centres in the
  // container without needing its own stylesheet.
  function ensureNotice(container, service) {
    if (!container) return;
    if (container.querySelector(".klaro, .context-notice, .mythi-embed-notice")) return;

    var box = document.createElement("div");
    box.className = "mythi-embed-notice position-absolute top-50 start-50 translate-middle text-center px-3";

    var text = document.createElement("p");
    text.className = "text-sm mb-2";
    text.textContent = "External content from " + serviceTitle(service) +
      " is blocked until you allow it.";

    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "btn btn-sm btn-primary mb-0";
    btn.textContent = "Cookie settings";
    btn.addEventListener("click", function () {
      if (window.klaro && typeof klaro.show === "function") klaro.show();
    });

    box.appendChild(text);
    box.appendChild(btn);
    container.appendChild(box);
  }

  function clearNotice(container) {
    if (!container) return;
    var own = container.querySelector(".mythi-embed-notice");
    if (own) own.remove();
  }

  // Klaro stamps every embed it processed at init with data-modified-by-klaro and
  // inserts a data-type="placeholder" sibling holding the contextual notice.
  function klaroManages(el) {
    if (el.hasAttribute("data-modified-by-klaro")) return true;
    var prev = el.previousElementSibling;
    return !!(prev && prev.getAttribute("data-type") === "placeholder");
  }

  // For embeds Klaro owns, drive the spinner and NOTHING else.
  //
  // Klaro re-enables an embed by building a fresh clone and swapping it in —
  // that clone is where it restores `display` from data-original-display. Its
  // updateServiceElements bails early on `consent && element.src === data-src`,
  // so if we set src ourselves first, the swap never happens and the embed loads
  // while staying display:none. That is what made the notice's "Yes" button look
  // broken: the request went out, the iframe just stayed invisible.
  //
  // Because Klaro replaces the node, listeners have to be re-attached to
  // whichever iframe is currently in the container, hence the observer.
  function followKlaro(container, spin) {
    function sync() {
      var el = container.querySelector("iframe[data-src]");
      if (!el) return;
      if (!el.getAttribute("src")) { spin(false); return; }
      // Repair Klaro's accept-once path. Klaro re-enables an embed by swapping in
      // a clone whose display it restores from data-original-display, but for the
      // embed the visitor actually clicked "Yes" on, only the src comes back — the
      // display:none it applied while consent was missing stays. The embed then
      // loads at zero height and the button looks broken. (Every *other* embed on
      // the page, enabled by the same consent pass, comes back correctly.)
      // src is Klaro's decision and we never touch it; this only fixes visibility
      // of something Klaro has already chosen to load.
      if (el.style.display === "none") {
        el.style.display = el.getAttribute("data-original-display") || "block";
      }
      if (el.getAttribute("data-mythi-spinner")) return;
      el.setAttribute("data-mythi-spinner", "1");
      spin(true);
      el.addEventListener("load", function () { spin(false); }, { once: true });
      el.addEventListener("error", function () { spin(false); }, { once: true });
    }
    sync();
    new MutationObserver(sync).observe(container, {
      childList: true, subtree: true, attributes: true, attributeFilter: ["src"]
    });
  }

  // Set an embed's src from its data-src, but only with consent, and own the
  // loading spinner while doing it.
  //
  // The spinner sits in the same .iframe-container as the embed on every page
  // that uses one. It has to stay hidden until a load is actually in flight:
  // without consent nothing loads, Klaro puts its "enable external content"
  // notice where the embed would be, and a spinner left running sits on top of
  // that notice.
  //
  // Reads the src ATTRIBUTE, never the property — the property resolves src=""
  // to the document's own URL, so a property check never sees an unloaded embed.
  function loadEmbed(el, onLoad) {
    if (!el || !el.getAttribute("data-src")) return;

    var container = (el.closest && el.closest(".iframe-container")) || el.parentElement;
    var spinner = container && container.querySelector(".iframe-spinner");
    function spin(on) {
      if (spinner) spinner.classList.toggle("d-none", !on);
    }

    // Server-rendered embeds: hands off the src, Klaro has it.
    if (klaroManages(el)) {
      followKlaro(container, spin);
      return;
    }

    function finish() {
      spin(false);
      if (typeof onLoad === "function") onLoad();
    }

    // Already loading or loaded (e.g. the panel was reopened).
    if (el.getAttribute("src")) {
      el.addEventListener("load", finish, { once: true });
      return;
    }

    spin(false);
    var service = el.getAttribute("data-name") || "";
    function apply() {
      clearNotice(container);
      if (el.getAttribute("src")) return;
      spin(true);
      el.addEventListener("load", finish, { once: true });
      el.addEventListener("error", finish, { once: true });
      el.setAttribute("src", el.getAttribute("data-src"));
    }
    if (!service) { apply(); return; }
    if (!granted(service)) ensureNotice(container, service);
    whenGranted(service, apply);
  }

  window.MythiConsent = {
    granted: granted,
    whenGranted: whenGranted,
    loadEmbed: loadEmbed
  };
})();
