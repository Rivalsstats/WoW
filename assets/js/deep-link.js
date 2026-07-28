/* Deep links & share permalinks — loaded on every page via javascript_imports.html.
 *
 * Everything worth pointing a friend at on MythiStone lives behind a modal, an
 * accordion or a tab (the per-dungeon talent diffs, the routes modal, the set
 * combos, the tier list target tabs). None of that used to survive a copy of the
 * address bar. This module gives all of it a URL:
 *
 *   /classes/Healer/Mistweaver_Monk#dungeonCollapse-560
 *   /classes/Healer/Mistweaver_Monk#dungeonCollapse-560&hero=31
 *   /dungeons/ara-kara-city-of-echoes#panel-shortest
 *
 * Grammar: "#<targetId>" optionally followed by "&key=value" pairs. The target is
 * an element id (or a data-share-id, for panels whose id embeds a volatile run
 * id). The key/value pairs carry state that is not an ancestor container — the
 * spec page's hero tree, the comps modal's selection — and are supplied by the
 * pages themselves through registerState().
 *
 * Three jobs:
 *   1. resolve — on load and on hashchange, open every container hiding the
 *      target, then scroll to it and flash it.
 *   2. sync    — as the user opens/closes panels, rewrite the hash with
 *      replaceState so the address bar is always copy-pasteable. replaceState,
 *      not pushState, matches items.js/route-search.js: Back leaves the page.
 *   3. share   — inject a copy-link button into every accordion and modal header
 *      whose panel is addressable, so no template has to carry the markup.
 *
 * Revealing goes through the Bootstrap API, never classList.add('show'): the
 * spec page hydrates per-dungeon talent trees on show.bs.collapse and sets the
 * keystone.guru iframe src on shown.bs.collapse, so a link that skipped the
 * events would open an empty panel.
 */
(function () {
  "use strict";

  // Site chrome that shares the collapse markup but is not page content. The
  // sidenav's role accordion in particular uses collapse-Tank|Healer|Dps, which
  // collides with the spec page's collapse-<gearSlot> namespace.
  var EXCLUDED_ANCESTORS = ".sidenav, .navbar, .fixed-plugin, aside, .dropdown-menu";
  var FLASH_MS = 2000;
  var COPIED_MS = 2000;
  // Safety net: if a shown.bs.* event never arrives (element removed, Bootstrap
  // missing), the chain must still finish instead of hanging the resolve.
  var REVEAL_TIMEOUT_MS = 800;
  // Re-assert the scroll position at these delays while late images shift layout.
  var SETTLE_DELAYS_MS = [300, 900, 1800];

  var states = {};      // key -> { read, apply }
  var revealers = [];   // { match, show } for containers Bootstrap doesn't own
  var currentTarget = null;
  var suspendSync = false;

  // ---------------------------------------------------------------- helpers

  function shareId(el) {
    if (!el || el.nodeType !== 1) return "";
    return el.getAttribute("data-share-id") || el.id || "";
  }

  // Addressable = has a stable handle, is page content, and hasn't opted out.
  function isTrackable(el) {
    if (!el || el.nodeType !== 1) return false;
    if (!shareId(el)) return false;
    if (el.hasAttribute("data-no-deep-link")) return false;
    if (el.closest && el.closest(EXCLUDED_ANCESTORS)) return false;
    return true;
  }

  function isBootstrapContainer(el) {
    return el.classList.contains("modal") ||
      el.classList.contains("collapse") ||
      el.classList.contains("tab-pane");
  }

  function customRevealer(el) {
    for (var i = 0; i < revealers.length; i++) {
      try {
        if (revealers[i].match(el)) return revealers[i];
      } catch (e) {
        console.error("deep-link: revealer match failed", e);
      }
    }
    return null;
  }

  function isContainer(el) {
    return isBootstrapContainer(el) || !!customRevealer(el);
  }

  function isOpen(el) {
    if (el.classList.contains("tab-pane")) return el.classList.contains("active");
    if (isBootstrapContainer(el)) return el.classList.contains("show");
    var r = customRevealer(el);
    return r && typeof r.isOpen === "function" ? !!r.isOpen(el) : true;
  }

  // The <button>/<a> that owns a tab pane. Panes carry aria-labelledby pointing
  // at their trigger (see simc_tierlist.html / dashboard.html); the data-bs-target
  // lookup is the fallback for panes that don't.
  function triggerForPane(pane) {
    var labelledBy = pane.getAttribute("aria-labelledby");
    if (labelledBy) {
      var byLabel = document.getElementById(labelledBy);
      if (byLabel) return byLabel;
    }
    if (!pane.id) return null;
    return document.querySelector(
      '[data-bs-toggle="tab"][data-bs-target="#' + cssEscape(pane.id) + '"],' +
      '[data-bs-toggle="pill"][data-bs-target="#' + cssEscape(pane.id) + '"],' +
      '[data-bs-toggle="tab"][href="#' + cssEscape(pane.id) + '"]'
    );
  }

  function cssEscape(value) {
    if (window.CSS && typeof CSS.escape === "function") return CSS.escape(value);
    return String(value).replace(/["\\\]]/g, "\\$&");
  }

  // ------------------------------------------------------------ hash format

  function parseHash() {
    var raw = window.location.hash.replace(/^#/, "");
    var out = { target: "", state: {} };
    if (!raw) return out;
    raw.split("&").forEach(function (part, i) {
      if (!part) return;
      var eq = part.indexOf("=");
      if (eq === -1) {
        // Only the first bare segment is the target; later ones are malformed
        // and simply ignored rather than throwing on user-typed input.
        if (i === 0) out.target = decode(part);
        return;
      }
      out.state[decode(part.slice(0, eq))] = decode(part.slice(eq + 1));
    });
    return out;
  }

  function decode(s) {
    try { return decodeURIComponent(s); } catch (e) { return s; }
  }

  // encodeURIComponent, minus the escaping of characters that are perfectly legal
  // in a fragment and far more readable unescaped. Only & = # % and whitespace
  // actually matter to the grammar; comma/pipe/colon show up in list values.
  function encodeSegment(value) {
    return encodeURIComponent(value)
      .replace(/%2C/gi, ",")
      .replace(/%7C/gi, "|")
      .replace(/%3A/gi, ":");
  }

  function formatHash(target, state) {
    var parts = [];
    if (target) parts.push(encodeSegment(target));
    Object.keys(state).forEach(function (k) {
      parts.push(encodeSegment(k) + "=" + encodeSegment(state[k]));
    });
    return parts.join("&");
  }

  // Registered state that differs from the page default. Anything at its default
  // is omitted so an untouched page keeps a bare URL.
  function readStates() {
    var out = {};
    Object.keys(states).forEach(function (key) {
      var value;
      try {
        value = states[key].read();
      } catch (e) {
        console.error("deep-link: could not read state '" + key + "'", e);
        return;
      }
      if (value === null || value === undefined || value === "") return;
      out[key] = String(value);
    });
    return out;
  }

  function applyStates(state) {
    Object.keys(states).forEach(function (key) {
      if (!Object.prototype.hasOwnProperty.call(state, key)) return;
      try {
        states[key].apply(state[key]);
      } catch (e) {
        console.error("deep-link: could not apply state '" + key + "'", e);
      }
    });
  }

  // ---------------------------------------------------------------- resolve

  function findTarget(id) {
    if (!id) return null;
    var el = null;
    try { el = document.getElementById(id); } catch (e) { el = null; }
    if (el) return el;
    try {
      el = document.querySelector('[data-share-id="' + cssEscape(id) + '"]');
    } catch (e) {
      el = null;
    }
    return el;
  }

  // Outermost container first, so a collapse inside a modal is only opened once
  // the modal has laid out (the routes iframe sizes itself on shown.bs.collapse).
  function containerChain(el) {
    var chain = [];
    var node = el;
    while (node && node !== document.body) {
      if (isContainer(node)) chain.push(node);
      node = node.parentElement;
    }
    return chain.reverse();
  }

  // A modal blocks the page behind it, so navigating to a target outside it (the
  // in-page hash links, or a hashchange) has to dismiss it first — otherwise the
  // visitor lands on a section they cannot see or scroll to.
  // Resolves once they are fully hidden: an in-flight modal still holds
  // overflow:hidden on <body>, which would silently swallow the scroll.
  function closeStaleModals(chain) {
    if (typeof bootstrap === "undefined" || !bootstrap.Modal) return Promise.resolve();
    var waits = [];
    document.querySelectorAll(".modal.show").forEach(function (modal) {
      if (chain.indexOf(modal) !== -1) return;
      waits.push(new Promise(function (resolve) {
        var timer = setTimeout(resolve, REVEAL_TIMEOUT_MS);
        modal.addEventListener("hidden.bs.modal", function () {
          clearTimeout(timer);
          resolve();
        }, { once: true });
        bootstrap.Modal.getOrCreateInstance(modal).hide();
      }));
    });
    return Promise.all(waits);
  }

  function reveal(el) {
    return new Promise(function (resolve) {
      var custom = customRevealer(el);
      if (custom) {
        try { custom.show(el); } catch (e) { console.error("deep-link: revealer failed", e); }
        resolve();
        return;
      }
      if (isOpen(el) || typeof bootstrap === "undefined") {
        resolve();
        return;
      }

      var settled = false;
      var timer = setTimeout(done, REVEAL_TIMEOUT_MS);
      function done() {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve();
      }

      if (el.classList.contains("modal")) {
        el.addEventListener("shown.bs.modal", done, { once: true });
        bootstrap.Modal.getOrCreateInstance(el).show();
        return;
      }
      if (el.classList.contains("tab-pane")) {
        var trigger = triggerForPane(el);
        if (!trigger || !bootstrap.Tab) { done(); return; }
        trigger.addEventListener("shown.bs.tab", done, { once: true });
        bootstrap.Tab.getOrCreateInstance(trigger).show();
        return;
      }
      el.addEventListener("shown.bs.collapse", done, { once: true });
      // toggle:false — Collapse's constructor toggles on instantiation by
      // default, which would fight the explicit show() below.
      bootstrap.Collapse.getOrCreateInstance(el, { toggle: false }).show();
    });
  }

  function headerFor(el) {
    var labelledBy = el.getAttribute("aria-labelledby");
    if (labelledBy) {
      var header = document.getElementById(labelledBy);
      if (header) return header;
    }
    return null;
  }

  // These pages carry hundreds of un-sized icons, so everything above the target
  // keeps moving as they arrive: a single scroll on arrival lands hundreds of
  // pixels off. Re-assert the position while the page settles, and stop the
  // moment the visitor touches the scroll themselves.
  function settleInView(focal) {
    var cancelled = false;
    function cancel() { cancelled = true; }
    ["wheel", "touchstart", "keydown", "mousedown"].forEach(function (evt) {
      window.addEventListener(evt, cancel, { once: true, passive: true });
    });
    function settle() {
      if (cancelled) return;
      focal.scrollIntoView({ behavior: "instant", block: "start" });
    }
    if (document.readyState !== "complete") {
      window.addEventListener("load", settle, { once: true });
    }
    SETTLE_DELAYS_MS.forEach(function (ms) { setTimeout(settle, ms); });
  }

  function highlight(el) {
    var focal = headerFor(el) || el;
    // "instant", not "smooth"/"auto": <html> carries scroll-behavior: smooth, and
    // an animated scroll on these pages is cancelled the moment anything above
    // the target reflows — which, with this many un-sized icons, is immediately.
    focal.scrollIntoView({ behavior: "instant", block: "start" });
    if (document.readyState !== "complete") settleInView(focal);
    focal.classList.add("deep-link-flash");
    setTimeout(function () { focal.classList.remove("deep-link-flash"); }, FLASH_MS);
    var focusable = focal.querySelector("button, a[href], [tabindex]");
    if (focusable && focusable.focus) focusable.focus({ preventScroll: true });
  }

  function resolveHash() {
    var parsed = parseHash();
    // Applied before the chain because a state handler may itself be what builds
    // the target (the comps modal is populated from its state), and again after
    // in case revealing replaced the content. Handlers must be idempotent.
    applyStates(parsed.state);
    if (!parsed.target) {
      syncUrl();
      return;
    }
    var el = findTarget(parsed.target);
    if (!el) {
      // Unknown fragment: could be a hand-edited link or a stale route panel.
      // Leave the URL as-is and do nothing rather than jumping the page.
      return;
    }
    // Each reveal fires shown.bs.*, and each of those would rewrite the URL with
    // a half-open chain. Hold sync off until the whole chain has settled.
    suspendSync = true;
    var chain = containerChain(el);
    var step = closeStaleModals(chain);
    chain.forEach(function (container) {
      step = step.then(function () { return reveal(container); });
    });
    step.then(function () {
      applyStates(parsed.state);
      currentTarget = isTrackable(el) ? el : nearestOpenAncestor(el);
      highlight(el);
    }).catch(function (err) {
      console.error("deep-link: could not resolve " + parsed.target, err);
    }).then(function () {
      suspendSync = false;
      syncUrl();
    });
  }

  // -------------------------------------------------------------------- sync

  function nearestOpenAncestor(node) {
    var current = node;
    while (current && current !== document.body) {
      if (isContainer(current) && isTrackable(current) && isOpen(current)) return current;
      current = current.parentElement;
    }
    return null;
  }

  function syncUrl() {
    if (suspendSync) return;
    var target = currentTarget && document.contains(currentTarget) ? shareId(currentTarget) : "";
    var hash = formatHash(target, readStates());
    var url = window.location.pathname + window.location.search + (hash ? "#" + hash : "");
    window.history.replaceState(null, "", url);
  }

  function onShown(el) {
    if (!isTrackable(el)) return;
    // Deepest wins: an inner panel's shown.bs.* fires after its container's.
    currentTarget = el;
    syncUrl();
  }

  function onHidden(el) {
    if (!currentTarget) return;
    if (el !== currentTarget && !el.contains(currentTarget)) return;
    currentTarget = nearestOpenAncestor(el.parentElement);
    syncUrl();
  }

  function bindSync() {
    ["shown.bs.collapse", "shown.bs.modal"].forEach(function (evt) {
      document.addEventListener(evt, function (e) { onShown(e.target); });
    });
    ["hidden.bs.collapse", "hidden.bs.modal"].forEach(function (evt) {
      document.addEventListener(evt, function (e) { onHidden(e.target); });
    });
    // shown.bs.tab fires on the trigger, not the pane.
    document.addEventListener("shown.bs.tab", function (e) {
      var selector = e.target.getAttribute("data-bs-target") || e.target.getAttribute("href");
      if (!selector) return;
      var pane = null;
      try { pane = document.querySelector(selector); } catch (err) { pane = null; }
      if (pane) onShown(pane);
    });
  }

  // ------------------------------------------------------------------ share

  function permalinkFor(el) {
    var hash = formatHash(shareId(el), readStates());
    return window.location.origin + window.location.pathname +
      window.location.search + (hash ? "#" + hash : "");
  }

  function makeCopyButton(getUrl, label) {
    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "deep-link-copy";
    btn.title = label;
    btn.setAttribute("aria-label", label);
    var icon = document.createElement("i");
    icon.className = "bi bi-link-45deg";
    icon.setAttribute("aria-hidden", "true");
    btn.appendChild(icon);

    // Same clipboard + 2s feedback shape as the Export Talent String and
    // "Copy full build" buttons on the spec page.
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      var url = getUrl();
      Promise.resolve()
        .then(function () { return navigator.clipboard.writeText(url); })
        .then(function () {
          icon.className = "bi bi-check-lg";
          btn.classList.add("deep-link-copied");
        })
        .catch(function (err) {
          console.error("deep-link: copy failed", err);
          icon.className = "bi bi-exclamation-triangle";
          btn.classList.add("deep-link-error");
        })
        .then(function () {
          setTimeout(function () {
            icon.className = "bi bi-link-45deg";
            btn.classList.remove("deep-link-copied");
            btn.classList.remove("deep-link-error");
          }, COPIED_MS);
        });
    });
    return btn;
  }

  function panelForHeader(header) {
    var toggle = header.querySelector('[data-bs-toggle="collapse"]');
    if (!toggle) return null;
    var selector = toggle.getAttribute("data-bs-target") || toggle.getAttribute("href");
    if (!selector) return null;
    try { return document.querySelector(selector); } catch (e) { return null; }
  }

  function shareable(el) {
    return el && isTrackable(el) && !el.hasAttribute("data-no-share");
  }

  function injectShareButtons() {
    // Accordion headers: the button sits as a sibling of .accordion-button, so
    // Bootstrap's delegated collapse handler never sees the click and the panel
    // does not toggle when someone copies its link.
    document.querySelectorAll(".accordion-header").forEach(function (header) {
      if (header.querySelector(".deep-link-copy")) return;
      var panel = panelForHeader(header);
      if (!shareable(panel)) return;
      header.classList.add("deep-link-host");
      header.appendChild(makeCopyButton(function () {
        return permalinkFor(panel);
      }, "Copy link to this section"));
    });

    // Modal headers copy whatever is open inside the modal right now — sync
    // keeps that in the address bar, so the live URL is the right thing to copy.
    document.querySelectorAll(".modal-header").forEach(function (header) {
      if (header.querySelector(".deep-link-copy")) return;
      var modal = header.closest(".modal");
      if (!shareable(modal)) return;
      header.classList.add("deep-link-host");
      var btn = makeCopyButton(function () {
        return currentTarget && document.contains(currentTarget)
          ? permalinkFor(currentTarget)
          : permalinkFor(modal);
      }, "Copy link to this view");
      var close = header.querySelector(".btn-close");
      header.insertBefore(btn, close || null);
    });

    // Opt-in for plain sections: data-share on any element with an id puts the
    // same control in its .card-header (or in the element itself).
    document.querySelectorAll("[data-share]").forEach(function (section) {
      if (!shareable(section)) return;
      var host = section.querySelector(".card-header") || section;
      if (host.querySelector(".deep-link-copy")) return;
      host.classList.add("deep-link-host");
      host.appendChild(makeCopyButton(function () {
        return permalinkFor(section);
      }, "Copy link to this section"));
    });
  }

  // ----------------------------------------------------------------- public

  // Registration happens from each page's own script. This file loads with
  // javascript_imports.html near the end of <body>, so a script block placed
  // *above* that include must register on DOMContentLoaded — boot runs one
  // macrotask later, so anything registered by then is in time.
  window.MythiLink = {
    // Page-supplied state that isn't an ancestor container (hero tree, filters).
    // read() returns null when the value equals the page default.
    registerState: function (key, handlers) {
      states[key] = handlers;
    },
    // Containers Bootstrap doesn't own (the dungeon page's run panels).
    // isOpen is optional and defaults to "already open".
    registerRevealer: function (handlers) {
      revealers.push(handlers);
    },
    // Custom containers have no shown.bs.* event; call this after opening one so
    // the hash follows the user the same way a Bootstrap panel would.
    notifyShown: function (el) {
      onShown(el);
    },
    // Call after changing registered state so the address bar keeps up.
    sync: syncUrl,
    permalinkFor: permalinkFor,
    // Re-run injection after markup is added (route results are re-rendered
    // client-side on every search).
    refresh: injectShareButtons
  };

  // ------------------------------------------------------------------- boot

  function boot() {
    injectShareButtons();
    bindSync();
    resolveHash();
  }

  // One macrotask after DOMContentLoaded, not on it: this file loads before every
  // page's own script block, so booting on the event itself would run ahead of
  // the pages that register their state and build the widgets a link may target
  // (the item page's key-level picker, for one). Deferring costs a tick on a
  // deep-linked load and makes registration order stop mattering.
  function scheduleBoot() { setTimeout(boot, 0); }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", scheduleBoot);
  } else {
    scheduleBoot();
  }

  window.addEventListener("hashchange", function () {
    resolveHash();
  });
})();
