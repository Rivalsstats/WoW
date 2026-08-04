/* Share buttons — the "Share on Socials!" row in the sidenav footer
 * (templates/sidenav.html), loaded on every page via javascript_imports.html.
 *
 * The buttons used to carry hard-coded hrefs pointing at the site root, so a
 * share taken from a spec page advertised the home page. Everything needed to
 * do better is already in the document head of every generated page:
 *
 *   <link rel="canonical">      the public URL of *this* page. Canonical, not
 *                               location.href, because the same page is served
 *                               from the github.io mirror and from a local
 *                               http.server during development — neither of
 *                               which is a URL anyone should be handed.
 *   <meta property="og:title">  the headline the social card renders.
 *   <meta property="og:image">  the 1200x630 preview (backend_scripts/
 *                               renderAllSocialImages.py), which the native
 *                               share sheet can post as a file.
 *
 * The URL is rebuilt immediately before every activation rather than once on
 * load, because deep-link.js rewrites the hash with replaceState as panels open
 * and replaceState fires neither hashchange nor popstate. Refreshing on
 * pointerdown/focus (not just click) keeps middle-click, "open in new tab" and
 * "copy link address" honest too, since those never produce a click event.
 */
(function () {
  "use strict";

  // Per-network handles: the same account, spelled the way each network expects.
  var MENTIONS = {
    x: "@Mythistone",
    bluesky: "@mythistone.bsky.social"
  };
  var COPIED_MS = 2000;

  var enc = encodeURIComponent;

  // ---------------------------------------------------------------- page facts

  function metaContent(selector) {
    var el = document.querySelector(selector);
    if (!el) return "";
    return (el.getAttribute("content") || "").trim();
  }

  // Canonical minus any query/fragment of its own, plus whatever state the
  // visitor has actually opened: ?key=10 filters (items.js, route-search.js) and
  // the #panel fragment deep-link.js keeps in the address bar.
  function pageUrl() {
    var canonical = document.querySelector('link[rel="canonical"]');
    var base = canonical && canonical.href
      ? canonical.href.split("#")[0].split("?")[0]
      : window.location.origin + window.location.pathname;
    return base + window.location.search + window.location.hash;
  }

  // Most og:titles — and every <title> the document.title fallback sees — end in
  // "| MythiStone". Each share text below already names the site, as a handle on
  // X and Bluesky and through the link's own domain on Reddit and Facebook, so
  // the suffix would only say it a second time.
  function pageTitle() {
    var raw = metaContent('meta[property="og:title"]') || document.title;
    return raw.replace(/\s*[|–-]\s*mythistone\s*$/i, "").trim();
  }

  function previewImage() {
    return metaContent('meta[property="og:image"]');
  }

  // ------------------------------------------------------------ intent URLs

  var NETWORKS = {
    x: function (ctx) {
      return "https://twitter.com/intent/tweet?text=" +
        enc(ctx.title + " — via " + MENTIONS.x) + "&url=" + enc(ctx.url);
    },
    // Bluesky's compose intent has no separate url parameter; the link rides in
    // the text, where the client turns it into a link facet.
    bluesky: function (ctx) {
      return "https://bsky.app/intent/compose?text=" +
        enc(ctx.title + " — via " + MENTIONS.bluesky + " " + ctx.url);
    },
    facebook: function (ctx) {
      return "https://www.facebook.com/sharer/sharer.php?u=" + enc(ctx.url);
    },
    // r/CompetitiveWoW and friends are where this data actually gets discussed,
    // and Reddit's submit intent prefills both fields.
    reddit: function (ctx) {
      return "https://www.reddit.com/submit?url=" + enc(ctx.url) +
        "&title=" + enc(ctx.title);
    }
  };

  function refresh(root) {
    var ctx = { url: pageUrl(), title: pageTitle() };
    root.querySelectorAll("a[data-share-network]").forEach(function (link) {
      var build = NETWORKS[link.getAttribute("data-share-network")];
      if (!build) return;
      link.href = build(ctx);
    });
  }

  // -------------------------------------------------------------- copy link

  // Same clipboard + 2s feedback shape as the deep-link copy buttons and the
  // spec page's "Copy full build".
  function initCopy(btn) {
    var icon = btn.querySelector("i");
    var original = icon ? icon.className : "";

    function reset() {
      if (icon) icon.className = original;
      btn.classList.remove("btn-success", "btn-danger");
      btn.classList.add("btn-primary");
    }

    btn.addEventListener("click", function () {
      Promise.resolve()
        .then(function () { return navigator.clipboard.writeText(pageUrl()); })
        .then(function () {
          if (icon) icon.className = "bi bi-check-lg";
          btn.classList.remove("btn-primary");
          btn.classList.add("btn-success");
        })
        .catch(function (err) {
          console.error("share: copy failed", err);
          if (icon) icon.className = "bi bi-exclamation-triangle";
          btn.classList.remove("btn-primary");
          btn.classList.add("btn-danger");
        })
        .then(function () { setTimeout(reset, COPIED_MS); });
    });
  }

  // ------------------------------------------------------------ native share

  // navigator.share is the only route to Instagram, WhatsApp or the Discord app:
  // none of them has a web intent URL. Instagram in particular only offers
  // itself in the sheet when there is an image to post, so the preview card is
  // fetched as a File and attached when it is available.
  //
  // The fetch is warmed on hover/focus/pointerdown and the result is attached
  // synchronously on click: awaiting it inside the click handler would spend the
  // transient user activation that navigator.share requires, and Safari rejects
  // the call outright once that is gone. No image simply means a link share.
  function initNative(btn) {
    if (!navigator.share) {
      // Desktop Chrome/Firefox: leave the button hidden rather than offering a
      // control that throws.
      return;
    }
    btn.hidden = false;

    var file = null;
    var warming = false;

    function warm() {
      if (warming || file || typeof File !== "function") return;
      var src = previewImage();
      if (!src) return;
      warming = true;
      fetch(src, { mode: "cors" })
        .then(function (res) {
          if (!res.ok) throw new Error("HTTP " + res.status);
          return res.blob();
        })
        .then(function (blob) {
          var name = (src.split("/").pop() || "mythistone.png").split("?")[0];
          file = new File([blob], name, { type: blob.type || "image/png" });
        })
        .catch(function (err) {
          // Expected on the github.io mirror, where the canonical
          // mythistone.com image is cross-origin and served without CORS
          // headers. The sheet still gets the link, which is what most targets
          // want anyway.
          console.warn("share: preview image unavailable, sharing link only", err);
        });
    }

    ["pointerenter", "focus", "pointerdown"].forEach(function (evt) {
      btn.addEventListener(evt, warm);
    });

    btn.addEventListener("click", function () {
      var url = pageUrl();
      var title = pageTitle();
      var data = { title: title, text: title, url: url };
      if (file && navigator.canShare && navigator.canShare({ files: [file] })) {
        // The link moves into `text` here: several targets drop `url` entirely
        // once files are present, and a preview card with no way back to the
        // page is worth less than the link.
        data = { title: title, text: title + " " + url, files: [file] };
      }
      Promise.resolve()
        .then(function () { return navigator.share(data); })
        .catch(function (err) {
          if (err && err.name === "AbortError") return;  // sheet dismissed
          console.error("share: native share failed", err);
        });
    });
  }

  // -------------------------------------------------------------------- boot

  function boot() {
    var containers = document.querySelectorAll("[data-share-buttons]");
    if (!containers.length) return;

    containers.forEach(function (root) {
      refresh(root);
      // Cover every way a link can be activated: pointerdown precedes both the
      // middle-click navigation and the context menu, focus covers keyboard.
      ["pointerdown", "focusin", "click"].forEach(function (evt) {
        root.addEventListener(evt, function () { refresh(root); }, true);
      });

      var copy = root.querySelector('[data-share-network="copy"]');
      if (copy) initCopy(copy);
      var native = root.querySelector('[data-share-network="native"]');
      if (native) initNative(native);
    });

    window.addEventListener("hashchange", function () {
      containers.forEach(refresh);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
