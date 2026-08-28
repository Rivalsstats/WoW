/*
 * Top Trends bar — seamless, count-independent infinite ticker.
 *
 * The template renders a single `.trends-seq` row of items. A pure two-copy CSS
 * marquee only stays seamless when one copy is wider than the viewport; on a wide
 * screen with few items you'd see a gap and a jump. So we clone the sequence
 * enough times to overfill the viewport (>= 2x), then animate the track by
 * exactly one sequence width via the `--trends-shift` CSS variable — the next
 * identical copy lands where the first was, so the loop is invisible regardless
 * of item count or viewport width. Vanilla JS, no dependencies.
 */
(function () {
  "use strict";
  var SPEED = 70; // pixels per second

  function debounce(fn, ms) {
    var t;
    return function () {
      clearTimeout(t);
      t = setTimeout(fn, ms);
    };
  }

  function setup(track) {
    var marquee = track.closest ? track.closest(".trends-marquee") : null;
    if (!marquee) return;

    var hasBsTooltip = window.bootstrap && window.bootstrap.Tooltip;

    // Drop any clones from a previous layout pass before measuring. Dispose their
    // Bootstrap tooltip instances first so re-layout doesn't leak detached instances.
    var clones = track.querySelectorAll(".trends-seq.trends-clone");
    for (var i = 0; i < clones.length; i++) {
      if (hasBsTooltip) {
        var stale = clones[i].querySelectorAll('[data-bs-toggle="tooltip"]');
        for (var s = 0; s < stale.length; s++) {
          var inst = window.bootstrap.Tooltip.getInstance(stale[s]);
          if (inst) inst.dispose();
        }
      }
      clones[i].remove();
    }
    track.classList.remove("is-animated");

    var seq = track.querySelector(".trends-seq");
    if (!seq) return;

    var seqW = seq.getBoundingClientRect().width;
    var viewW = marquee.getBoundingClientRect().width;
    if (seqW <= 0 || viewW <= 0) return;

    // Enough copies that a one-sequence shift never uncovers the viewport.
    var copies = Math.max(2, Math.ceil((viewW * 2) / seqW) + 1);
    for (var c = 1; c < copies; c++) {
      var clone = seq.cloneNode(true);
      clone.classList.add("trends-clone");
      clone.setAttribute("aria-hidden", "true");
      track.appendChild(clone);
    }

    track.style.setProperty("--trends-shift", seqW + "px");
    track.style.setProperty("--trends-duration", seqW / SPEED + "s");
    track.classList.add("is-animated");

    // Rich (HTML) tooltips are per-element JS instances that don't survive cloning,
    // so init every tooltip trigger now — originals and clones alike. getOrCreateInstance
    // is idempotent, so originals already initialised by material-dashboard are untouched.
    if (hasBsTooltip) {
      var tips = track.querySelectorAll('[data-bs-toggle="tooltip"]');
      for (var t = 0; t < tips.length; t++) {
        window.bootstrap.Tooltip.getOrCreateInstance(tips[t]);
      }
    }
  }

  function init() {
    var tracks = document.querySelectorAll(".trends-track");
    for (var i = 0; i < tracks.length; i++) setup(tracks[i]);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
  // Re-measure after late icon loads and on resize/orientation change.
  window.addEventListener("load", init);
  window.addEventListener("resize", debounce(init, 250));
})();
