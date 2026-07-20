/*
 * analyzer.js — the "Am I meta?" gear check.
 *
 * Parses a pasted SimulationCraft addon export entirely in the browser, resolves
 * the spec via the baked lookup tables (window.SIMC_CLASS_TOKENS / SPEC_INDEX /
 * SPEC_DISPLAY), fetches that spec's meta snapshot from
 * /assets/json/spec_meta/<spec_id>.json (baked by generateSpecPages.py) and
 * renders a slot-by-slot comparison. No data ever leaves the page.
 */
(function () {
  "use strict";

  // SimC export slot token -> the slot name used in the baked meta JSON.
  var SLOT_MAP = {
    head: "HEAD", neck: "NECK", shoulder: "SHOULDER", shoulders: "SHOULDER",
    back: "BACK", chest: "CHEST", wrist: "WRIST", wrists: "WRIST",
    hands: "HANDS", waist: "WAIST", legs: "LEGS", feet: "FEET",
    finger1: "FINGER_1", finger2: "FINGER_2",
    trinket1: "TRINKET_1", trinket2: "TRINKET_2",
    main_hand: "MAIN_HAND", off_hand: "OFF_HAND",
  };
  // Interchangeable pairs — a ring/trinket counts as meta if it matches EITHER
  // slot's meta pick, since players slot them in any order.
  var GROUPS = { FINGER_1: "FINGER_1,FINGER_2", FINGER_2: "FINGER_1,FINGER_2",
                 TRINKET_1: "TRINKET_1,TRINKET_2", TRINKET_2: "TRINKET_1,TRINKET_2" };
  // Display order for the report.
  var SLOT_ORDER = ["HEAD", "NECK", "SHOULDER", "BACK", "CHEST", "WRIST", "HANDS",
    "WAIST", "LEGS", "FEET", "FINGER_1", "FINGER_2", "TRINKET_1", "TRINKET_2",
    "MAIN_HAND", "OFF_HAND"];
  // Slots the meta tracks enchants for map to their enchant slot_group key.
  var ENCHANT_GROUP = { BACK: "BACK", CHEST: "CHEST", WRIST: "WRIST", LEGS: "LEGS",
    FEET: "FEET", FINGER_1: "FINGER", FINGER_2: "FINGER",
    MAIN_HAND: "WEAPON", OFF_HAND: "WEAPON" };

  var input = document.getElementById("simc-input");
  var results = document.getElementById("analyzer-results");
  var analyzeBtn = document.getElementById("analyze-btn");
  var clearBtn = document.getElementById("clear-btn");
  if (!input || !results || !analyzeBtn) return;

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function showError(msg) {
    results.innerHTML = '<div class="alert alert-warning text-dark text-sm mb-0">' +
      '<i class="material-symbols-rounded align-middle me-1">warning</i>' + esc(msg) + "</div>";
  }

  // Parse a SimC export into { classId, specToken, slots: {SLOT: {id, enchant, gems}} }.
  function parseSimc(text) {
    var lines = text.split(/\r?\n/);
    var out = { classId: null, specToken: null, slots: {} };
    lines.forEach(function (raw) {
      var line = raw.trim();
      if (!line || line[0] === "#") return;
      var eq = line.indexOf("=");
      if (eq === -1) return;
      var key = line.slice(0, eq).trim().toLowerCase();
      var rest = line.slice(eq + 1);

      if (out.classId == null && Object.prototype.hasOwnProperty.call(window.SIMC_CLASS_TOKENS, key)) {
        out.classId = String(window.SIMC_CLASS_TOKENS[key]);
        return;
      }
      if (key === "spec") {
        out.specToken = rest.replace(/"/g, "").trim().toLowerCase();
        return;
      }
      if (Object.prototype.hasOwnProperty.call(SLOT_MAP, key)) {
        var idM = rest.match(/(?:^|,)id=(\d+)/);
        if (!idM) return; // empty slot line
        var enchM = rest.match(/(?:^|,)enchant_id=(\d+)/);
        var gemM = rest.match(/(?:^|,)gem_id=([\d/]+)/);
        out.slots[SLOT_MAP[key]] = {
          id: parseInt(idM[1], 10),
          enchant: enchM ? parseInt(enchM[1], 10) : null,
          gems: gemM ? gemM[1].split("/").filter(Boolean).map(Number) : [],
        };
      }
    });
    return out;
  }

  // The set of meta item ids acceptable for a slot (its own pick, plus the
  // paired slot's pick for interchangeable rings/trinkets).
  function acceptableIds(metaSlots, slotName) {
    var names = (GROUPS[slotName] || slotName).split(",");
    var top = new Set(), bis = new Set();
    names.forEach(function (n) {
      var m = metaSlots[n];
      if (!m) return;
      if (m.top_id != null) top.add(m.top_id);
      if (m.bis_id != null) bis.add(m.bis_id);
    });
    return { top: top, bis: bis };
  }

  function render(meta, parsed) {
    var disp = (window.SPEC_DISPLAY || {})[meta.spec_id] || { name: meta.spec, class: meta.class, icon: null };
    var rows = [];
    var comparable = 0, good = 0;

    SLOT_ORDER.forEach(function (slotName) {
      var metaSlot = meta.slots[slotName];
      var user = parsed.slots[slotName];
      if (!metaSlot || !user) return; // only compare slots both sides have
      comparable++;

      var acc = acceptableIds(meta.slots, slotName);
      var status, label, cls;
      if (acc.bis.has(user.id)) { status = "bis"; label = "SimC BiS"; cls = "an-bis"; good++; }
      else if (acc.top.has(user.id)) { status = "meta"; label = "Meta pick"; cls = "an-meta"; good++; }
      else { status = "off"; label = "Off-meta"; cls = "an-off"; }

      // Enchant check for enchantable slots the meta tracks.
      var enchNote = "";
      var eg = ENCHANT_GROUP[slotName];
      var metaEnch = eg && meta.enchants ? meta.enchants[eg] : null;
      if (metaEnch && metaEnch.id) {
        if (!user.enchant) enchNote = '<span class="an-flag an-flag-bad">Missing enchant</span>';
        else if (user.enchant !== metaEnch.id) enchNote = '<span class="an-flag an-flag-warn">Off-meta enchant</span>';
        else enchNote = '<span class="an-flag an-flag-ok">Enchant ✓</span>';
      }

      var metaName = metaSlot.top_name || ("item " + metaSlot.top_id);
      rows.push(
        '<tr>' +
          '<td class="an-slot text-xxs text-uppercase text-secondary">' + esc(slotName.replace(/_/g, " ")) + '</td>' +
          '<td class="text-xs"><a href="https://www.wowhead.com/item=' + user.id + '" target="_blank" rel="noopener" data-wowhead="item=' + user.id + '">Your item</a></td>' +
          '<td><span class="badge ' + cls + '">' + label + '</span></td>' +
          '<td class="text-xs text-secondary">' + (status === "off" ? ("meta: " + esc(metaName)) : "") + " " + enchNote + '</td>' +
        '</tr>'
      );
    });

    if (comparable === 0) {
      showError("Couldn't match any gear slots. Make sure you pasted the full SimC export (the lines like head=,id=...).");
      return;
    }

    var score = Math.round((good / comparable) * 100);
    var stats = (meta.stat_priority || []).map(function (s) {
      return '<span class="badge an-stat">' + esc(s) + "</span>";
    }).join(" ");

    var iconHtml = disp.icon ? '<img src="/data/icons/' + esc(disp.icon) + '.jpg" class="an-spec-icon me-2" alt="">' : "";
    results.innerHTML =
      '<div class="d-flex align-items-center mb-3">' + iconHtml +
        '<div><div class="font-weight-bolder">' + esc(disp.name) + " " + esc(disp.class) + '</div>' +
        '<div class="text-xs text-secondary">' + good + " of " + comparable + " slots on the meta or BiS pick</div></div>" +
        '<div class="ms-auto text-end"><div class="an-score ' + (score >= 80 ? "an-score-hi" : score >= 50 ? "an-score-mid" : "an-score-lo") + '">' + score + '%</div>' +
        '<div class="text-xxs text-uppercase text-secondary">meta match</div></div>' +
      '</div>' +
      (stats ? '<div class="mb-3 text-xs"><span class="text-uppercase text-secondary me-2">Stat priority</span>' + stats + '</div>' : "") +
      '<div class="table-responsive"><table class="table align-items-center mb-0 an-table"><tbody>' + rows.join("") + '</tbody></table></div>' +
      '<p class="text-xxs text-secondary mt-2 mb-0">Gems and talents aren’t compared yet. Meta = most-equipped item for the slot; BiS = SimulationCraft rank 1.</p>';

    if (window.$WowheadPower && typeof window.$WowheadPower.refreshLinks === "function") {
      try { window.$WowheadPower.refreshLinks(); } catch (e) { /* tooltips are best-effort */ }
    }
  }

  function analyze() {
    var text = input.value || "";
    if (!text.trim()) { showError("Paste your SimC export first."); return; }
    var parsed = parseSimc(text);
    if (parsed.classId == null || !parsed.specToken) {
      showError("Couldn't detect your class/spec. Paste the full export including the line like mage=\"Name\" and spec=fire.");
      return;
    }
    var specId = (window.SPEC_INDEX || {})[parsed.classId + "|" + parsed.specToken];
    if (specId == null) {
      showError("That class/spec isn't tracked yet (" + esc(parsed.specToken) + ").");
      return;
    }
    results.innerHTML = '<p class="text-sm text-secondary mb-0">Analyzing…</p>';
    fetch("/assets/json/spec_meta/" + specId + ".json")
      .then(function (r) { if (!r.ok) throw new Error("no meta"); return r.json(); })
      .then(function (meta) { render(meta, parsed); })
      .catch(function () {
        showError("No meta data available for this spec yet. Check back after the next update.");
      });
  }

  analyzeBtn.addEventListener("click", analyze);
  if (clearBtn) clearBtn.addEventListener("click", function () {
    input.value = "";
    results.innerHTML = '<p class="text-sm text-secondary mb-0">Paste your export and press <strong>Analyze</strong> to see your report.</p>';
    input.focus();
  });
})();
