/*
 * analyzer.js — the "Am I meta?" gear check.
 *
 * Parses a pasted SimulationCraft addon export entirely in the browser, resolves
 * the spec via the baked lookup tables (window.SIMC_CLASS_TOKENS / SPEC_INDEX /
 * SPEC_DISPLAY), fetches that spec's meta snapshot from
 * /assets/json/spec_meta/<spec_id>.json (baked by generateSpecPages.py) and
 * renders a compact slot-by-slot icon grid. No data ever leaves the page.
 *
 * A slot's meta target is the spec page's TOP (Raider.io top-50) or SIM
 * (SimulationCraft rank 1) pick; on high-diversity slots that have neither
 * (neck, trinkets, weapons), it falls back to the single most-popular item
 * (`common`). The equipped item passes if it matches a target. If the meta
 * item is sitting in the player's bags, the tile flags it as an owned upgrade.
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
  // Interchangeable pairs — a ring/trinket counts as a match if it matches
  // EITHER slot's target, since players slot them in any order.
  var GROUPS = { FINGER_1: "FINGER_1,FINGER_2", FINGER_2: "FINGER_1,FINGER_2",
                 TRINKET_1: "TRINKET_1,TRINKET_2", TRINKET_2: "TRINKET_1,TRINKET_2" };
  // Display order for the report.
  var SLOT_ORDER = ["HEAD", "NECK", "SHOULDER", "BACK", "CHEST", "WRIST", "HANDS",
    "WAIST", "LEGS", "FEET", "FINGER_1", "FINGER_2", "TRINKET_1", "TRINKET_2",
    "MAIN_HAND", "OFF_HAND"];
  // Gear slot -> the enchant slot_group key top players' enchant data uses.
  // Which of these a spec actually enchants is gated at render time on
  // meta.enchant_slot_groups, so listing a slot here never forces a "missing"
  // flag on specs/patches that don't enchant it.
  var ENCHANT_GROUP = { HEAD: "HEAD", SHOULDER: "SHOULDER", BACK: "BACK", CHEST: "CHEST",
    WRIST: "WRIST", LEGS: "LEGS", FEET: "FEET", FINGER_1: "FINGER", FINGER_2: "FINGER",
    MAIN_HAND: "WEAPON", OFF_HAND: "WEAPON" };

  var QUESTION = "/data/icons/inv_misc_questionmark.png";

  var input = document.getElementById("simc-input");
  var results = document.getElementById("analyzer-results");
  var analyzeBtn = document.getElementById("analyze-btn");
  var clearBtn = document.getElementById("clear-btn");
  if (!input || !results || !analyzeBtn) return;

  // id -> {icon, quality, slug} for the site's popular items, so we can draw the
  // user's own equipped icons and link them to /items. Populated once from
  // /assets/json/items_index.json. Items not in this ~500-item index (rare/PvP
  // gear) fall back to a questionmark tile with a Wowhead tooltip — a
  // legitimately-unknown user item, not a data error.
  var itemsIndex = null;
  var itemsIndexPromise = null;

  function loadItemsIndex() {
    if (itemsIndexPromise) return itemsIndexPromise;
    itemsIndexPromise = fetch("/assets/json/items_index.json")
      .then(function (r) { return r.ok ? r.json() : []; })
      .then(function (arr) {
        var map = {};
        (arr || []).forEach(function (it) {
          if (it && it.id != null) map[it.id] = { icon: it.icon, quality: it.quality, slug: it.slug };
        });
        itemsIndex = map;
        return map;
      })
      .catch(function () { itemsIndex = {}; return itemsIndex; });
    return itemsIndexPromise;
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function showError(msg) {
    results.innerHTML = '<div class="alert alert-warning text-dark text-sm mb-0">' +
      '<i class="material-symbols-rounded align-middle me-1">warning</i>' + esc(msg) + "</div>";
  }

  // Parse a SimC export. Returns { classId, specToken, slots, bags, hasBags }.
  // `slots` are the equipped items; `bags` are items in the "### Gear from Bags"
  // section (commented-out `# slot=,id=...` lines), grouped by slot for the
  // owned-upgrade check.
  function parseSimc(text) {
    var lines = text.split(/\r?\n/);
    var out = { classId: null, specToken: null, slots: {}, bags: {}, hasBags: false };
    var inBags = false;

    function parseGear(rest) {
      var idM = rest.match(/(?:^|,)id=(\d+)/);
      if (!idM) return null;
      var bonusM = rest.match(/(?:^|,)bonus_id=([\d/]+)/);
      var enchM = rest.match(/(?:^|,)enchant_id=(\d+)/);
      var gemM = rest.match(/(?:^|,)gem_id=([\d/]+)/);
      var craftM = rest.match(/(?:^|,)crafted_stats=([\d/]+)/);
      return {
        id: parseInt(idM[1], 10),
        bonus: bonusM ? bonusM[1].split("/").filter(Boolean) : [],
        enchant: enchM ? parseInt(enchM[1], 10) : null,
        gems: gemM ? gemM[1].split("/").filter(Boolean).map(Number) : [],
        crafted: craftM ? craftM[1].split("/").filter(Boolean) : [],
      };
    }

    lines.forEach(function (raw) {
      var line = raw.trim();
      if (!line) return;

      if (line[0] === "#") {
        if (/gear from bags/i.test(line)) { inBags = true; return; }
        if (/^###/.test(line) && !/gear from bags/i.test(line)) { inBags = false; return; }
        // Inside the bags block, gear lines are commented: "# slot=,id=..."
        if (inBags) {
          var body = line.replace(/^#+\s*/, "");
          var beq = body.indexOf("=");
          if (beq === -1) return;
          var bkey = body.slice(0, beq).trim().toLowerCase();
          if (!Object.prototype.hasOwnProperty.call(SLOT_MAP, bkey)) return;
          var bgear = parseGear(body.slice(beq + 1));
          if (!bgear) return;
          var grp = groupKey(SLOT_MAP[bkey]);
          (out.bags[grp] || (out.bags[grp] = {}))[bgear.id] = bgear;
          out.hasBags = true;
        }
        return;
      }

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
        var gear = parseGear(rest);
        if (gear) out.slots[SLOT_MAP[key]] = gear;
      }
    });
    return out;
  }

  // Slot -> the key rings/trinkets share (so a bag ring counts for either finger).
  function groupKey(slotName) {
    if (slotName === "FINGER_1" || slotName === "FINGER_2") return "FINGER";
    if (slotName === "TRINKET_1" || slotName === "TRINKET_2") return "TRINKET";
    return slotName;
  }

  // Gather the acceptable target id sets for a slot across its grouped names:
  //   sim/top = the ideal picks; common = the most-popular fallback.
  function acceptableIds(metaSlots, slotName) {
    var names = (GROUPS[slotName] || slotName).split(",");
    var sim = new Set(), top = new Set(), common = new Set();
    names.forEach(function (n) {
      var m = metaSlots[n];
      if (!m) return;
      if (m.sim && m.sim.id != null) sim.add(m.sim.id);
      (m.top || []).forEach(function (t) { if (t.id != null) top.add(t.id); });
      if (m.common && m.common.id != null) common.add(m.common.id);
    });
    return { sim: sim, top: top, common: common };
  }

  // The meta picks to display for a slot: the group-union of SIM + TOP, or the
  // group-union of `common` when there's no TOP/SIM (the fallback target).
  // Returns [{pick, kind}], kind in {sim, top, meta}.
  function displayTargets(metaSlots, slotName) {
    var names = (GROUPS[slotName] || slotName).split(",");
    var seen = {}, out = [];
    function add(pick, kind) {
      if (!pick || pick.id == null || seen[pick.id]) return;
      seen[pick.id] = true; out.push({ pick: pick, kind: kind });
    }
    names.forEach(function (n) { var m = metaSlots[n]; if (m && m.sim) add(m.sim, "sim"); });
    names.forEach(function (n) { var m = metaSlots[n]; if (m) (m.top || []).forEach(function (t) { add(t, "top"); }); });
    if (!out.length) {
      names.forEach(function (n) { var m = metaSlots[n]; if (m && m.common) add(m.common, "meta"); });
    }
    return out;
  }

  function userMeta(id) { return (itemsIndex && itemsIndex[id]) || null; }

  // Build the &bonus=..&spec=..&ench=..&gems=..&pcs=.. tail shared by the tooltip
  // and the Wowhead link, from either a meta pick or a parsed user item.
  function wowheadParams(o, specId) {
    var p = "";
    var bonus = o.bonus && o.bonus.length ? o.bonus.join(":") : "";
    if (bonus) p += "&bonus=" + bonus;
    if (specId != null) p += "&spec=" + specId;
    var ench = o.ench != null ? o.ench : o.enchant;
    if (ench) p += "&ench=" + ench;
    if (o.gems && o.gems.length) p += "&gems=" + o.gems.join(":");
    if (o.pcs && o.pcs.length) p += "&pcs=" + o.pcs.join(":");
    if (o.crafted && o.crafted.length) p += "&crafted-stats=" + o.crafted.join(":");
    return p;
  }

  // One clickable, Wowhead-tooltipped item icon. Links to the internal /items
  // page when a slug is known, else to Wowhead. `data` supplies bonus/ench/gems
  // for the tooltip; `extra` is overlay markup (status glyph / chip).
  function iconEl(id, iconName, quality, slug, data, specId, cls, extra) {
    var q = quality != null ? " border-quality-" + quality : "";
    var src = iconName ? "/data/icons/" + esc(iconName) + ".png" : QUESTION;
    var params = wowheadParams(data || {}, specId);
    var href = slug ? "/items/" + esc(slug) + (specId != null ? "?spec=" + specId : "")
                    : "https://www.wowhead.com/item=" + id + (params ? "?" + params.replace(/^&/, "") : "");
    var target = slug ? "" : ' target="_blank" rel="noopener"';
    return '<a class="an-icon' + q + (cls ? " " + cls : "") + '"' + target +
      ' href="' + href + '" data-wowhead="item=' + id + params + '">' +
      '<img src="' + src + '" alt="" loading="lazy" onerror="this.src=\'' + QUESTION + '\'">' +
      (extra || "") + "</a>";
  }

  // Meta-target badge, reusing the spec page's SIM/TOP badge styling and
  // tooltips (spec_page.html:51-52) so the same signal reads the same everywhere.
  // The most-popular fallback (`meta`) carries no badge — it isn't a
  // classification, just the item shown where no TOP/SIM target exists.
  function chip(kind, pick, meta) {
    if (kind === "sim") {
      var simTip = "Best item for this slot according to SimulationCraft" +
        (pick && pick.dps_pct != null && pick.dps_pct >= 0.05
          ? " (+" + pick.dps_pct.toFixed(1) + "% DPS over the most-equipped item)" : "") + ".";
      return '<span class="badge simc-badge item-icon-sim-badge" data-bs-toggle="tooltip"' +
        ' data-bs-container="body" title="' + esc(simTip) + '">SIM</span>';
    }
    if (kind === "top") {
      var pct = pick && pick.pct != null ? Math.round(pick.pct) : null;
      var topTip = "Used by " + (pct != null ? pct : "N/A") + "% of the top 50 " +
        (meta.spec || "") + " " + (meta.class || "") +
        " players according to Raider.io verified Loadouts.";
      return '<span class="badge bis-badge item-icon-top-badge" data-bs-toggle="tooltip"' +
        ' data-bs-container="body" title="' + esc(topTip) + '">TOP</span>';
    }
    return "";
  }

  // A small Wowhead-tooltipped icon for an enchant (linked via its scroll itemId)
  // or a gem (an item). No internal /items page for these, so link to Wowhead.
  function auxIconEl(o, whRef, cls, extra) {
    var q = o.quality != null ? " border-quality-" + o.quality : "";
    var src = o.icon ? "/data/icons/" + esc(o.icon) + ".png" : QUESTION;
    var linkId = whRef.split("=")[1];
    return '<a class="an-icon an-icon-sm' + q + (cls ? " " + cls : "") + '" target="_blank" rel="noopener"' +
      ' href="https://www.wowhead.com/' + esc(whRef) + '" data-wowhead="' + esc(whRef) + '">' +
      '<img src="' + src + '" alt="" loading="lazy" onerror="this.src=\'' + QUESTION + '\'">' +
      (extra || "") + "</a>";
  }

  // Gems and enchants are scored against the single most-popular top-50 combo
  // (a multiset baked into meta.gem_combo / meta.enchant_combo). The combo is a
  // per-id quantity *budget*: a socketed gem / applied enchant is OK while its
  // id stays within the combo's count for that id, "warn" once the player uses
  // more of an id than top players do (all occurrences flagged), and "bad" when
  // the id isn't in the combo at all.

  // Collapse a combo's entries into an {id: qty} allowance budget.
  function comboBudget(combo) {
    var byId = {};
    if (combo && combo.entries) combo.entries.forEach(function (e) { byId[e.id] = e.qty; });
    return byId;
  }

  // {id: entry} from a combo, so we can draw the known icon for an equipped id.
  function comboInfo(combo) {
    var m = {};
    if (combo && combo.entries) combo.entries.forEach(function (e) { m[e.id] = e; });
    return m;
  }

  // Score one equipped id against the budget and the player's full id counts.
  function classifyAgainstCombo(id, playerCounts, budget) {
    var allowed = budget[id] || 0;
    if (allowed === 0) return "bad";        // not part of the top combo
    if ((playerCounts[id] || 0) > allowed) return "warn"; // more than top players use
    return "ok";
  }

  var MARK = { ok: "an-mark-ok", warn: "an-mark-warn", bad: "an-mark-bad" };
  var GLYPH = { ok: "✓", warn: "⚠", bad: "✕" };

  // Corner status glyph (✓ / ⚠ / ✕) overlaid on a small gem/enchant icon.
  function statusMark(st, tip) {
    return '<span class="an-mark ' + MARK[st] + '" title="' + esc(tip) + '">' + GLYPH[st] + "</span>";
  }

  // Count every gem id (across all sockets) and enchant id the player runs, so
  // the over-quantity check can see the whole build at once.
  function tallyPlayer(parsed) {
    var gems = {}, enchants = {};
    Object.keys(parsed.slots).forEach(function (s) {
      (parsed.slots[s].gems || []).forEach(function (id) { gems[id] = (gems[id] || 0) + 1; });
      var e = parsed.slots[s].enchant;
      if (e) enchants[e] = (enchants[e] || 0) + 1;
    });
    return { gems: gems, enchants: enchants };
  }

  // Inline TOP badge for the combo header. chip("top",…) uses the absolutely
  // positioned item-icon-top-badge (meant to sit on a gear icon); in a text
  // header it has no positioned ancestor and escapes to the page corner, so we
  // render a plain inline badge with the same wording here instead.
  function topBadgeInline(pct, meta) {
    var p = pct != null ? Math.round(pct) : null;
    var tip = "Used by " + (p != null ? p : "N/A") + "% of the top 50 " +
      (meta.spec || "") + " " + (meta.class || "") +
      " players according to Raider.io verified Loadouts.";
    return '<span class="badge bis-badge an-legend-badge" data-bs-toggle="tooltip"' +
      ' data-bs-container="body" title="' + esc(tip) + '">TOP</span>';
  }

  // Reference row for a combo: the target multiset (qty badges, inline TOP badge
  // + %), each entry ticked when the player runs the full quantity, shown with a
  // have/need count when partially there, greyed only when absent. Mirrors the
  // spec page's Gem/Enchant Combos sections.
  function comboSection(combo, playerCounts, kind, meta) {
    if (!combo || !combo.entries || !combo.entries.length) return "";
    var isEnch = kind === "enchant";
    var total = 0, matched = 0;
    var tiles = combo.entries.map(function (e) {
      var have = playerCounts[e.id] || 0;
      var full = have >= e.qty;
      var partial = have > 0 && have < e.qty;
      total += e.qty; matched += Math.min(have, e.qty);
      var whRef = isEnch && e.itemId ? "item=" + e.itemId : "item=" + e.id;
      // ×N shows the target count; on a partial entry the "have/need" badge
      // already carries it, so don't stack both.
      var qty = (e.qty > 1 && !partial) ? '<span class="combo-qty-badge">×' + e.qty + "</span>" : "";
      var mark = full ? '<span class="an-mark an-mark-ok">✓</span>'
        : partial ? '<span class="an-count-badge" title="You have ' + have + " of " + e.qty + '">' + have + "/" + e.qty + "</span>"
        : "";
      var cls = full || partial ? "an-gem-used" : "an-gem-unused";
      return auxIconEl(e, whRef, cls, qty + mark);
    }).join("");
    var label = isEnch ? "Enchants" : "Gems";
    var summary = matched + " of " + total + " " + (isEnch ? "enchants" : "gems") + " match";
    return '<div class="an-gems">' +
      '<div class="an-gems-head"><span class="text-uppercase text-secondary me-2">' + label + "</span>" +
      topBadgeInline(combo.pct, meta) +
      '<span class="text-xs text-secondary ms-2">' + esc(summary) + "</span></div>" +
      '<div class="an-gems-row">' + tiles + "</div></div>";
  }

  function render(meta, parsed) {
    var specId = meta.spec_id;
    var disp = (window.SPEC_DISPLAY || {})[specId] || { name: meta.spec, class: meta.class, icon: null };
    var tiles = [];
    var comparable = 0, good = 0;

    // Top-combo budgets + the player's full id counts, built once so the
    // per-slot gem/enchant checks (and the over-quantity flag) see every socket.
    var gemBudget = comboBudget(meta.gem_combo);
    var gemInfo = comboInfo(meta.gem_combo);
    var enchBudget = comboBudget(meta.enchant_combo);
    var enchInfo = comboInfo(meta.enchant_combo);
    var counts = tallyPlayer(parsed);

    // Which bare slots to flag as "missing an enchant". Top players enchant
    // enchant_group_expected[G] slots in each group; within a group we treat the
    // slots that already carry an enchant as covering that quota first, then flag
    // only as many still-bare slots as are needed to reach it. This stops an
    // un-enchantable caster off-hand (WEAPON expected 1, main hand already done)
    // from being flagged, while a dual-wielder (WEAPON expected 2) still is.
    var enchExpected = meta.enchant_group_expected || {};
    var missingSlots = {};
    var groupSlots = {};
    SLOT_ORDER.forEach(function (s) {
      if (!parsed.slots[s]) return;
      var g = ENCHANT_GROUP[s];
      if (g) (groupSlots[g] || (groupSlots[g] = [])).push(s);
    });
    Object.keys(groupSlots).forEach(function (g) {
      var expected = enchExpected[g] || 0;
      if (!expected) return;
      var slotsG = groupSlots[g];
      var enchanted = slotsG.filter(function (s) { return parsed.slots[s].enchant; }).length;
      var bare = slotsG.filter(function (s) { return !parsed.slots[s].enchant; });
      bare.slice(0, Math.max(0, expected - enchanted)).forEach(function (s) { missingSlots[s] = true; });
    });

    SLOT_ORDER.forEach(function (slotName) {
      var metaSlot = meta.slots[slotName];
      var user = parsed.slots[slotName];
      if (!metaSlot || !user) return; // only compare slots both sides have

      var acc = acceptableIds(meta.slots, slotName);
      var hasIdeal = acc.sim.size > 0 || acc.top.size > 0;
      var hasAny = hasIdeal || acc.common.size > 0;
      if (!hasAny) return; // truly no meta data for this slot

      var status;
      if (acc.sim.has(user.id)) status = "sim";
      else if (acc.top.has(user.id)) status = "top";
      else if (hasIdeal) status = "off";              // has TOP/SIM, user misses
      else if (acc.common.has(user.id)) status = "meta"; // fallback: popular pick
      else status = "off";                            // misses the popular fallback

      comparable++;
      var matched = status !== "off";
      if (matched) good++;

      // User's equipped item, with a status overlay glyph.
      var um = userMeta(user.id);
      var mark = matched ? '<span class="an-mark an-mark-ok">✓</span>'
                         : '<span class="an-mark an-mark-bad">✕</span>';
      var yours = iconEl(user.id, um && um.icon, um && um.quality, um && um.slug,
        user, specId, "an-user", mark);

      // Meta target icons (group-union; SIM/TOP, or the popular fallback).
      var targets = displayTargets(meta.slots, slotName).map(function (t) {
        return iconEl(t.pick.id, t.pick.icon, t.pick.quality, t.pick.slug, t.pick, specId, "", chip(t.kind, t.pick, meta));
      });
      var targetHtml = targets.length ? '<div class="an-targets">' + targets.join("") + "</div>" : "";

      // Owned-upgrade: is an acceptable meta item sitting in the player's bags?
      var inBagsHtml = "";
      if (!matched) {
        var bagGroup = parsed.bags[groupKey(slotName)] || {};
        var wantIds = [];
        acc.sim.forEach(function (i) { wantIds.push(i); });
        acc.top.forEach(function (i) { wantIds.push(i); });
        if (!hasIdeal) acc.common.forEach(function (i) { wantIds.push(i); });
        var ownedId = wantIds.filter(function (i) { return bagGroup[i]; })[0];
        if (ownedId != null) {
          inBagsHtml = '<div class="an-inbags">' +
            '<i class="material-symbols-rounded">backpack</i> In your bags</div>';
        }
      }

      // GEM cell (fixed left of the footer): each socketed gem drawn as a small
      // gem-style icon with a ✓ / ⚠ / ✕ corner glyph. Off-combo and over-used
      // gems are flagged; a gem the top combo knows uses its own icon, an
      // off-combo gem falls back to a questionmark + Wowhead tooltip.
      var gemCell = "";
      if (meta.gem_combo && user.gems && user.gems.length) {
        gemCell = user.gems.map(function (gid) {
          var st = classifyAgainstCombo(gid, counts.gems, gemBudget);
          var info = gemInfo[gid] || { id: gid };
          var tip = st === "warn"
              ? "Top players use only " + (gemBudget[gid] || 0) + "× this gem — you have " + (counts.gems[gid] || 0) + "."
            : st === "bad" ? "Not in the top gem combo."
            : (info.name || "Gem") + " matches the top combo.";
          return auxIconEl(info, "item=" + gid, "an-gem-slot", statusMark(st, tip));
        }).join("");
      }

      // ENCHANT cell (fixed right of the footer): the applied enchant drawn the
      // same way as a gem — a small icon with a status glyph. A bare slot the top
      // players enchant shows a placeholder ✕; a slot they don't enchant (or one
      // already covered by another slot in the group) shows nothing.
      var enchCell = "";
      var eg = ENCHANT_GROUP[slotName];
      if (meta.enchant_combo && eg && (enchExpected[eg] || 0) > 0) {
        if (!user.enchant) {
          if (missingSlots[slotName]) {
            enchCell = '<span class="an-icon an-icon-sm an-gem-slot an-ench-missing">' +
              '<img src="' + QUESTION + '" alt="">' + statusMark("bad", "Missing enchant") + "</span>";
          }
        } else {
          var est = classifyAgainstCombo(user.enchant, counts.enchants, enchBudget);
          var einfo = enchInfo[user.enchant] || { id: user.enchant };
          var whRef = einfo.itemId ? "item=" + einfo.itemId : "item=" + user.enchant;
          var etip = est === "ok" ? ((einfo.name || "Enchant") + " matches the top combo.")
            : est === "warn" ? "You use this enchant more times than the top players do."
            : "Off-combo enchant — not in the top players' set.";
          enchCell = auxIconEl(einfo, whRef, "an-gem-slot", statusMark(est, etip));
        }
      }

      // Already on a meta pick → just the equipped item; no exchange arrow /
      // target (nothing to change). Off-meta slots show the swap to make.
      var body = '<div class="an-tile-body"><div class="an-yours">' + yours + "</div>";
      if (!matched) {
        body += '<span class="material-symbols-rounded an-arrow">arrow_right_alt</span>' + targetHtml;
      }
      body += "</div>";

      // Footer band right under the item: gems then enchant, grouped tightly.
      // "In your bags" is pinned to the very bottom (see .an-inbags) instead of
      // floating mid-tile.
      var foot = (gemCell || enchCell)
        ? '<div class="an-foot">' +
          (gemCell ? '<div class="an-foot-gems">' + gemCell + "</div>" : "") +
          (enchCell ? '<div class="an-foot-ench">' + enchCell + "</div>" : "") +
          "</div>"
        : "";

      tiles.push(
        '<div class="an-tile an-' + status + '">' +
          '<div class="an-slot-label">' + esc(slotName.replace(/_/g, " ")) + "</div>" +
          body +
          foot +
          inBagsHtml +
        "</div>"
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
      '<div class="d-flex align-items-center mb-2">' + iconHtml +
        '<div><div class="font-weight-bolder">' + esc(disp.name) + " " + esc(disp.class) + "</div>" +
        '<div class="text-xs text-secondary">' + good + " of " + comparable + " scored slots on a meta pick</div></div>" +
        '<div class="ms-auto text-end"><div class="an-score ' + (score >= 80 ? "an-score-hi" : score >= 50 ? "an-score-mid" : "an-score-lo") + '">' + score + "%</div>" +
        '<div class="text-xxs text-uppercase text-secondary">meta match</div></div>' +
      "</div>" +
      (stats ? '<div class="my-3 text-xs"><span class="text-uppercase text-secondary me-2">Stat priority</span>' + stats + "</div>" : "") +
      '<div class="an-grid">' + tiles.join("") + "</div>" +
      comboSection(meta.gem_combo, counts.gems, "gem", meta) +
      comboSection(meta.enchant_combo, counts.enchants, "enchant", meta) +
      '<p class="text-xxs text-secondary mt-3 mb-0">' +
        '<span class="badge simc-badge an-legend-badge">SIM</span> SimulationCraft rank 1 &nbsp; ' +
        '<span class="badge bis-badge an-legend-badge">TOP</span> Raider.io top-50 loadout pick. ' +
        'Where a slot has neither, the most-popular item is shown for reference. ' +
        'Gems and enchants are scored per slot against the top players’ most-popular combo. ' +
        'Click an item for its details page; hover for the Wowhead tooltip. Talents aren’t compared yet.</p>';

    if (window.$WowheadPower && typeof window.$WowheadPower.refreshLinks === "function") {
      try { window.$WowheadPower.refreshLinks(); } catch (e) { /* tooltips are best-effort */ }
    }
    // The global tooltip init (material-dashboard.js) only scans the DOM at page
    // load; our badges are rendered afterward, so wire them up here.
    if (window.bootstrap && window.bootstrap.Tooltip) {
      results.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(function (el) {
        try { window.bootstrap.Tooltip.getOrCreateInstance(el); } catch (e) { /* best-effort */ }
      });
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
    Promise.all([
      fetch("/assets/json/spec_meta/" + specId + ".json").then(function (r) {
        if (!r.ok) throw new Error("no meta");
        return r.json();
      }),
      loadItemsIndex(),
    ])
      .then(function (out) { render(out[0], parsed); })
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
