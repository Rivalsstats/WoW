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
 *
 * The player's own items are drawn from items_index.json (the ~500 items with an
 * /items page) and, for everything else they can equip, the sharded icon index
 * at /assets/json/item_icons/ (baked by generateAnalyzerPage.py).
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
  // Scoring order for the report. Load-bearing beyond mere display: the
  // unique-equipped reservation (groupSuggested) hands a ring/trinket to the
  // FIRST slot of its pair, and the enchant quota walks slots in this order.
  var SLOT_ORDER = ["HEAD", "NECK", "SHOULDER", "BACK", "CHEST", "WRIST", "HANDS",
    "WAIST", "LEGS", "FEET", "FINGER_1", "FINGER_2", "TRINKET_1", "TRINKET_2",
    "MAIN_HAND", "OFF_HAND"];

  // Armory display grouping — mirrors generateSpecPages.py LEFT_ORDER /
  // RIGHT_ORDER / WEAPON_SLOTS / TRINKET_SLOTS so the report reads like the
  // spec page's Gear Overview (two panes: armour left/right, then weapons
  // beside trinkets) instead of flowing row-major through SLOT_ORDER.
  var LEFT_ORDER = ["HEAD", "NECK", "SHOULDER", "BACK", "CHEST", "WRIST"];
  var RIGHT_ORDER = ["HANDS", "WAIST", "LEGS", "FEET", "FINGER_1", "FINGER_2"];
  var WEAPON_SLOTS = ["MAIN_HAND", "OFF_HAND"];
  var TRINKET_SLOTS = ["TRINKET_1", "TRINKET_2"];
  var DISPLAY_BLOCKS = [
    { left: { slots: LEFT_ORDER }, right: { slots: RIGHT_ORDER } },
    { left: { slots: WEAPON_SLOTS, head: "Weapon" },
      right: { slots: TRINKET_SLOTS, head: "Trinkets" } },
  ];

  // A slot scored but not placed in a pane would silently vanish from the
  // report. render() runs inside analyze()'s promise chain, whose .catch
  // rewrites every error into "No meta data available for this spec yet" — so
  // check the grouping here, at load, where the failure can't be disguised.
  (function () {
    var placed = [];
    DISPLAY_BLOCKS.forEach(function (b) {
      placed = placed.concat(b.left.slots, b.right.slots);
    });
    var missing = SLOT_ORDER.filter(function (s) { return placed.indexOf(s) === -1; });
    var extra = placed.filter(function (s) { return SLOT_ORDER.indexOf(s) === -1; });
    if (missing.length || extra.length) {
      throw new Error("analyzer.js: DISPLAY_BLOCKS must cover SLOT_ORDER exactly" +
        (missing.length ? " (unplaced: " + missing.join(",") + ")" : "") +
        (extra.length ? " (unknown: " + extra.join(",") + ")" : ""));
    }
  })();
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
  // /assets/json/items_index.json. Items outside this ~500-item index (rare/PvP
  // gear) still get their real icon from the sharded icon index below — they
  // just have no /items page to link to.
  var itemsIndex = null;
  var itemsIndexPromise = null;

  // Fallback icon lookup for everything else a player can equip: the full
  // catalog is baked by generateAnalyzerPage.py into
  // /assets/json/item_icons/<id//ICON_SHARD_SIZE>.json as
  // {id: [icon, quality(, 1 when two-handed)]}.
  // We know the equipped ids before we need their icons, so only the few buckets
  // they land in are ever fetched.
  var ICON_SHARD_SIZE = 1000;
  var iconIndex = {};          // merged id -> shard entry of loaded shards
  var iconShardPromises = {};  // bucket -> in-flight/settled fetch

  function loadIconShards(ids) {
    var known = window.ITEM_ICON_BUCKETS || [];
    var exists = {};
    known.forEach(function (b) { exists[b] = true; });
    var wanted = {};
    ids.forEach(function (id) {
      var b = Math.floor(id / ICON_SHARD_SIZE);
      // A bucket the build never wrote means "no equippable items in that id
      // range" — an unknown item, which legitimately stays a questionmark.
      if (exists[b]) wanted[b] = true;
    });
    return Promise.all(Object.keys(wanted).map(function (b) {
      if (!iconShardPromises[b]) {
        // A bucket that IS baked but won't load is a broken deploy, not an
        // unknown item: let it reject so the caller surfaces an error.
        iconShardPromises[b] = fetch("/assets/json/item_icons/" + b + ".json")
          .then(function (r) {
            if (!r.ok) {
              var err = new Error("item icon shard " + b + " failed: HTTP " + r.status);
              err.dataError = true;
              throw err;
            }
            return r.json();
          })
          .then(function (o) {
            Object.keys(o).forEach(function (id) { iconIndex[id] = o[id]; });
          });
      }
      return iconShardPromises[b];
    }));
  }

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

  // Spec-independent gem/enchant catalog: { gems: {<gemItemId>: {...}},
  // enchants: {<enchantId>: {..., itemId, spellId}} }. Baked once by
  // generateSpecPages.py so the per-slot cells can draw the real icon/name for
  // ANY gem/enchant a player runs — not just the current spec's top combo — and
  // link an enchant by its scroll item/spell rather than mis-reading the
  // enchant_id as an item id.
  var gxIndex = { gems: {}, enchants: {} };
  var gxIndexPromise = null;

  function loadGemEnchantIndex() {
    if (gxIndexPromise) return gxIndexPromise;
    gxIndexPromise = fetch("/assets/json/gem_enchant_index.json")
      .then(function (r) { return r.ok ? r.json() : {}; })
      .then(function (o) {
        gxIndex = { gems: (o && o.gems) || {}, enchants: (o && o.enchants) || {} };
        return gxIndex;
      })
      .catch(function () { gxIndex = { gems: {}, enchants: {} }; return gxIndex; });
    return gxIndexPromise;
  }

  // bonus id -> item quality, baked by generateAnalyzerPage.py from the same
  // data/static/bonus_quality_map.json the spec pages use. A Mythic+ drop is
  // catalogued as *rare* and promoted to epic by a quality bonus id, so the
  // catalog's base quality alone paints the wrong rim on the player's own gear.
  var bonusQuality = null;
  var bonusQualityPromise = null;

  function loadBonusQuality() {
    if (bonusQualityPromise) return bonusQualityPromise;
    bonusQualityPromise = fetch("/assets/json/bonus_quality.json")
      .then(function (r) {
        // Every build writes this file, so a miss is a broken deploy, not a data
        // gap. Rejecting keeps a silently empty map from re-introducing the very
        // mis-colouring this table exists to fix.
        if (!r.ok) {
          var err = new Error("bonus quality map failed: HTTP " + r.status);
          err.dataError = true;
          throw err;
        }
        return r.json();
      })
      .then(function (o) { bonusQuality = o; return o; });
    return bonusQualityPromise;
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
  // `excludeIds` drops picks already worn in a sibling slot of an interchangeable
  // pair (unique-equipped rings/trinkets); when that empties out the TOP/SIM set,
  // we still fall back to `common` so the slot always shows *some* suggestion.
  // Returns [{pick, kind}], kind in {sim, top, meta}.
  function displayTargets(metaSlots, slotName, excludeIds) {
    var names = (GROUPS[slotName] || slotName).split(",");
    var seen = {}, out = [];
    excludeIds = excludeIds || {};
    function add(pick, kind) {
      if (!pick || pick.id == null || seen[pick.id] || excludeIds[pick.id]) return;
      seen[pick.id] = true; out.push({ pick: pick, kind: kind });
    }
    names.forEach(function (n) { var m = metaSlots[n]; if (m && m.sim) add(m.sim, "sim"); });
    names.forEach(function (n) { var m = metaSlots[n]; if (m) (m.top || []).forEach(function (t) { add(t, "top"); }); });
    if (!out.length) {
      names.forEach(function (n) { var m = metaSlots[n]; if (m && m.common) add(m.common, "meta"); });
    }
    return out;
  }

  // Icon/quality (and slug, when the item has its own page) for one of the
  // player's items: the popular-items manifest first, then the full catalog
  // shard. Only an item in neither is drawn as a questionmark.
  function userMeta(id) {
    var hit = itemsIndex && itemsIndex[id];
    if (hit) return hit;
    var shard = iconIndex[id];
    return shard ? { icon: shard[0], quality: shard[1], slug: null } : null;
  }

  // The rarity a player's item actually shows: its catalog quality, overridden by
  // any quality-carrying bonus id on it. Same rule as generateSpecPages.py's
  // convert_slots (truthy-only, last bonus wins), so a slot's "yours" and "meta
  // pick" tiles can never disagree about the same item.
  function resolveQuality(baseQuality, bonusIds) {
    var q = baseQuality;
    (bonusIds || []).forEach(function (b) {
      var bq = bonusQuality[b];
      if (bq) q = bq;
    });
    return q;
  }

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

  // Where an item points: its own /items page when the site has one, else
  // Wowhead.
  function itemLink(id, slug, data, specId) {
    var params = wowheadParams(data || {}, specId);
    return slug
      ? { href: "/items/" + esc(slug) + (specId != null ? "?spec=" + specId : ""), target: "", params: params }
      : { href: "https://www.wowhead.com/item=" + id + (params ? "?" + params.replace(/^&/, "") : ""),
          target: ' target="_blank" rel="noopener"', params: params };
  }

  // One clickable, Wowhead-tooltipped item icon. `data` supplies bonus/ench/gems
  // for the tooltip; `extra` is overlay markup (status glyph / chip); `label` is
  // the link's accessible name — the rows are icons only, so without it a
  // screen reader reads the whole link as nothing at all.
  function iconEl(id, iconName, quality, slug, data, specId, cls, extra, label) {
    var q = quality != null ? " border-quality-" + quality : "";
    var src = iconName ? "/data/icons/" + esc(iconName) + ".png" : QUESTION;
    var lk = itemLink(id, slug, data, specId);
    var params = lk.params, href = lk.href, target = lk.target;
    return '<a class="an-icon' + q + (cls ? " " + cls : "") + '"' + target +
      (label ? ' aria-label="' + esc(label) + '"' : "") +
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

  // Does this meta pick occupy both hands? Baked as `two_handed` on the pick
  // itself (spec_meta) and as the third element of an icon shard entry; either
  // answer is derived from the same commonUtils.occupies_both_hands, so the
  // shard covers specs whose meta JSON predates the flag. The shard mark knows
  // nothing about Titan's Grip — safe here because only twoHandPicks() reads it,
  // and that runs only for a spec with no OFF_HAND slot of its own.
  function isTwoHanded(pick) {
    if (!pick) return false;
    if (pick.two_handed) return true;
    var shard = iconIndex[pick.id];
    return !!(shard && shard[2]);
  }

  // A spec whose top main hand is a two-hander has no OFF_HAND slot in its meta
  // JSON at all — generateSpecPages drops it. Keep only the two-handed picks of
  // the MAIN_HAND slot so the off-hand can be scored against them.
  function twoHandPicks(mhSlot) {
    if (!mhSlot) return null;
    var sim = isTwoHanded(mhSlot.sim) ? mhSlot.sim : null;
    var top = (mhSlot.top || []).filter(isTwoHanded);
    var common = isTwoHanded(mhSlot.common) ? mhSlot.common : null;
    if (!sim && !top.length && !common) return null;
    return { sim: sim, top: top, common: common };
  }

  // Lay the scored tiles out in the spec page's armory grouping. Both panes of a
  // block are always emitted (an empty one keeps its half of the row, same as
  // spec_page.html's unconditional flex-fill divs); a block with no tiles at all
  // — a spec whose whole weapon/trinket pair fell out — is dropped entirely.
  function layout(tilesBySlot) {
    return DISPLAY_BLOCKS.map(function (block) {
      var panes = [block.left, block.right].map(function (pane) {
        return pane.slots.map(function (s) { return tilesBySlot[s] || ""; }).join("");
      });
      if (!panes[0] && !panes[1]) return "";
      return '<div class="an-block">' +
        [block.left, block.right].map(function (pane, i) {
          return '<div class="an-col">' +
            (pane.head ? '<div class="an-col-head">' + esc(pane.head) + "</div>" : "") +
            panes[i] + "</div>";
        }).join("") +
        "</div>";
    }).join("");
  }

  function render(meta, parsed) {
    var specId = meta.spec_id;
    var disp = (window.SPEC_DISPLAY || {})[specId] || { name: meta.spec, class: meta.class, icon: null };
    // Keyed by slot, not a flat list: the scoring loop below walks SLOT_ORDER,
    // but layout() places the tiles in the armory grouping afterwards.
    var tilesBySlot = {};
    var comparable = 0, good = 0;
    // How wide the modifier zone has to be: the busiest slot in THIS report
    // decides, so the zone's leading hairline lands on the same x on every row.
    // Combined into --an-mod-slots below.
    var maxGems = 0, anyEnch = false;

    // Equipping the meta two-hander costs the player BOTH weapons, so a
    // one-hand + off-hand player has to be told about both slots, not just the
    // main hand. The spec has no OFF_HAND meta slot in that case, so we score
    // the off-hand against the two-handed MAIN_HAND picks through a virtual
    // slot — every tile behind it (gems, enchants, badges) then falls out of the
    // normal per-slot path. Specs that really do use an off-hand keep their own
    // OFF_HAND slot and never reach this.
    var metaSlots = meta.slots;
    var twoHandSwap = false;
    if (!metaSlots.OFF_HAND && parsed.slots.OFF_HAND && parsed.slots.MAIN_HAND) {
      var th = twoHandPicks(metaSlots.MAIN_HAND);
      var already = th && (
        (th.sim && th.sim.id === parsed.slots.MAIN_HAND.id) ||
        (th.common && th.common.id === parsed.slots.MAIN_HAND.id) ||
        th.top.some(function (t) { return t.id === parsed.slots.MAIN_HAND.id; })
      );
      if (th && !already) {
        metaSlots = Object.assign({}, metaSlots, { OFF_HAND: th });
        twoHandSwap = true;
      }
    }

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

    // Ids already suggested to an earlier slot of an interchangeable pair, keyed
    // by group (FINGER / TRINKET). A unique-equipped item must be recommended to
    // at most one slot of the pair, so when both slots are off-meta the second
    // one falls through to the next distinct pick instead of repeating the first.
    var groupSuggested = {};

    SLOT_ORDER.forEach(function (slotName) {
      var metaSlot = metaSlots[slotName];
      var user = parsed.slots[slotName];
      if (!metaSlot || !user) return; // only compare slots both sides have

      var acc = acceptableIds(metaSlots, slotName);
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
      var slotLabel = slotName.replace(/_/g, " ").toLowerCase();
      var yours = iconEl(user.id, um && um.icon,
        resolveQuality(um && um.quality, user.bonus), um && um.slug,
        user, specId, "an-user", mark, "your " + slotLabel + " item");

      // Ids we must not suggest for this slot: anything worn in a sibling slot of
      // an interchangeable pair (unique-equipped — can't wear a second copy) plus
      // anything already recommended to an earlier slot of the same pair (so the
      // two off-meta slots get distinct suggestions, never the same item twice).
      var grp = groupKey(slotName);
      var suggestedInGroup = groupSuggested[grp] || (groupSuggested[grp] = {});
      var excludeIds = {};
      (GROUPS[slotName] ? GROUPS[slotName].split(",") : []).forEach(function (n) {
        if (n === slotName) return;
        var sib = parsed.slots[n];
        if (sib) excludeIds[sib.id] = true;
      });
      Object.keys(suggestedInGroup).forEach(function (id) { excludeIds[id] = true; });

      // Meta target picks (group-union; SIM/TOP, or the popular fallback), minus
      // the excluded ids above. When excluding empties the TOP/SIM set,
      // displayTargets falls back to the most-popular `common` pick so the slot
      // still shows a distinct suggestion. Record what we suggest so the paired
      // slot skips it.
      var picks = displayTargets(metaSlots, slotName, excludeIds);
      // Only a slot that actually shows a swap (off-meta) reserves its pick; a
      // matched slot shows no target, so recording its picks would wrongly starve
      // an off-meta sibling of the very item it should be told to equip.
      if (!matched) picks.forEach(function (t) { suggestedInGroup[t.pick.id] = true; });
      var targets = picks.map(function (t) {
        return iconEl(t.pick.id, t.pick.icon, t.pick.quality, t.pick.slug, t.pick, specId, "",
          chip(t.kind, t.pick, meta), "meta pick for " + slotLabel);
      });
      var targetHtml = targets.length ? '<div class="an-targets">' + targets.join("") + "</div>" : "";

      // Owned-upgrade: is an acceptable meta item sitting in the player's bags?
      var inBagsHtml = "";
      if (!matched) {
        // A two-hander suggested for the OFF_HAND tile is a main-hand item, so
        // it sits under the export's main_hand bag group.
        var bagGroup = parsed.bags[
          twoHandSwap && slotName === "OFF_HAND" ? "MAIN_HAND" : groupKey(slotName)
        ] || {};
        var wantIds = [];
        acc.sim.forEach(function (i) { if (!excludeIds[i]) wantIds.push(i); });
        acc.top.forEach(function (i) { if (!excludeIds[i]) wantIds.push(i); });
        if (!hasIdeal) acc.common.forEach(function (i) { if (!excludeIds[i]) wantIds.push(i); });
        var ownedId = wantIds.filter(function (i) { return bagGroup[i]; })[0];
        if (ownedId != null) {
          // Trails the swap it belongs to ("change to this — you already own
          // it"), inside the item zone, so it never reaches the modifier columns.
          inBagsHtml = '<span class="an-inbags">' +
            '<i class="material-symbols-rounded">backpack</i> In your bags</span>';
        }
      }

      // GEM cell (fixed left of the footer): each socketed gem drawn as a small
      // gem-style icon with a ✓ / ⚠ / ✕ corner glyph. Off-combo and over-used
      // gems are flagged; a gem the top combo knows uses its own icon, an
      // off-combo gem falls back to a questionmark + Wowhead tooltip.
      // Does this slot carry a gem/enchant problem (off-combo, over-used, or a
      // missing-but-expected enchant)? Drives the tile's amber "item's fine but
      // fix a socket/enchant" colour when the item itself is already a meta pick.
      var footIssue = false;

      var gemCell = "";
      if (meta.gem_combo && user.gems && user.gems.length) {
        gemCell = user.gems.map(function (gid) {
          var st = classifyAgainstCombo(gid, counts.gems, gemBudget);
          if (st !== "ok") footIssue = true;
          // Combo entry first (carries qty context); otherwise the global gem
          // catalog so an off-combo gem still shows its real icon/name.
          var info = gemInfo[gid] || gxIndex.gems[gid] || { id: gid };
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
            footIssue = true;
            enchCell = '<span class="an-icon an-icon-sm an-gem-slot an-ench-missing">' +
              '<img src="' + QUESTION + '" alt="">' + statusMark("bad", "Missing enchant") + "</span>";
          }
        } else {
          var est = classifyAgainstCombo(user.enchant, counts.enchants, enchBudget);
          if (est !== "ok") footIssue = true;
          // Combo entry first (carries qty context); otherwise the global enchant
          // catalog so an off-combo enchant still shows its real icon/name. Link
          // via the enchant's scroll itemId (or spellId) — NEVER item=<enchantId>,
          // which resolves to an unrelated item that happens to share the number.
          var einfo = enchInfo[user.enchant] || gxIndex.enchants[user.enchant] || { id: user.enchant };
          var whRef = einfo.itemId ? "item=" + einfo.itemId
            : einfo.spellId ? "spell=" + einfo.spellId
            : "item=" + user.enchant;
          var etip = est === "ok" ? ((einfo.name || "Enchant") + " matches the top combo.")
            : est === "warn" ? "You use this enchant more times than the top players do."
            : "Off-combo enchant — not in the top players' set.";
          enchCell = auxIconEl(einfo, whRef, "an-gem-slot", statusMark(est, etip));
        }
      }

      // ITEM ZONE — everything about the item itself, packed left: what you
      // wear, the swap to make, and whether you already own the target. This is
      // the row's only growing cell, so it pushes the modifier zone right.
      // Already on a meta pick → no arrow and no target; there's nothing to change.
      var itemZone = '<div class="an-item"><div class="an-yours">' + yours + "</div>" +
        (!matched && targetHtml
          ? '<span class="material-symbols-rounded an-arrow">arrow_right_alt</span>' + targetHtml
          : "") +
        inBagsHtml + "</div>";

      // MODIFIER ZONE — enchant then gems (the order the spec page's render_slot
      // uses), packed hard against the row's right edge so nothing can float in
      // the middle of a reserved cell. The zone itself is a fixed width, which is
      // what keeps its leading hairline on one x down the whole column.
      if (gemCell) maxGems = Math.max(maxGems, user.gems.length);
      if (enchCell) anyEnch = true;
      var foot = '<div class="an-foot">' +
        '<div class="an-foot-ench">' + enchCell + "</div>" +
        '<div class="an-foot-gems">' + gemCell + "</div>" +
        "</div>";

      // Tile colour is now purely about "what should I fix here":
      //   red  = the item itself isn't a meta pick (swap it),
      //   amber = the item is fine but a gem/enchant is off,
      //   green = item, gems and enchants all check out.
      // (The SIM/TOP/most-popular distinction lives on the target badges, not
      // the tile, since which flavour of "meta" a slot matched didn't tell the
      // user anything actionable.)
      var tileState = !matched ? "bad" : (footIssue ? "warn" : "good");

      tilesBySlot[slotName] =
        '<div class="an-tile an-' + tileState + '">' +
          '<div class="an-slot-label">' + esc(slotName.replace(/_/g, " ")) + "</div>" +
          itemZone +
          foot +
        "</div>";
    });

    if (comparable === 0) {
      showError("Couldn't match any gear slots. Make sure you pasted the full SimC export (the lines like head=,id=...).");
      return;
    }

    var score = Math.round((good / comparable) * 100);
    var iconHtml = disp.icon ? '<img src="/data/icons/' + esc(disp.icon) + '.jpg" class="an-spec-icon me-3" alt="">' : "";

    // The page asks one question, so its answer gets its own band: the score as
    // a progress ring, big enough to read before anything else on the card.
    // The same hi/mid/lo class drives the ring colour and the figure, so the
    // thresholds stay defined in one place (analyzer.css).
    var scoreClass = score >= 80 ? "an-score-hi" : score >= 50 ? "an-score-mid" : "an-score-lo";
    var summary =
      '<div class="an-summary ' + scoreClass + '">' +
        '<div class="an-summary-id">' + iconHtml +
          '<div><div class="an-summary-spec">' + esc(disp.name) + " " + esc(disp.class) + "</div>" +
          '<div class="text-xs text-secondary">' + good + " of " + comparable +
            " scored slots on a meta pick</div></div>" +
        "</div>" +
        '<div class="an-summary-score">' +
          '<div class="an-ring" style="--an-pct:' + score + '" role="img"' +
            ' aria-label="' + score + '% meta match">' +
            '<span class="an-ring-arc"></span>' +
            '<span class="an-ring-pct">' + score + "%</span></div>" +
          '<div class="an-ring-label">meta match</div>' +
        "</div>" +
      "</div>";

    // Width of the modifier zone, in icon slots: the busiest slot's gem count
    // plus one for the enchant column. Zero means no slot in this report has
    // either, and `an-has-mods` drops the zone (and its divider) entirely.
    var modSlots = maxGems + (anyEnch ? 1 : 0);

    results.innerHTML =
      summary +
      '<div class="an-grid' + (modSlots ? " an-has-mods" : "") +
        '" style="--an-mod-slots:' + modSlots + '">' + layout(tilesBySlot) + "</div>" +
      comboSection(meta.gem_combo, counts.gems, "gem", meta) +
      comboSection(meta.enchant_combo, counts.enchants, "enchant", meta) +
      '<p class="text-xxs text-secondary mt-2 mb-0">' +
        'Talents aren’t compared yet.</p>';

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
      loadGemEnchantIndex(),
      loadBonusQuality(),
    ])
      .then(function (out) {
        var meta = out[0];
        // Second hop, now that we know which equipped items the popular-items
        // manifest doesn't cover: pull just those items' icon shards. Bag items
        // are never drawn, so they need no icons.
        var wanted = Object.keys(parsed.slots)
          .map(function (s) { return parsed.slots[s].id; })
          .filter(function (id) { return !(itemsIndex && itemsIndex[id]); });
        // A player wearing an off-hand on a spec with no OFF_HAND meta slot also
        // needs the main-hand picks' shards, since that is where the "occupies
        // both hands" mark lives — items_index carries no such flag.
        if (!meta.slots.OFF_HAND && parsed.slots.OFF_HAND && meta.slots.MAIN_HAND) {
          var mh = meta.slots.MAIN_HAND;
          [mh.sim, mh.common].concat(mh.top || []).forEach(function (p) {
            if (p && p.id != null) wanted.push(p.id);
          });
        }
        return loadIconShards(wanted).then(function () { render(meta, parsed); });
      })
      .catch(function (err) {
        showError(err && err.dataError
          ? "Couldn't load the item data. Reload the page and try again."
          : "No meta data available for this spec yet. Check back after the next update.");
      });
  }

  analyzeBtn.addEventListener("click", analyze);
  if (clearBtn) clearBtn.addEventListener("click", function () {
    input.value = "";
    results.innerHTML = '<p class="text-sm text-secondary mb-0">Paste your export and press <strong>Analyze</strong> to see your report.</p>';
    input.focus();
  });
})();
