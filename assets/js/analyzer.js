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
  // Scoring order for the report. Load-bearing beyond mere display: an
  // interchangeable pair's meta picks are dealt best-first in this order (so the
  // top pick lands on FINGER_1 / TRINKET_1), and the enchant quota walks slots
  // in this order too.
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
    var out = { classId: null, specToken: null, slots: {}, bags: {}, hasBags: false, talents: null };
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
      // The active loadout is the first bare `talents=` line. Saved loadouts are
      // commented (`# talents=`) and handled by the `#` branch above; the
      // separate `omnium_talents=` line is a different key and never matches.
      if (key === "talents") {
        if (out.talents == null) out.talents = rest.trim();
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

  // Score one slot's equipped item against its (group-wide) meta targets.
  // Returns null when the slot has no meta data at all. Used by both the render
  // loop and planGroupTargets(), so the two can never disagree about which slot
  // of an interchangeable pair still needs a suggestion.
  function scoreSlot(metaSlots, slotName, user) {
    var acc = acceptableIds(metaSlots, slotName);
    var hasIdeal = acc.sim.size > 0 || acc.top.size > 0;
    if (!hasIdeal && acc.common.size === 0) return null; // truly no meta data
    var status = acc.sim.has(user.id) ? "sim"
      : acc.top.has(user.id) ? "top"
      : hasIdeal ? "off"                    // has TOP/SIM, user misses
      : acc.common.has(user.id) ? "meta"    // fallback: popular pick
      : "off";                              // misses the popular fallback
    return { status: status, matched: status !== "off" };
  }

  // The meta picks to display for a slot: the group-union of SIM + TOP, or the
  // group-union of `common` when there's no TOP/SIM (the fallback target).
  // `excludeIds` drops picks that can't be suggested here (an item already worn
  // in a sibling slot of an interchangeable pair, or one dealt to that sibling);
  // when that empties out the TOP/SIM set, we still fall back to `common` so the
  // slot always shows *some* suggestion.
  // For an interchangeable pair this returns the whole group union — it's
  // planGroupTargets() that deals that union across the pair's slots rather than
  // handing it all to one of them.
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

  // Which meta picks each slot of an interchangeable pair (FINGER / TRINKET) is
  // told to equip: { slotName: [{pick, kind}] }.
  //
  // A unique-equipped ring/trinket may only be recommended once, so the group's
  // distinct picks are DEALT across the pair's off-meta slots — best pick to the
  // first slot, second to the second, then round-robin. Handing the whole union
  // to the first slot instead (what the old per-slot reservation did) starved
  // the second one into the unbadged most-popular `common` fallback even though
  // a real TOP pick was still unclaimed.
  function planGroupTargets(metaSlots, parsed) {
    var plan = {};
    var groups = {};  // FINGER / TRINKET -> its slots, in SLOT_ORDER order
    SLOT_ORDER.forEach(function (s) {
      if (!GROUPS[s] || !metaSlots[s] || !parsed.slots[s]) return;
      var g = groupKey(s);
      (groups[g] || (groups[g] = [])).push(s);
    });

    Object.keys(groups).forEach(function (g) {
      var slotsG = groups[g];
      // Worn anywhere in the pair: unique-equipped, so never suggestable. A slot
      // that IS wearing a pick scores as matched and shows no target of its own,
      // so excluding its item costs the off-meta sibling nothing.
      var worn = {};
      slotsG.forEach(function (s) { worn[parsed.slots[s].id] = true; });

      var off = slotsG.filter(function (s) {
        var sc = scoreSlot(metaSlots, s, parsed.slots[s]);
        return sc && !sc.matched;
      });
      if (!off.length) return;

      // Group union minus the worn ids — either slot name of the pair yields the
      // same list (GROUPS maps both to the same names).
      var pool = displayTargets(metaSlots, off[0], worn);
      // Best-first, so the top pick lands on the first slot: SIM picks keep the
      // lead displayTargets already gave them, TOP picks sort by popularity.
      // (sort is stable, so equal-pct picks keep the baked order.)
      var sims = pool.filter(function (t) { return t.kind === "sim"; });
      var rest = pool.filter(function (t) { return t.kind !== "sim"; })
        .sort(function (a, b) { return (b.pick.pct || 0) - (a.pick.pct || 0); });
      pool = sims.concat(rest);

      off.forEach(function (s) { plan[s] = []; });
      var used = {};
      pool.forEach(function (t, i) {
        plan[off[i % off.length]].push(t);
        used[t.pick.id] = true;
      });

      // Fewer distinct picks than off-meta slots: the leftover slot still gets a
      // suggestion — the most-popular pick nothing else has claimed.
      off.forEach(function (s) {
        if (plan[s].length) return;
        var ex = {};
        Object.keys(worn).forEach(function (id) { ex[id] = true; });
        Object.keys(used).forEach(function (id) { ex[id] = true; });
        plan[s] = displayTargets(metaSlots, s, ex);
        plan[s].forEach(function (t) { used[t.pick.id] = true; });
      });
    });
    return plan;
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

  // The single place that turns an enchant into a Wowhead ref. Prefer the
  // enchanting scroll item, else the enchant/rune spell (DK weapon runes have no
  // scroll item, only a spellId), else the raw enchant id as a last resort. Never
  // link item=<enchant_id>: that number collides with an unrelated item and gives
  // a useless tooltip. `eid` is the SimC enchant_id; `einfo` is the combo entry or
  // catalog entry that may carry itemId/spellId. gxIndex backfills either field so
  // a combo entry missing one still resolves via the full enchant catalog. Every
  // enchant render site routes through here so the itemId-else-spellId decision
  // lives in exactly one place.
  function enchantRef(eid, einfo) {
    einfo = einfo || {};
    var cat = gxIndex.enchants[eid] || {};
    var itemId = einfo.itemId != null ? einfo.itemId : cat.itemId;
    var spellId = einfo.spellId != null ? einfo.spellId : cat.spellId;
    return itemId ? "item=" + itemId
      : spellId ? "spell=" + spellId
      : "item=" + eid;
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
      var whRef = isEnch ? enchantRef(e.id, e) : "item=" + e.id;
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

  // ---- Talents -----------------------------------------------------------
  //
  // The player's talents come as a Blizzard loadout string on the export's
  // `talents=` line. We decode it in the browser against the per-spec tree data
  // (fullNodeOrder + nodes) baked into meta.talents by generateSpecPages.py, and
  // compare it to the most-run meta build for the player's hero tree.
  //
  // The decode is Blizzard's stable "serialization version 2" bitstream (see
  // Blizzard_ClassTalentImportExport.lua): a header of an 8-bit version, a 16-bit
  // spec id and a 128-bit tree hash (ignored — a zero hash is valid), then, for
  // every node in fullNodeOrder, a `selected` bit and, when set, a `purchased`
  // bit, an optional 6-bit partial rank and an optional 2-bit choice index. All
  // that changes patch to patch is the tree data, which we pull from Raidbots.
  var TALENT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  var TALENT_CHAR_IDX = (function () {
    var m = {};
    for (var i = 0; i < TALENT_CHARS.length; i++) m[TALENT_CHARS.charAt(i)] = i;
    return m;
  })();
  var LOADOUT_VERSION = 2;

  // Decode a loadout string against a tree ({fullNodeOrder, nodes}). Returns
  // { version, specId, selected: {nodeId: {entryIndex, rank, purchased}} }, or
  // { version, unsupported: true } when the format version is one we don't read,
  // or null when the string is malformed.
  function decodeLoadout(code, tree) {
    if (!code || !tree || !tree.fullNodeOrder) return null;
    var bits = [];
    for (var i = 0; i < code.length; i++) {
      var v = TALENT_CHAR_IDX[code.charAt(i)];
      if (v == null) return null; // not a loadout string (invalid char)
      for (var b = 0; b < 6; b++) bits.push((v >> b) & 1); // 6 bits per char, LSB first
    }
    var p = 0;
    function read(n) {
      var r = 0;
      for (var i = 0; i < n; i++) {
        if (p >= bits.length) return null; // ran past the end
        r |= bits[p++] << i;
      }
      return r;
    }
    var version = read(8);
    if (version !== LOADOUT_VERSION) return { version: version, unsupported: true };
    var specId = read(16);
    for (var h = 0; h < 16; h++) read(8); // tree hash, not validated
    var selected = {};
    var order = tree.fullNodeOrder;
    for (var n = 0; n < order.length; n++) {
      var isSel = read(1);
      if (isSel == null) break; // tolerate a truncated tail rather than throwing
      if (!isSel) continue;
      var isPurchased = read(1);
      var rank = null, entryIndex = 0;
      if (isPurchased) {
        if (read(1)) rank = read(6);     // partially ranked
        if (read(1)) entryIndex = read(2); // choice node: which entry
      }
      selected[order[n]] = { entryIndex: entryIndex || 0, rank: rank, purchased: !!isPurchased };
    }
    return { version: version, specId: specId, selected: selected };
  }

  function ttNode(tree, nid) { return tree.nodes[nid] || tree.nodes[String(nid)] || null; }
  function ttEntry(node, idx) {
    if (!node || !node.entries || !node.entries.length) return null;
    return node.entries[idx] || node.entries[0];
  }

  // Purchased picks as {nodeId: entryIndex}. Excludes free/granted nodes and the
  // hero-tree selection node so the comparison reflects real talent choices.
  function ttTaken(decoded, tree) {
    var out = {};
    var sel = (decoded && decoded.selected) || {};
    Object.keys(sel).forEach(function (nid) {
      if (!sel[nid].purchased) return;
      var node = ttNode(tree, nid);
      if (!node || node.free || node.g === "sub") return;
      out[nid] = sel[nid].entryIndex || 0;
    });
    return out;
  }

  // Which hero tree a build sits in: the subTreeId most of its picks belong to.
  function ttHeroTree(decoded, tree) {
    var sel = (decoded && decoded.selected) || {};
    var counts = {};
    Object.keys(sel).forEach(function (nid) {
      if (!sel[nid].purchased) return;
      var node = ttNode(tree, nid);
      if (node && node.subTreeId != null) {
        var k = String(node.subTreeId);
        counts[k] = (counts[k] || 0) + 1;
      }
    });
    var best = null, bc = 0;
    Object.keys(counts).forEach(function (k) { if (counts[k] > bc) { bc = counts[k]; best = k; } });
    return best;
  }

  // One talent icon+name, coloured by its state vs the meta build.
  function ttTalentEl(entry, node, stateCls) {
    var icon = entry && entry.icon ? "/data/icons/" + esc(entry.icon) + ".png" : QUESTION;
    var sp = entry && entry.spellId;
    var href = sp ? "https://www.wowhead.com/spell=" + sp : "#";
    var wh = sp ? ' data-wowhead="spell=' + sp + '"' : "";
    var rank = "";
    if (node && node.maxRanks > 1) rank = ' <span class="an-tt-rank">' + node.maxRanks + "/" + node.maxRanks + "</span>";
    return '<a class="an-tt-talent ' + stateCls + '" target="_blank" rel="noopener" href="' + href + '"' + wh + '>' +
      '<img src="' + icon + '" alt="" loading="lazy" onerror="this.src=\'' + QUESTION + '\'">' +
      '<span class="an-tt-name">' + esc(entry ? entry.name : "?") + rank + "</span></a>";
  }

  var TT_GROUP_LABEL = { class: "Class", spec: "Spec", hero: "Hero" };

  function heroTreeName(T, h) {
    var st = (T.subTrees || {})[h] || (T.subTrees || {})[String(h)];
    return st && st.name ? st.name : "Hero tree";
  }

  // Decode the pasted build + the chosen meta build and diff them. Pure data so
  // render() can read the talent score for the shared header, and the card can be
  // re-rendered on a hero switch. Returns null when the spec has no talent data,
  // { error } when the string can't be used, else the diff.
  function talentDiff(meta, parsed, T, compareHero) {
    if (!T || !T.nodes) return null;
    if (!parsed.talents) return { error: "No talent string found in your export." };
    var pd = decodeLoadout(parsed.talents, T);
    if (!pd) return { error: "Couldn't read your talent string." };
    if (pd.unsupported) return { error: "Your talent string uses a newer format we can't read yet." };
    if (pd.specId && meta.spec_id && pd.specId !== meta.spec_id) {
      return { error: "Your talent string is for a different spec than your gear." };
    }
    var playerTaken = ttTaken(pd, T);
    var playerHero = ttHeroTree(pd, T);
    var popular = T.popular_hero != null ? String(T.popular_hero) : null;
    var byHero = T.meta_by_hero || {};
    // Which meta build to compare against: the caller's pick, else the player's
    // own hero tree, else the popular one.
    var hero = compareHero != null ? String(compareHero) : null;
    if (!hero || !byHero[hero]) hero = (playerHero && byHero[playerHero]) ? playerHero : popular;
    var metaTaken = {};
    if (hero && byHero[hero] && byHero[hero].loadout) {
      var md = decodeLoadout(byHero[hero].loadout, T);
      if (md && !md.unsupported) metaTaken = ttTaken(md, T);
    }
    var metaIds = Object.keys(metaTaken);
    var matches = 0, missing = [];
    metaIds.forEach(function (nid) {
      if (Object.prototype.hasOwnProperty.call(playerTaken, nid) && playerTaken[nid] === metaTaken[nid]) matches++;
      else missing.push(nid);
    });
    var haveScore = metaIds.length > 0;
    return {
      playerTaken: playerTaken, metaTaken: metaTaken, playerHero: playerHero, hero: hero,
      popular: popular, byHero: byHero, matches: matches, missing: missing,
      haveScore: haveScore, score: haveScore ? Math.round((matches / metaIds.length) * 100) : null,
    };
  }

  // Render the talents card from a precomputed diff. The match figure lives in
  // the shared header now, so the card carries the hero-tree switch, a copy-meta-
  // loadout button, the build (Class tree | hero choice column | Spec tree, the
  // hero tree drawn spec-page style as just its choice nodes), and the missing list.
  function talentCardHtml(meta, parsed, T, diff) {
    function wrap(inner) { return '<div class="an-card an-talents-card">' + inner + "</div>"; }
    if (!T || !T.nodes || !diff) return "";
    if (diff.error) {
      return wrap('<div class="an-card-head">Talents</div>' +
        '<p class="text-xs text-secondary mb-0">' + esc(diff.error) + "</p>");
    }
    var playerTaken = diff.playerTaken, metaTaken = diff.metaTaken, playerHero = diff.playerHero;
    var hero = diff.hero, popular = diff.popular, byHero = diff.byHero;
    var haveScore = diff.haveScore, missing = diff.missing;
    function heroName(h) { return heroTreeName(T, h); }

    // Head: title + copy-meta-loadout button.
    var copyBtn = "";
    if (hero && byHero[hero] && byHero[hero].loadout) {
      copyBtn = '<button type="button" class="btn btn-sm btn-outline-secondary mb-0 an-tt-copy"' +
        ' data-loadout="' + esc(byHero[hero].loadout) + '">' +
        '<i class="material-symbols-rounded align-middle text-sm me-1">content_copy</i>Copy meta loadout</button>';
    }
    var head = '<div class="an-tt-headrow"><div class="an-card-head">Talents</div>' + copyBtn + "</div>";

    var heroKeys = Object.keys(byHero);

    var heroNote = "";
    if (playerHero && popular && playerHero !== popular) {
      heroNote = '<p class="an-tt-note text-xs mb-2">' +
        '<i class="material-symbols-rounded align-middle me-1">info</i>' +
        "You're playing <strong>" + esc(heroName(playerHero)) + "</strong>. Most meta " +
        esc(meta.spec || "") + " players run <strong>" + esc(heroName(popular)) +
        "</strong> — you're being compared to the " + esc(heroName(hero)) + " build.</p>";
    }

    function has(o, k) { return Object.prototype.hasOwnProperty.call(o, k); }
    function isActive(nid) { var n = ttNode(T, nid); return !!(n && (n.free || has(playerTaken, nid))); }
    function nodeState(nid, node) {
      if (node.free) return "an-free an-have";
      if (has(playerTaken, nid)) {
        if (!haveScore) return "an-have";
        return metaTaken[nid] === playerTaken[nid] ? "an-have an-match" : "an-have an-off";
      }
      if (haveScore && has(metaTaken, nid)) return "an-miss";
      return "an-dim";
    }
    function ntypeOf(node) {
      if (node.type === "tiered") return "passive";
      if (node.entries && node.entries.length > 1) return "choice";
      var e = node.entries && node.entries[0];
      return (e && e.type) || "passive";
    }
    // Meta pick-rate badge for a node (spec-page style: hidden for free nodes,
    // absent when spec_meta carries no percentage for the id).
    var nodePct = T.node_pct || {};
    function pctBadge(nid, node) {
      if (node.free) return "";
      var p = nodePct[nid] != null ? nodePct[nid] : nodePct[String(nid)];
      if (p == null) return "";
      return '<span class="tt-badge">' + p + "%</span>";
    }
    // The icon + badges for a node, drawn spec-page style: choice nodes get the
    // octagon-border + arrows, passive/active/tiered keep the round/tiered icon
    // (border-radius comes from the outer .tt-node[data-ntype] rule). The
    // analyzer state class (an-match/off/miss/...) is layered on the wrapper.
    function nodeInner(nid, node) {
      var have = has(playerTaken, nid);
      var idx = have ? playerTaken[nid] : (metaTaken[nid] != null ? metaTaken[nid] : 0);
      var entry = ttEntry(node, idx);
      var icon = entry && entry.icon ? "/data/icons/" + esc(entry.icon) + ".png" : QUESTION;
      var sp = entry && entry.spellId;
      var href = sp ? "https://www.wowhead.com/spell=" + sp : "#";
      var wh = sp ? ' data-wowhead="spell=' + sp + '"' : "";
      var alt = esc(entry ? entry.name : "");
      var rankBadge = node.maxRanks > 1 ? '<span class="an-ttn-rank">' + node.maxRanks + "</span>" : "";
      var missBadge = nodeState(nid, node) === "an-miss" ? '<span class="an-ttn-miss">+</span>' : "";
      var badges = rankBadge + missBadge + pctBadge(nid, node);
      if (ntypeOf(node) === "choice") {
        return '<div class="tt-choice-wrapper" style="--border-color:#ffb000;width:100%;height:100%;">' +
          '<div class="arrow-left"></div>' +
          '<div class="tt-octagon-border" style="width:100%;height:100%;">' +
          '<a href="' + href + '" target="_blank" rel="noopener"' + wh + '>' +
          '<img class="tt-octagon" src="' + icon + '" alt="' + alt +
          '" loading="lazy" onerror="this.src=\'' + QUESTION + '\'"></a>' +
          '</div><div class="arrow-right"></div></div>' + badges;
      }
      return '<a href="' + href + '" target="_blank" rel="noopener"' + wh + '>' +
        '<img src="' + icon + '" alt="' + alt +
        '" loading="lazy" onerror="this.src=\'' + QUESTION + '\'"></a>' + badges;
    }
    // A positioned class/spec tree column with connector edges.
    function ttColumn(ids, extraCls) {
      var ns = ids.map(function (id) { return { id: String(id), node: ttNode(T, id) }; })
                  .filter(function (o) { return o.node; });
      if (!ns.length) return "";
      var xs = ns.map(function (o) { return o.node.x; }), ys = ns.map(function (o) { return o.node.y; });
      var minx = Math.min.apply(null, xs) - 150, maxx = Math.max.apply(null, xs) + 150;
      var miny = Math.min.apply(null, ys) - 150, maxy = Math.max.apply(null, ys) + 150;
      var w = Math.max(1, maxx - minx), h = Math.max(1, maxy - miny);
      var L = function (x) { return (x - minx) / w * 100; }, Tp = function (y) { return (y - miny) / h * 100; };
      var inGroup = {}; ns.forEach(function (o) { inGroup[o.id] = true; });
      var lines = ns.map(function (o) {
        return (o.node.next || []).map(function (cid) {
          var c = ttNode(T, cid);
          if (!c || !inGroup[String(cid)]) return "";
          var active = isActive(o.id) && isActive(String(cid));
          return '<line x1="' + L(o.node.x) + '%" y1="' + Tp(o.node.y) + '%" x2="' + L(c.x) +
            '%" y2="' + Tp(c.y) + '%" stroke="' + (active ? "#ffb000" : "#555") + '" stroke-width="2"></line>';
        }).join("");
      }).join("");
      var nodesHtml = ns.map(function (o) {
        return '<div class="tt-node an-ttn ' + nodeState(o.id, o.node) + '" data-ntype="' + ntypeOf(o.node) +
          '" style="left:' + L(o.node.x) + "%;top:" + Tp(o.node.y) + '%">' + nodeInner(o.id, o.node) + "</div>";
      }).join("");
      return '<div class="tt-column tt-tree-column ' + (extraCls || "") + '">' +
        '<svg class="tt-edges">' + lines + "</svg>" + nodesHtml + "</div>";
    }
    // Hero tree centre, spec-page style: a big glowing hero-tree icon with its
    // meta pick-share, a switch affordance (reuses the .an-tt-hero-btn handler by
    // cycling to the next tree), then the tree's CHOICE nodes stacked below.
    function heroColumn(ids) {
      var st = (T.subTrees || {})[hero] || (T.subTrees || {})[String(hero)] || {};
      var icon = st.icon ? "/data/icons/" + esc(st.icon) + ".png" : QUESTION;
      var total = 0, mine = 0;
      heroKeys.forEach(function (h) { total += (byHero[h] && byHero[h].count) || 0; });
      mine = (byHero[hero] && byHero[hero].count) || 0;
      var share = total > 0 ? Math.round(mine / total * 100) : null;
      var multi = heroKeys.length > 1;
      var nextHero = multi ? heroKeys[(heroKeys.indexOf(hero) + 1) % heroKeys.length] : null;
      var sharePill = share != null
        ? '<div class="tt-hero-share" title="' + share + '% of meta builds run this tree">' + share + "%</div>"
        : "";
      var hint = multi
        ? '<span class="tt-hero-switch-hint" aria-hidden="true" title="Click to switch hero tree">' +
          '<i class="material-symbols-rounded">swap_horiz</i></span>'
        : "";
      var switchAttrs = multi
        ? ' class="tt-hero-switch tt-hero-switch-active an-tt-hero-btn" role="button" tabindex="0"' +
          ' data-hero="' + esc(nextHero) + '" title="Click to switch hero tree"'
        : ' class="tt-hero-switch"';
      var choice = ids.map(function (id) { return { id: String(id), node: ttNode(T, id) }; })
        .filter(function (o) { return o.node && o.node.entries && o.node.entries.length > 1; })
        .sort(function (a, b) { return (a.node.y - b.node.y) || (a.node.x - b.node.x); });
      var nodesHtml = choice.map(function (o) {
        return '<div class="an-tt-hnode an-ttn ' + nodeState(o.id, o.node) + '" data-ntype="choice">' +
          nodeInner(o.id, o.node) + "</div>";
      }).join("");
      var heroLabel = esc(st.name || "Hero tree");
      return '<div class="tt-column tt-hero-column an-tt-hero-column">' +
        '<div class="an-tt-hero-head">' +
        '<div class="position-relative d-inline-block">' +
        '<div' + switchAttrs + '>' +
        '<img class="tt-hero-icon" src="' + icon + '" alt="' + heroLabel +
        '" onerror="this.src=\'' + QUESTION + '\'">' + sharePill + hint + "</div></div>" +
        '<div class="an-tt-hero-name">' + heroLabel + "</div></div>" +
        '<div class="an-tt-hero-nodes">' + nodesHtml + "</div></div>";
    }

    var classIds = [], specIds = [], heroIds = [];
    var shownHero = hero;
    Object.keys(T.nodes).forEach(function (nid) {
      var n = T.nodes[nid];
      if (n.g === "class") classIds.push(nid);
      else if (n.g === "spec") specIds.push(nid);
      else if (n.g === "hero" && String(n.subTreeId) === String(shownHero)) heroIds.push(nid);
    });
    var legend =
      '<div class="an-tt-legend">' +
      '<span class="an-tt-key an-key-match">Matches meta</span>' +
      '<span class="an-tt-key an-key-off">Your off-meta pick</span>' +
      '<span class="an-tt-key an-key-miss">Meta runs, you don\'t</span></div>';

    // Positioned tree when nodes carry coordinates; flat chip fallback otherwise.
    var hasCoords = Object.keys(T.nodes).some(function (nid) {
      var n = T.nodes[nid];
      return n && isFinite(n.x) && isFinite(n.y) && (n.x !== 0 || n.y !== 0);
    });
    var buildSection;
    if (hasCoords) {
      buildSection = legend +
        '<div id="static-talent-tree" class="talent-tree-wrapper an-tt-wrapper">' +
          ttColumn(classIds, "") + heroColumn(heroIds) + ttColumn(specIds, "") +
        "</div>";
    } else {
      var chipGroups = { class: [], spec: [], hero: [] };
      Object.keys(playerTaken).forEach(function (nid) {
        var node = ttNode(T, nid);
        if (!node) return;
        var g = node.g === "hero" ? "hero" : node.g === "spec" ? "spec" : "class";
        var entry = ttEntry(node, playerTaken[nid]);
        var st = !haveScore ? "an-tt-plain" : (metaTaken[nid] === playerTaken[nid] ? "an-tt-match" : "an-tt-off");
        chipGroups[g].push({ html: ttTalentEl(entry, node, st), name: entry ? entry.name : "" });
      });
      buildSection = legend + ["class", "spec", "hero"].map(function (g) {
        if (!chipGroups[g].length) return "";
        chipGroups[g].sort(function (a, b) { return a.name.localeCompare(b.name); });
        return '<div class="an-tt-group"><div class="an-tt-group-head">' + TT_GROUP_LABEL[g] +
          '</div><div class="an-tt-talents">' + chipGroups[g].map(function (x) { return x.html; }).join("") + "</div></div>";
      }).join("");
    }

    return wrap(head + heroNote + buildSection);
  }

  // Shared "meta match" header rings: a small Gear ring, a big overall ring
  // (gear and talents weighted 50/50), and a small Talents ring. A null score
  // renders a muted dash.
  function scoreClassOf(pct) {
    return pct == null ? "an-score-none" : pct >= 80 ? "an-score-hi" : pct >= 50 ? "an-score-mid" : "an-score-lo";
  }
  function ringEl(pct, label, sizeCls) {
    var txt = pct == null ? "—" : pct + "%";
    return '<div class="an-mh-ring ' + sizeCls + " " + scoreClassOf(pct) + '">' +
      '<div class="an-ring" style="--an-pct:' + (pct || 0) + '" role="img" aria-label="' + esc(label) + " " + txt + '">' +
        '<span class="an-ring-arc"></span><span class="an-ring-pct">' + txt + "</span></div>" +
      '<div class="an-ring-label">' + esc(label) + "</div></div>";
  }
  function combineScore(gear, tal) {
    var p = [];
    if (gear != null) p.push(gear);
    if (tal != null) p.push(tal);
    return p.length ? Math.round(p.reduce(function (a, b) { return a + b; }, 0) / p.length) : null;
  }
  function matchRings(gear, tal) {
    return '<div class="an-mh-rings">' +
      ringEl(gear, "Gear", "an-mh-small") +
      ringEl(combineScore(gear, tal), "Meta match", "an-mh-big") +
      ringEl(tal, "Talents", "an-mh-small") +
      "</div>";
  }

  // Wire the hero-tree switch once: a click re-renders just the talents card
  // against the chosen tree, using the context stashed by the last render().
  var talentCtx = null;
  // Hero-tree switch: re-diff against the chosen tree, re-render the card, and
  // update the shared header's Talents + overall rings (gear is unaffected).
  results.addEventListener("click", function (ev) {
    var btn = ev.target.closest && ev.target.closest(".an-tt-hero-btn");
    if (!btn || !talentCtx) return;
    var diff = talentDiff(talentCtx.meta, talentCtx.parsed, talentCtx.tree, btn.getAttribute("data-hero"));
    var host = document.getElementById("an-talents");
    if (host) {
      host.innerHTML = talentCardHtml(talentCtx.meta, talentCtx.parsed, talentCtx.tree, diff);
      refreshTalentTooltips(host);
    }
    var talentScore = diff && diff.haveScore ? diff.score : null;
    var ringsBox = results.querySelector(".an-mh-rings");
    if (ringsBox) ringsBox.outerHTML = matchRings(talentCtx.gearScore, talentScore);
  });

  // Copy the currently-compared meta loadout string to the clipboard.
  results.addEventListener("click", function (ev) {
    var btn = ev.target.closest && ev.target.closest(".an-tt-copy");
    if (!btn) return;
    var code = btn.getAttribute("data-loadout") || "";
    function flash() {
      if (btn.dataset.flashing) return;
      btn.dataset.flashing = "1";
      var prev = btn.innerHTML;
      btn.innerHTML = '<i class="material-symbols-rounded align-middle text-sm me-1">check</i>Copied!';
      setTimeout(function () { btn.innerHTML = prev; delete btn.dataset.flashing; }, 1500);
    }
    function fallbackCopy() {
      var ta = document.createElement("textarea");
      ta.value = code; ta.style.position = "fixed"; ta.style.opacity = "0";
      document.body.appendChild(ta); ta.select();
      try { document.execCommand("copy"); } catch (e) { /* best-effort */ }
      document.body.removeChild(ta);
    }
    // Try the async clipboard API; on any failure (e.g. a sandboxed frame that
    // blocks it) fall back to execCommand. Either way the user gets feedback.
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(code).then(flash, function () { fallbackCopy(); flash(); });
    } else {
      fallbackCopy(); flash();
    }
  });

  // The tree geometry (fullNodeOrder + positioned nodes) comes from the fresh,
  // credential-free /assets/json/talent_trees/<spec>.json; the meta loadout
  // strings to compare against come from spec_meta.talents. Merge so the tree
  // stays correct even when spec_meta was baked before the geometry existed.
  function buildTalentTree(meta, treeFile) {
    var mt = meta.talents || null;
    if (treeFile && treeFile.nodes) {
      return {
        fullNodeOrder: treeFile.fullNodeOrder || (mt && mt.fullNodeOrder) || [],
        nodes: treeFile.nodes,
        subTrees: treeFile.subTrees || (mt && mt.subTrees) || {},
        meta_by_hero: (mt && mt.meta_by_hero) || {},
        popular_hero: mt ? mt.popular_hero : null,
        node_pct: (mt && mt.node_pct) || {},
      };
    }
    // No separate tree file (older deploy): fall back to whatever spec_meta holds.
    if (mt && mt.nodes) return mt;
    return null;
  }

  // Per-spec talent tree geometry. Tolerant: a miss just means no tree section.
  function loadTalentTree(specId) {
    return fetch("/assets/json/talent_trees/" + specId + ".json")
      .then(function (r) { return r.ok ? r.json() : null; })
      .catch(function () { return null; });
  }

  function refreshTalentTooltips(host) {
    if (window.$WowheadPower && typeof window.$WowheadPower.refreshLinks === "function") {
      try { window.$WowheadPower.refreshLinks(); } catch (e) { /* best-effort */ }
    }
  }

  function render(meta, parsed, treeFile) {
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

    // How the interchangeable pairs' picks are split, decided up front: the
    // render loop can't work it out slot by slot, since dealing needs to know
    // which slots of a pair are off-meta before the first of them renders.
    // Built from the post-swap metaSlots so it scores the same table the loop does.
    var groupPlan = planGroupTargets(metaSlots, parsed);

    SLOT_ORDER.forEach(function (slotName) {
      var metaSlot = metaSlots[slotName];
      var user = parsed.slots[slotName];
      if (!metaSlot || !user) return; // only compare slots both sides have

      var sc = scoreSlot(metaSlots, slotName, user);
      if (!sc) return; // truly no meta data for this slot

      comparable++;
      var matched = sc.matched;
      if (matched) good++;

      // User's equipped item, with a status overlay glyph.
      var um = userMeta(user.id);
      var mark = matched ? '<span class="an-mark an-mark-ok">✓</span>'
                         : '<span class="an-mark an-mark-bad">✕</span>';
      var slotLabel = slotName.replace(/_/g, " ").toLowerCase();
      var yours = iconEl(user.id, um && um.icon,
        resolveQuality(um && um.quality, user.bonus), um && um.slug,
        user, specId, "an-user", mark, "your " + slotLabel + " item");

      // Meta target picks. A slot of an interchangeable pair takes the share
      // planGroupTargets dealt it (empty when the slot is already on a meta pick
      // — a matched slot shows no target anyway); every other slot takes its own
      // SIM/TOP picks, or the most-popular `common` fallback.
      var picks = GROUPS[slotName] ? (groupPlan[slotName] || [])
                                   : displayTargets(metaSlots, slotName);
      var targets = picks.map(function (t) {
        // Badge OUTSIDE the Wowhead <a>: hovering it must not also open the item
        // tooltip, which would then be drawn across the badge's own tooltip.
        // Same structure the spec page uses (spec_page.html:51). The wrapper is
        // the badge's positioning context and is the same box as the icon, so
        // the badge lands exactly where it did inside the link.
        return '<span class="an-icon-wrap">' +
          iconEl(t.pick.id, t.pick.icon, t.pick.quality, t.pick.slug, t.pick, specId, "", "",
            "meta pick for " + slotLabel) +
          chip(t.kind, t.pick, meta) + "</span>";
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
        // Exactly the picks this slot is being told to equip — so a paired slot
        // flags the bag it can actually use, not one reserved for its sibling.
        var wantIds = picks.map(function (t) { return t.pick.id; });
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
          // catalog so an off-combo enchant still shows its real icon/name. enchantRef
          // links via the scroll itemId or the rune/enchant spellId — NEVER
          // item=<enchantId>, which resolves to an unrelated item.
          var einfo = enchInfo[user.enchant] || gxIndex.enchants[user.enchant] || { id: user.enchant };
          var whRef = enchantRef(user.enchant, einfo);
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

    // Gear match now folds in gems and enchants, not just slots: each required
    // gem/enchant instance in the meta combo the player satisfies counts toward
    // the score alongside each scored slot they hit.
    var gemMet = 0, gemNeed = 0;
    Object.keys(gemBudget).forEach(function (id) {
      gemNeed += gemBudget[id];
      gemMet += Math.min(counts.gems[id] || 0, gemBudget[id]);
    });
    var enchMet = 0, enchNeed = 0;
    Object.keys(enchBudget).forEach(function (id) {
      enchNeed += enchBudget[id];
      enchMet += Math.min(counts.enchants[id] || 0, enchBudget[id]);
    });
    var gearTotal = comparable + gemNeed + enchNeed;
    var gearScore = gearTotal ? Math.round((good + gemMet + enchMet) / gearTotal * 100) : null;

    var iconHtml = disp.icon ? '<img src="/data/icons/' + esc(disp.icon) + '.jpg" class="an-mh-icon" alt="">' : "";

    // Talents: decode + diff up front so its score feeds the shared header.
    var talentTree = buildTalentTree(meta, treeFile);
    var diff = talentTree ? talentDiff(meta, parsed, talentTree, null) : null;
    var talentScore = diff && diff.haveScore ? diff.score : null;
    talentCtx = { meta: meta, parsed: parsed, tree: talentTree, gearScore: gearScore };

    // One shared "meta match" header for the whole report: spec identity plus a
    // small Gear ring, a big overall ring (gear + talents 50/50) and a small
    // Talents ring.
    var header =
      '<div class="an-match-header">' +
        '<div class="an-mh-id">' + iconHtml +
          '<div class="an-mh-spec">' + esc(disp.name) + " " + esc(disp.class) + "</div></div>" +
        matchRings(gearScore, talentScore) +
      "</div>";

    // Width of the modifier zone, in icon slots: the busiest slot's gem count
    // plus one for the enchant column. Zero means no slot in this report has
    // either, and `an-has-mods` drops the zone (and its divider) entirely.
    var modSlots = maxGems + (anyEnch ? 1 : 0);

    var talentsHtml = '<div id="an-talents">' +
      (talentTree ? talentCardHtml(meta, parsed, talentTree, diff) : "") + "</div>";

    // Two columns below the header: gear (grid + gem/enchant combos) on the left,
    // talents on the right. They stack on smaller screens and split from xl.
    var gearHtml =
      '<div class="an-card an-gear-card"><div class="an-card-head">Gear</div>' +
        '<div class="an-grid' + (modSlots ? " an-has-mods" : "") +
          '" style="--an-mod-slots:' + modSlots + '">' + layout(tilesBySlot) + "</div>" +
        comboSection(meta.gem_combo, counts.gems, "gem", meta) +
        comboSection(meta.enchant_combo, counts.enchants, "enchant", meta) +
      "</div>";

    results.innerHTML =
      header +
      '<div class="row g-4 an-results-row">' +
        '<div class="col-12 col-xl-5 an-col-gear">' + gearHtml + "</div>" +
        '<div class="col-12 col-xl-7 an-col-talents">' + talentsHtml + "</div>" +
      "</div>";

    if (window.$WowheadPower && typeof window.$WowheadPower.refreshLinks === "function") {
      try { window.$WowheadPower.refreshLinks(); } catch (e) { /* tooltips are best-effort */ }
    }
    // The global tooltip init (material-dashboard.js) only scans the DOM at page
    // load; our badges are rendered afterward, so wire them up here.
    if (window.bootstrap && window.bootstrap.Tooltip) {
      results.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(function (el) {
        // A badge that sits ON an item icon opens its tooltip to the LEFT:
        // Wowhead draws the item tooltip to the icon's right, and the default
        // "top" placement put the two on top of each other. fallbackPlacements
        // is pinned so Popper can't flip it back into the collision in a narrow
        // pane. The combo-header badge isn't on an icon and keeps the default.
        var onIcon = el.classList.contains("item-icon-top-badge") ||
                     el.classList.contains("item-icon-sim-badge");
        try {
          window.bootstrap.Tooltip.getOrCreateInstance(el,
            onIcon ? { placement: "left", fallbackPlacements: ["left", "top"] } : {});
        } catch (e) { /* best-effort */ }
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
      loadTalentTree(specId),
    ])
      .then(function (out) {
        var meta = out[0];
        var treeFile = out[4];
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
        return loadIconShards(wanted).then(function () { render(meta, parsed, treeFile); });
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
