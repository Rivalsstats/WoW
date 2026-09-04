/*
 * tierlist-modal.js — the Sim DPS Tierlist "what was this simmed with?" modal.
 *
 * Each DPS bar on the tierlist (Popular / SimC BIS) is a button carrying its
 * spec id + gear set. A click opens one shared Bootstrap modal that shows the
 * exact gear and talents SimulationCraft used for that (spec, gear set): the
 * gear in the spec page's double-column armory layout, the talents as the same
 * positioned tree the spec page and analyzer draw.
 *
 * Data source: /assets/json/tierlist_gear.json, emitted by
 * generateSimcProfiles.py (the DB-having sim-profiles job) as
 *   { "<specId>": { "popular": {talents, slots}, "simcbis": {talents, slots} } }
 * where each slot is { id, name, icon, quality, bonus[], enchant?, gems?[] } and
 * `talents` is the Blizzard loadout export string. Item icon/name/rarity are
 * pre-resolved server-side; enchant/gem icons come from the shared
 * gem_enchant_index.json catalog; the talent tree is decoded in the browser
 * against /assets/json/talent_trees/<spec>.json — the very files and CSS the
 * analyzer already ships, so the modal matches the spec page without a DB.
 *
 * The Blizzard "serialization version 2" loadout decode below mirrors
 * analyzer.js decodeLoadout (same bitstream); kept self-contained here so the
 * tierlist page needs none of the analyzer's meta-comparison machinery.
 */
(function () {
  "use strict";

  var QUESTION = "/data/icons/inv_misc_questionmark.png";

  // Armory display grouping — mirrors the spec page's Gear Overview (two panes:
  // armour left/right, then weapons beside trinkets).
  var LEFT_ORDER = ["HEAD", "NECK", "SHOULDER", "BACK", "CHEST", "WRIST"];
  var RIGHT_ORDER = ["HANDS", "WAIST", "LEGS", "FEET", "FINGER_1", "FINGER_2"];
  var WEAPON_SLOTS = ["MAIN_HAND", "OFF_HAND"];
  var TRINKET_SLOTS = ["TRINKET_1", "TRINKET_2"];
  var DISPLAY_BLOCKS = [
    { left: { slots: LEFT_ORDER }, right: { slots: RIGHT_ORDER } },
    { left: { slots: WEAPON_SLOTS, head: "Weapon" },
      right: { slots: TRINKET_SLOTS, head: "Trinkets" } },
  ];

  var modalEl = document.getElementById("gearModal");
  if (!modalEl) return;
  var titleEl = document.getElementById("gearModalTitle");
  var iconEl = document.getElementById("gearModalIcon");
  var bodyEl = document.getElementById("gearModalBody");

  // ---- shipped-catalog loaders (each fetched once, tolerant) ---------------
  var gearData = null, gearPromise = null;
  function loadGearData() {
    if (gearPromise) return gearPromise;
    gearPromise = fetch("/assets/json/tierlist_gear.json")
      .then(function (r) {
        // A 404 means the file isn't present (a template-only --debug preview,
        // which never emits it) — degrade to the per-spec "no gear recorded"
        // notice rather than a hard error. Any other failure is a real problem.
        if (r.status === 404) return {};
        if (!r.ok) { var e = new Error("gear data HTTP " + r.status); e.dataError = true; throw e; }
        return r.json();
      })
      .then(function (o) { gearData = o || {}; return gearData; });
    return gearPromise;
  }

  // id -> {icon, quality, slug} for the ~500 items with an /items page, so gear
  // links point at the internal item page when one exists (else Wowhead).
  var itemsIndex = null, itemsPromise = null;
  function loadItemsIndex() {
    if (itemsPromise) return itemsPromise;
    itemsPromise = fetch("/assets/json/items_index.json")
      .then(function (r) { return r.ok ? r.json() : []; })
      .then(function (arr) {
        var map = {};
        (arr || []).forEach(function (it) { if (it && it.id != null) map[it.id] = it; });
        itemsIndex = map;
        return map;
      })
      .catch(function () { itemsIndex = {}; return itemsIndex; });
    return itemsPromise;
  }

  // { gems: {<gemItemId>: {name, icon, quality}},
  //   enchants: {<enchantId>: {name, icon, quality, itemId?, spellId?}} }.
  var gxIndex = { gems: {}, enchants: {} }, gxPromise = null;
  function loadGemEnchantIndex() {
    if (gxPromise) return gxPromise;
    gxPromise = fetch("/assets/json/gem_enchant_index.json")
      .then(function (r) { return r.ok ? r.json() : {}; })
      .then(function (o) { gxIndex = { gems: (o && o.gems) || {}, enchants: (o && o.enchants) || {} }; return gxIndex; })
      .catch(function () { gxIndex = { gems: {}, enchants: {} }; return gxIndex; });
    return gxPromise;
  }

  // Per-spec talent tree geometry ({fullNodeOrder, nodes, subTrees}). Tolerant:
  // a miss just drops the talent section.
  var treeCache = {};
  function loadTree(specId) {
    if (treeCache[specId]) return treeCache[specId];
    treeCache[specId] = fetch("/assets/json/talent_trees/" + specId + ".json")
      .then(function (r) { return r.ok ? r.json() : null; })
      .catch(function () { return null; });
    return treeCache[specId];
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  // ---- gear rendering ------------------------------------------------------

  // The &bonus=..&spec=..&ench=..&gems=.. tail shared by the tooltip and link.
  function wowheadParams(entry, specId) {
    var p = "";
    if (entry.bonus && entry.bonus.length) p += "&bonus=" + entry.bonus.join(":");
    if (specId != null) p += "&spec=" + specId;
    if (entry.enchant) p += "&ench=" + entry.enchant;
    if (entry.gems && entry.gems.length) p += "&gems=" + entry.gems.join(":");
    return p;
  }

  // The enchanting scroll item, else the rune/enchant spell (DK weapon runes
  // have no scroll item), else the raw enchant id as a last resort — never
  // item=<enchant_id>, which resolves to an unrelated item. Mirrors analyzer.js.
  function enchantRef(eid, einfo) {
    einfo = einfo || {};
    if (einfo.itemId != null) return "item=" + einfo.itemId;
    if (einfo.spellId != null) return "spell=" + einfo.spellId;
    return "item=" + eid;
  }

  // A small Wowhead-tooltipped aux icon (enchant / gem).
  function auxIconEl(info, whRef, label) {
    var q = info && info.quality != null ? " border-quality-" + info.quality : "";
    var src = info && info.icon ? "/data/icons/" + esc(info.icon) + ".png" : QUESTION;
    return '<a class="gm-aux-icon' + q + '" target="_blank" rel="noopener"' +
      (label ? ' aria-label="' + esc(label) + '"' : "") +
      ' href="https://www.wowhead.com/' + esc(whRef) + '" data-wowhead="' + esc(whRef) + '">' +
      '<img src="' + src + '" alt="" loading="lazy" onerror="this.src=\'' + QUESTION + '\'"></a>';
  }

  // One gear slot row: item icon + name (rarity coloured), then enchant + gems.
  function slotTile(slotName, entry, specId) {
    if (!entry) return "";
    var q = entry.quality != null ? entry.quality : null;
    var rim = q != null ? " border-quality-" + q : "";
    var nameCls = q != null ? " item-quality-" + q : "";
    var src = entry.icon ? "/data/icons/" + esc(entry.icon) + ".png" : QUESTION;
    var params = wowheadParams(entry, specId);

    var idx = itemsIndex || {};
    var known = idx[entry.id];
    var href, target;
    if (known && known.slug) {
      href = "/items/" + esc(known.slug) + (specId != null ? "?spec=" + specId : "");
      target = "";
    } else {
      href = "https://www.wowhead.com/item=" + entry.id + (params ? "?" + params.replace(/^&/, "") : "");
      target = ' target="_blank" rel="noopener"';
    }

    var aux = "";
    if (entry.enchant) {
      var einfo = gxIndex.enchants[entry.enchant] || { id: entry.enchant };
      aux += auxIconEl(einfo, enchantRef(entry.enchant, einfo), "Enchant");
    }
    (entry.gems || []).forEach(function (gid) {
      var ginfo = gxIndex.gems[gid] || { id: gid };
      aux += auxIconEl(ginfo, "item=" + gid, "Gem");
    });

    var label = esc(slotName.replace(/_/g, " "));
    return '<div class="gm-slot">' +
      '<a class="gm-item-icon' + rim + '"' + target + ' href="' + href + '"' +
        ' data-wowhead="item=' + entry.id + params + '" aria-label="' + esc(entry.name || slotName) + '">' +
        '<img src="' + src + '" alt="" loading="lazy" onerror="this.src=\'' + QUESTION + '\'"></a>' +
      '<div class="gm-item-main">' +
        '<div class="gm-slot-label">' + label + '</div>' +
        '<a class="gm-item-name' + nameCls + '"' + target + ' href="' + href + '"' +
          ' data-wowhead="item=' + entry.id + params + '">' + esc(entry.name || ("Item " + entry.id)) + '</a>' +
      '</div>' +
      (aux ? '<div class="gm-aux">' + aux + '</div>' : "") +
      '</div>';
  }

  function gearHtml(gset, specId) {
    var slots = (gset && gset.slots) || {};
    var blocks = DISPLAY_BLOCKS.map(function (block) {
      var panes = [block.left, block.right].map(function (pane) {
        var tiles = pane.slots.map(function (s) { return slotTile(s, slots[s], specId); }).join("");
        return { head: pane.head, tiles: tiles };
      });
      if (!panes[0].tiles && !panes[1].tiles) return "";
      return '<div class="gm-block">' + panes.map(function (p) {
        return '<div class="gm-col">' +
          (p.head ? '<div class="gm-col-head">' + esc(p.head) + '</div>' : "") +
          p.tiles + '</div>';
      }).join("") + '</div>';
    }).join("");
    if (!blocks) return '<p class="text-sm text-secondary mb-0">No gear recorded for this set.</p>';
    return '<div class="gm-gear">' + blocks + '</div>';
  }

  // ---- talent decode + tree render (no meta comparison) --------------------

  var TALENT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  var TALENT_CHAR_IDX = (function () {
    var m = {};
    for (var i = 0; i < TALENT_CHARS.length; i++) m[TALENT_CHARS.charAt(i)] = i;
    return m;
  })();
  var LOADOUT_VERSION = 2;

  function decodeLoadout(code, tree) {
    if (!code || !tree || !tree.fullNodeOrder) return null;
    var bits = [];
    for (var i = 0; i < code.length; i++) {
      var v = TALENT_CHAR_IDX[code.charAt(i)];
      if (v == null) return null;
      for (var b = 0; b < 6; b++) bits.push((v >> b) & 1);
    }
    var p = 0;
    function read(n) {
      var r = 0;
      for (var i = 0; i < n; i++) {
        if (p >= bits.length) return null;
        r |= bits[p++] << i;
      }
      return r;
    }
    var version = read(8);
    if (version !== LOADOUT_VERSION) return { unsupported: true };
    read(16); // spec id
    for (var h = 0; h < 16; h++) read(8); // tree hash, ignored
    var selected = {};
    var order = tree.fullNodeOrder;
    for (var n = 0; n < order.length; n++) {
      var isSel = read(1);
      if (isSel == null) break;
      if (!isSel) continue;
      var isPurchased = read(1);
      var entryIndex = 0;
      if (isPurchased) {
        if (read(1)) read(6);          // partial rank (ignored for display)
        if (read(1)) entryIndex = read(2);
      }
      selected[order[n]] = { entryIndex: entryIndex || 0, purchased: !!isPurchased };
    }
    return { selected: selected };
  }

  function ttNode(tree, nid) { return tree.nodes[nid] || tree.nodes[String(nid)] || null; }
  function ttEntry(node, idx) {
    if (!node || !node.entries || !node.entries.length) return null;
    return node.entries[idx] || node.entries[0];
  }
  function isChoice(node) { return !!(node && node.entries && node.entries.length > 1); }

  // Which hero subtree the build sits in: the subTreeId most of its picks share.
  function heroTreeOf(selected, tree) {
    var counts = {};
    Object.keys(selected).forEach(function (nid) {
      if (!selected[nid].purchased) return;
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

  function ntypeOf(node) {
    if (node.type === "tiered") return "passive";
    if (isChoice(node)) return "choice";
    var e = node.entries && node.entries[0];
    return (e && e.type) || "passive";
  }

  // The icon (+ arrows for a choice node) for one tree node, greyed when the
  // build does not take it. Mirrors the spec page's render_tree_node visuals.
  function nodeInner(nid, node, selected) {
    var sel = selected[nid] || selected[String(nid)];
    var active = !!(node.free || (sel && sel.purchased));
    var idx = sel ? (sel.entryIndex || 0) : 0;
    var entry = ttEntry(node, idx);
    var icon = entry && entry.icon ? "/data/icons/" + esc(entry.icon) + ".png" : QUESTION;
    var sp = entry && entry.spellId;
    var href = sp ? "https://www.wowhead.com/spell=" + sp : "#";
    var wh = sp ? ' data-wowhead="spell=' + sp + '"' : "";
    var alt = esc(entry ? entry.name : "");
    var gray = active ? "" : "filter:grayscale(100%);opacity:0.45;";
    if (ntypeOf(node) === "choice") {
      return '<div class="tt-choice-wrapper" style="--border-color:#ffb000;width:100%;height:100%;' + gray + '">' +
        '<div class="arrow-left"></div>' +
        '<div class="tt-octagon-border" style="width:100%;height:100%;">' +
        '<a href="' + href + '" target="_blank" rel="noopener"' + wh + '>' +
        '<img class="tt-octagon" src="' + icon + '" alt="' + alt +
        '" loading="lazy" onerror="this.src=\'' + QUESTION + '\'"></a>' +
        '</div><div class="arrow-right"></div></div>';
    }
    return '<a href="' + href + '" target="_blank" rel="noopener"' + wh +
      ' style="' + gray + '">' +
      '<img src="' + icon + '" alt="' + alt +
      '" loading="lazy" onerror="this.src=\'' + QUESTION + '\'"></a>';
  }

  // A positioned class/spec tree column with connector edges (spec-page style).
  function ttColumn(ids, tree, selected, extraCls) {
    var ns = ids.map(function (id) { return { id: String(id), node: ttNode(tree, id) }; })
      .filter(function (o) { return o.node; });
    if (!ns.length) return "";
    var xs = ns.map(function (o) { return o.node.x; }), ys = ns.map(function (o) { return o.node.y; });
    var minx = Math.min.apply(null, xs) - 150, maxx = Math.max.apply(null, xs) + 150;
    var miny = Math.min.apply(null, ys) - 150, maxy = Math.max.apply(null, ys) + 150;
    var w = Math.max(1, maxx - minx), h = Math.max(1, maxy - miny);
    var L = function (x) { return (x - minx) / w * 100; }, Tp = function (y) { return (y - miny) / h * 100; };
    function isActive(nid) { var n = ttNode(tree, nid); var s = selected[nid] || selected[String(nid)]; return !!(n && (n.free || (s && s.purchased))); }
    var inGroup = {}; ns.forEach(function (o) { inGroup[o.id] = true; });
    var lines = ns.map(function (o) {
      return (o.node.next || []).map(function (cid) {
        var c = ttNode(tree, cid);
        if (!c || !inGroup[String(cid)]) return "";
        var active = isActive(o.id) && isActive(String(cid));
        return '<line x1="' + L(o.node.x) + '%" y1="' + Tp(o.node.y) + '%" x2="' + L(c.x) +
          '%" y2="' + Tp(c.y) + '%" stroke="' + (active ? "#ffb000" : "#555") + '" stroke-width="2"></line>';
      }).join("");
    }).join("");
    var nodesHtml = ns.map(function (o) {
      return '<div class="tt-node" data-ntype="' + ntypeOf(o.node) +
        '" style="left:' + L(o.node.x) + "%;top:" + Tp(o.node.y) + '%">' +
        nodeInner(o.id, o.node, selected) + "</div>";
    }).join("");
    return '<div class="tt-column tt-tree-column ' + (extraCls || "") + '">' +
      '<svg class="tt-edges">' + lines + "</svg>" + nodesHtml + "</div>";
  }

  // Hero column: the big glowing hero-tree icon + its choice nodes stacked.
  function heroColumn(ids, tree, selected, hero) {
    var st = (tree.subTrees || {})[hero] || (tree.subTrees || {})[String(hero)] || {};
    var icon = st.icon ? "/data/icons/" + esc(st.icon) + ".png" : QUESTION;
    var choice = ids.map(function (id) { return { id: String(id), node: ttNode(tree, id) }; })
      .filter(function (o) { return o.node && isChoice(o.node); })
      .sort(function (a, b) { return (a.node.y - b.node.y) || (a.node.x - b.node.x); });
    var nodesHtml = choice.map(function (o) {
      return '<div class="gm-hnode" data-ntype="choice">' + nodeInner(o.id, o.node, selected) + "</div>";
    }).join("");
    var label = esc(st.name || "Hero tree");
    return '<div class="tt-column tt-hero-column gm-hero-column">' +
      '<div class="gm-hero-head">' +
        '<img class="gm-hero-icon" src="' + icon + '" alt="' + label + '" onerror="this.src=\'' + QUESTION + '\'">' +
        '<div class="gm-hero-name">' + label + '</div></div>' +
      '<div class="gm-hero-nodes">' + nodesHtml + '</div></div>';
  }

  function talentsHtml(code, tree) {
    if (!tree || !tree.nodes) return '<p class="text-sm text-secondary mb-0">Talent tree unavailable for this spec.</p>';
    if (!code) return '<p class="text-sm text-secondary mb-0">No talent loadout recorded.</p>';
    var decoded = decodeLoadout(code, tree);
    if (!decoded || decoded.unsupported) return '<p class="text-sm text-secondary mb-0">Talent loadout could not be read.</p>';
    var selected = decoded.selected || {};
    var hero = heroTreeOf(selected, tree);
    var classIds = [], specIds = [], heroIds = [];
    Object.keys(tree.nodes).forEach(function (nid) {
      var n = tree.nodes[nid];
      if (n.g === "class") classIds.push(nid);
      else if (n.g === "spec") specIds.push(nid);
      else if (n.g === "hero" && String(n.subTreeId) === String(hero)) heroIds.push(nid);
    });
    // Positioned only when nodes carry real coordinates.
    var hasCoords = classIds.concat(specIds).some(function (nid) {
      var n = ttNode(tree, nid);
      return n && isFinite(n.x) && isFinite(n.y) && (n.x !== 0 || n.y !== 0);
    });
    if (!hasCoords) return '<p class="text-sm text-secondary mb-0">Talent tree layout unavailable for this spec.</p>';
    return '<div id="static-talent-tree" class="talent-tree-wrapper gm-tt-wrapper d-flex flex-column flex-xl-row ' +
      'justify-content-center align-items-center gap-4">' +
      ttColumn(classIds, tree, selected, "") +
      (heroIds.length ? heroColumn(heroIds, tree, selected, hero) : "") +
      ttColumn(specIds, tree, selected, "") +
      "</div>";
  }

  // ---- open + populate -----------------------------------------------------

  function refreshDynamic(root) {
    if (window.$WowheadPower && typeof window.$WowheadPower.refreshLinks === "function") {
      try { window.$WowheadPower.refreshLinks(); } catch (e) { /* best-effort */ }
    }
    if (window.bootstrap && window.bootstrap.Tooltip) {
      (root || document).querySelectorAll('[data-bs-toggle="tooltip"]').forEach(function (el) {
        try { window.bootstrap.Tooltip.getOrCreateInstance(el); } catch (e) { /* best-effort */ }
      });
    }
  }

  function populate(btn) {
    var specId = btn.getAttribute("data-spec-id");
    var gearset = btn.getAttribute("data-gearset");
    var label = btn.getAttribute("data-label") || "";
    var specName = btn.getAttribute("data-spec-name") || "";
    var className = btn.getAttribute("data-class-name") || "";
    var cleanClass = btn.getAttribute("data-clean-class") || "";
    var icon = btn.getAttribute("data-icon");

    if (iconEl) {
      if (icon) { iconEl.src = "/data/icons/" + icon + ".jpg"; iconEl.hidden = false; }
      else { iconEl.hidden = true; }
    }
    if (titleEl) {
      titleEl.innerHTML = '<span class="class-' + esc(cleanClass) + '-text">' +
        esc(specName) + " " + esc(className) + '</span> ' +
        '<span class="text-secondary">&middot; ' + esc(label) + '</span>';
    }
    bodyEl.innerHTML = '<p class="text-sm text-secondary mb-0">Loading gear and talents…</p>';

    Promise.all([loadGearData(), loadItemsIndex(), loadGemEnchantIndex(), loadTree(specId)])
      .then(function (out) {
        var tree = out[3];
        var forSpec = (gearData && gearData[specId]) || {};
        var set = forSpec[gearset];
        if (!set) {
          bodyEl.innerHTML = '<div class="alert alert-warning text-dark text-sm mb-0">' +
            '<i class="material-symbols-rounded align-middle me-1">warning</i>' +
            'No ' + esc(label) + ' gear was recorded for this spec.</div>';
          return;
        }
        bodyEl.innerHTML =
          '<div class="row g-4">' +
            '<div class="col-12 col-xl-5">' +
              '<h6 class="mb-2">Gear</h6>' + gearHtml(set, specId) +
            '</div>' +
            '<div class="col-12 col-xl-7">' +
              '<h6 class="mb-2">Talents</h6>' + talentsHtml(set.talents, tree) +
            '</div>' +
          '</div>';
        refreshDynamic(bodyEl);
      })
      .catch(function (err) {
        bodyEl.innerHTML = '<div class="alert alert-warning text-dark text-sm mb-0">' +
          '<i class="material-symbols-rounded align-middle me-1">warning</i>' +
          (err && err.dataError
            ? "Couldn't load the gear data. Reload the page and try again."
            : "Gear and talent details are unavailable for this spec right now.") + '</div>';
      });
  }

  var modal = window.bootstrap && window.bootstrap.Modal
    ? window.bootstrap.Modal.getOrCreateInstance(modalEl) : null;

  // ---- deep link (#gearModal&gear=<specId>-<gearset>) -----------------------
  //
  // The modal is built in JS from a spec id + gear set, so its contents can't be
  // reached by an element id alone: they ride in the hash as gear=<specId>-<gearset>,
  // mirroring the comps page Details modal (comp=<specIds>). deep-link.js keeps the
  // #gearModal target + this state in the address bar (copy-link button in the modal
  // header) and, on a fresh load, opens the right spec+gearset modal — independent of
  // which target-count tab is active, because the same button exists in every tab.
  var openGear = null;

  function openFor(btn) {
    var specId = btn.getAttribute("data-spec-id");
    var gearset = btn.getAttribute("data-gearset");
    openGear = { specId: specId, gearset: gearset };
    populate(btn);
    if (modal) modal.show();
    if (window.MythiLink) window.MythiLink.sync();
  }

  document.addEventListener("click", function (ev) {
    var btn = ev.target.closest && ev.target.closest("[data-gear-open]");
    if (!btn) return;
    ev.preventDefault();
    openFor(btn);
  });

  modalEl.addEventListener("hidden.bs.modal", function () {
    openGear = null;
    if (window.MythiLink) window.MythiLink.sync();
  });

  if (window.MythiLink) {
    window.MythiLink.registerState("gear", {
      read: function () {
        return openGear ? openGear.specId + "-" + openGear.gearset : null;
      },
      apply: function (value) {
        var i = String(value).indexOf("-");
        if (i < 1) return;
        var specId = String(value).slice(0, i);
        var gearset = String(value).slice(i + 1);
        if (!specId || !gearset) return;
        var btn = document.querySelector(
          '[data-gear-open][data-spec-id="' + specId + '"][data-gearset="' + gearset + '"]'
        );
        if (btn) openFor(btn);
      }
    });
  }
})();
