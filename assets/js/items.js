/* Items browse page (items.html), served at /pages/items.
 *
 * Browse grid of all items, filterable by slot/quality and searchable by name.
 * Each card links to the item's dedicated static page at /items/<slug>.
 *
 * Data: /assets/json/items_index.json (compact manifest, includes a `slug`).
 * Spec names/icons are injected by the template as window.specs_map.
 */
(function () {
  "use strict";

  // Wowhead's power.js is injected by Klaro (consent-gated, see items.html). Its
  // defaults recolor/rename/iconize every link it binds to, which would fight the
  // card's own quality colouring, so opt out before it can load. This file is a
  // plain body script and Klaro is deferred in the head, so we always win the race.
  window.whTooltips = { colorLinks: false, iconizeLinks: false, renameLinks: false };

  var SPECS = window.specs_map || {};
  var DUNGEONS = window.dungeons_map || {};
  var RAIDS = window.raids_map || {};

  // Character-sheet slot order, mirroring the spec page's LEFT/RIGHT panel
  // layout (HEAD, NECK, SHOULDER, BACK, CHEST, WRIST, HANDS, WAIST, LEGS, FEET,
  // FINGER, TRINKET, then weapons). Slots not listed sort alphabetically after.
  var SLOT_ORDER = [
    "Head", "Neck", "Shoulder", "Back", "Chest", "Wrist",
    "Hands", "Waist", "Legs", "Feet", "Finger", "Trinket",
    "Main Hand", "One-Hand", "Two-Hand", "Off Hand", "Held In Off-hand", "Ranged",
    "Other",
  ];
  var QUALITY_NAMES = {
    0: "Poor", 1: "Common", 2: "Uncommon", 3: "Rare",
    4: "Epic", 5: "Legendary", 6: "Artifact", 7: "Heirloom",
  };

  // Filter state is mirrored into the query string (?slot=one-hand&quality=epic&
  // sort=name&q=blade) so a filtered view survives a refresh and can be linked to
  // someone else. Slots are slugged from their display name, not from the
  // manifest's slotKey — that key collapses One-Hand/Two-Hand/Ranged into WEAPON.
  function slotSlug(name) { return String(name).toLowerCase().replace(/\s+/g, "-"); }
  var SLOT_BY_SLUG = {}; // "one-hand" -> "One-Hand", filled by buildSlotOptions
  var QUALITY_BY_SLUG = {}; // "epic" -> "4"
  // Drop-source filter (multi-select). Each item's manifest `sources` is a flat
  // list of tokens: "d:<dungeonId>", "r:<raidId>", "b:<raidId>:<encId>",
  // "crafted", "tier", "pvp", "other". The <option> values are those tokens; the URL carries
  // readable slugs instead. Both maps are filled by buildSourceOptions.
  var TOKEN_BY_SLUG = {}; // "pit-of-saron" -> "d:556", "nerubar-palace--ulgrax" -> "b:1207:2902"
  var SLUG_BY_TOKEN = {}; // "d:556" -> "pit-of-saron"
  Object.keys(QUALITY_NAMES).forEach(function (q) {
    QUALITY_BY_SLUG[QUALITY_NAMES[q].toLowerCase()] = q;
  });

  function el(id) { return document.getElementById(id); }
  function iconUrl(icon) { return "/data/icons/" + icon + ".png"; }
  function fmt(n) { return (n || 0).toLocaleString(); }
  function debounce(fn, ms) {
    var t;
    return function () { var a = arguments; clearTimeout(t); t = setTimeout(function () { fn.apply(null, a); }, ms); };
  }

  var PAGE_SIZE = 60;
  var all = [];
  var filtered = [];
  var shown = 0;

  // bootstrap-select (selectpicker) is loaded globally; refresh after we mutate
  // a <select>'s options so the styled dropdown picks them up.
  function refreshPicker(id) {
    if (window.jQuery && window.jQuery.fn.selectpicker) {
      window.jQuery("#" + id).selectpicker("refresh");
    }
  }

  // The source filter is a multi-select, so its value is an array of tokens.
  function getSourceTokens() {
    if (window.jQuery && window.jQuery.fn.selectpicker) {
      var v = window.jQuery("#source-filter").selectpicker("val");
      return v == null ? [] : (Array.isArray(v) ? v : [v]);
    }
    var out = [], opts = el("source-filter").options;
    for (var i = 0; i < opts.length; i++) if (opts[i].selected) out.push(opts[i].value);
    return out;
  }

  function buildSlotOptions() {
    var slots = {};
    all.forEach(function (i) { if (i.slot) slots[i.slot] = true; });
    var sel = el("slot-filter");
    var allOpt = document.createElement("option");
    allOpt.value = ""; allOpt.textContent = "All slots";
    sel.appendChild(allOpt);
    Object.keys(slots).sort(function (a, b) {
      var ia = SLOT_ORDER.indexOf(a); if (ia === -1) ia = Infinity;
      var ib = SLOT_ORDER.indexOf(b); if (ib === -1) ib = Infinity;
      return ia !== ib ? ia - ib : a.localeCompare(b);
    }).forEach(function (s) {
      var o = document.createElement("option");
      o.value = s; o.textContent = s;
      sel.appendChild(o);
      SLOT_BY_SLUG[slotSlug(s)] = s;
    });
    refreshPicker("slot-filter");
  }

  function buildQualityOptions() {
    var qualities = {};
    all.forEach(function (i) { if (i.quality != null) qualities[i.quality] = true; });
    var sel = el("quality-filter");
    var allOpt = document.createElement("option");
    allOpt.value = ""; allOpt.textContent = "All quality";
    sel.appendChild(allOpt);
    // Highest quality first (Legendary → Epic → Rare → …).
    Object.keys(qualities).map(Number).sort(function (a, b) { return b - a; }).forEach(function (q) {
      var o = document.createElement("option");
      o.value = String(q); o.textContent = QUALITY_NAMES[q] || ("Quality " + q);
      sel.appendChild(o);
    });
    refreshPicker("quality-filter");
  }

  // Bootstrap-select data-content markup with a leading icon, mirroring the route
  // search page's grouped pickers. Returns null when there is no icon to show.
  function iconContent(iconFile, label) {
    if (!iconFile) return null;
    return "<span class='dropdown-icon-item'><img src='/data/icons/" + iconFile +
      "' class='dropdown-icon' alt='' style='width:20px;height:20px;border-radius:4px;" +
      "object-fit:cover;flex:0 0 20px;margin-right:8px;'>" +
      "<span class='dropdown-icon-label'>" + label + "</span></span>";
  }

  function addSourceOption(parent, token, label, slug, content) {
    var o = document.createElement("option");
    o.value = token; o.textContent = label;
    o.setAttribute("data-tokens", label); // enables live-search by name
    if (content) o.setAttribute("data-content", content);
    parent.appendChild(o);
    TOKEN_BY_SLUG[slug] = token;
    SLUG_BY_TOKEN[token] = slug;
  }

  function buildSourceOptions() {
    // Discover which sources actually drop something in the current data, from the
    // per-item tokens, and lay them out grouped: acquisition categories (Crafted,
    // Tier Set, PvP), then a "Dungeons" optgroup, then one optgroup per raid (whole
    // raid + present bosses), then Other, with dividers between the blocks. Only
    // tokens a rendered item carries are offered, so empty instances never appear.
    var dungeonsPresent = {};        // dungeonId -> true
    var raidsPresent = {};           // raidId -> { encId -> true }
    var hasCrafted = false, hasTier = false, hasPvp = false, hasOther = false;
    all.forEach(function (i) {
      (i.sources || []).forEach(function (tok) {
        if (tok === "crafted") { hasCrafted = true; return; }
        if (tok === "tier") { hasTier = true; return; }
        if (tok === "pvp") { hasPvp = true; return; }
        if (tok === "other") { hasOther = true; return; }
        var p = String(tok).split(":");
        if (p[0] === "d") { dungeonsPresent[p[1]] = true; }
        else if (p[0] === "r") { raidsPresent[p[1]] = raidsPresent[p[1]] || {}; }
        else if (p[0] === "b") {
          (raidsPresent[p[1]] = raidsPresent[p[1]] || {})[p[2]] = true;
        }
      });
    });

    var sel = el("source-filter");
    sel.innerHTML = ""; // multi-select: empty selection means "all", no reset option
    function addDivider() {
      var o = document.createElement("option");
      o.setAttribute("data-divider", "true");
      sel.appendChild(o);
    }

    // Present instances, resolved + sorted. Only tokens that at least one rendered
    // item carries reach here, so empty dungeons/raids never appear in the list.
    var dungeonList = Object.keys(dungeonsPresent).map(function (id) {
      var d = DUNGEONS[id] || {};
      return { id: id, name: d.name || id, slug: d.slug || id, icon: d.icon };
    }).sort(function (a, b) { return String(a.name).localeCompare(String(b.name)); });
    var raidList = Object.keys(raidsPresent).map(function (id) {
      var r = RAIDS[id] || {};
      return { id: id, name: r.name || ("Raid " + id), slug: r.slug || id,
               icon: r.icon, bosses: r.bosses || {}, present: raidsPresent[id] };
    }).sort(function (a, b) { return String(a.name).localeCompare(String(b.name)); });

    var hasCategory = hasCrafted || hasTier || hasPvp;
    var hasInstance = dungeonList.length > 0 || raidList.length > 0;

    // 1) Acquisition categories first (how you get it outside a PvE instance).
    if (hasCrafted) addSourceOption(sel, "crafted", "Crafted", "crafted", null);
    if (hasTier) addSourceOption(sel, "tier", "Tier Set", "tier", null);
    if (hasPvp) addSourceOption(sel, "pvp", "PvP", "pvp", null);

    // 2) Dungeons, under a "Dungeons" header (mirroring each raid's title).
    if (hasCategory && hasInstance) addDivider();
    if (dungeonList.length) {
      var dog = document.createElement("optgroup");
      dog.label = "Dungeons";
      dungeonList.forEach(function (d) {
        addSourceOption(dog, "d:" + d.id, d.name, String(d.slug), iconContent(d.icon, d.name));
      });
      sel.appendChild(dog);
    }

    // 3) Raids, each an optgroup: whole-raid option + one option per present boss.
    raidList.forEach(function (r) {
      var og = document.createElement("optgroup");
      og.label = r.name;
      addSourceOption(og, "r:" + r.id, "All of " + r.name, String(r.slug),
        iconContent(r.icon, "All of " + r.name));
      Object.keys(r.present).map(function (enc) {
        var b = r.bosses[enc] || {};
        return { enc: enc, name: b.name || ("Boss " + enc), slug: b.slug || ("boss-" + enc) };
      }).sort(function (a, b) {
        return String(a.name).localeCompare(String(b.name));
      }).forEach(function (b) {
        addSourceOption(og, "b:" + r.id + ":" + b.enc, b.name, r.slug + "--" + b.slug, null);
      });
      sel.appendChild(og);
    });

    // 4) Other, last, after a divider.
    if (hasOther) {
      if (hasCategory || hasInstance) addDivider();
      addSourceOption(sel, "other", "Other", "other", null);
    }

    refreshPicker("source-filter");
  }

  // Resolve ?slot/?quality/?sort/?q into the values the controls carry. Values we
  // don't recognise are dropped (the control falls back to its default) — this is
  // user-typed URL input, not a data file, so it must not blow up the page.
  function readParams() {
    var sp = new URLSearchParams(window.location.search);
    var slot = sp.get("slot") || "";
    var quality = (sp.get("quality") || "").toLowerCase();
    var sort = sp.get("sort") || "";
    var source = sp.get("source") || "";
    return {
      q: sp.get("q") || "",
      // Slugging first means a raw display name (?slot=Held%20In%20Off-hand) resolves too.
      slot: SLOT_BY_SLUG[slotSlug(slot)] || "",
      quality: QUALITY_BY_SLUG[quality] || (/^\d+$/.test(quality) ? quality : ""),
      // Comma-separated list of readable source slugs (a lone legacy dungeon slug
      // still resolves). Raw tokens (d:.., r:.., b:.., crafted, tier, pvp, other) pass through.
      source: parseSourceParam(source),
      sort: sort === "name" || sort === "runs" ? sort : "runs",
    };
  }

  // "pit-of-saron,nerubar-palace--ulgrax,crafted" -> ["d:556","b:1207:2902","crafted"].
  function parseSourceParam(raw) {
    if (!raw) return [];
    return raw.split(",").map(function (s) { return s.trim(); }).filter(Boolean)
      .map(function (s) {
        if (TOKEN_BY_SLUG[s]) return TOKEN_BY_SLUG[s]; // readable slug
        if (SLUG_BY_TOKEN[s]) return s;                // already a token
        return null;
      }).filter(Boolean);
  }

  // selectpicker("val") is what keeps the styled button label in sync; setting
  // .value + "refresh" leaves the old selection in the picker's internal data for
  // options authored in the template (the label ends up showing both). It fires
  // changed.bs.select, not a native change, so our filter listeners don't re-run.
  function setSelect(id, value) {
    if (window.jQuery && window.jQuery.fn.selectpicker) {
      window.jQuery("#" + id).selectpicker("val", value);
    } else {
      el(id).value = value;
    }
  }

  function applyParamsToControls(p) {
    el("item-search").value = p.q;
    setSelect("slot-filter", p.slot);
    setSelect("quality-filter", p.quality);
    setSelect("source-filter", p.source);
    setSelect("sort-by", p.sort);
  }

  // Mirror the current controls back into the URL, omitting defaults so an
  // unfiltered page stays a bare /pages/items. replaceState (not pushState) keeps
  // Back leaving the page instead of unwinding one filter at a time, matching the
  // route search page; the search box's writes ride its existing input debounce.
  function updateUrl() {
    var sp = new URLSearchParams();
    var q = el("item-search").value.trim();
    var slot = el("slot-filter").value;
    var quality = el("quality-filter").value;
    var sourceTokens = getSourceTokens();
    var sort = el("sort-by").value;
    if (slot) sp.set("slot", slotSlug(slot));
    if (quality) sp.set("quality", (QUALITY_NAMES[quality] || quality).toLowerCase());
    if (sourceTokens.length) {
      sp.set("source", sourceTokens.map(function (t) { return SLUG_BY_TOKEN[t] || t; }).join(","));
    }
    if (sort && sort !== "runs") sp.set("sort", sort);
    if (q) sp.set("q", q);
    var qs = sp.toString();
    window.history.replaceState(null, "", window.location.pathname + (qs ? "?" + qs : ""));
  }

  function applyFilters() {
    var q = el("item-search").value.trim().toLowerCase();
    var slot = el("slot-filter").value;
    var quality = el("quality-filter").value;
    var sourceTokens = getSourceTokens();
    var sort = el("sort-by").value;

    filtered = all.filter(function (i) {
      if (q && i.name.toLowerCase().indexOf(q) === -1) return false;
      if (slot && i.slot !== slot) return false;
      if (quality && String(i.quality) !== quality) return false;
      // OR across selected sources: keep the item if it carries any selected token.
      if (sourceTokens.length) {
        var toks = i.sources || [];
        if (!sourceTokens.some(function (t) { return toks.indexOf(t) !== -1; })) return false;
      }
      return true;
    });
    if (sort === "name") filtered.sort(function (a, b) { return a.name.localeCompare(b.name); });
    else filtered.sort(function (a, b) { return b.runs - a.runs; });

    shown = 0;
    el("items-grid").innerHTML = "";
    el("items-empty").classList.toggle("d-none", filtered.length > 0);
    renderMore();
    updateUrl();
  }

  function itemCard(item) {
    var col = document.createElement("div");
    col.className = "col-12 col-md-6 col-xl-4";
    var a = document.createElement("a");
    a.className = "item-card";
    a.href = "/items/" + item.slug;
    // Tooltip shows the base item — the manifest carries no bonus ids, same as
    // the item page's own header link.
    a.dataset.wowhead = "item=" + item.id;
    var img = document.createElement("img");
    img.src = iconUrl(item.icon);
    img.alt = item.name;
    img.loading = "lazy";
    img.className = "border-quality-" + item.quality;
    var meta = document.createElement("div");
    meta.className = "meta flex-grow-1";
    var name = document.createElement("div");
    name.className = "name item-quality-" + item.quality;
    name.textContent = item.name;
    var sub = document.createElement("div");
    sub.className = "sub";
    var spec = item.top_spec != null ? SPECS[String(item.top_spec)] : null;
    sub.textContent = item.slot + " · " + fmt(item.runs) + " runs" +
      (spec ? " · mostly " + spec.name + " " + spec.className : "");
    meta.appendChild(name); meta.appendChild(sub);
    a.appendChild(img); a.appendChild(meta);
    col.appendChild(a);
    return col;
  }

  function renderMore() {
    var grid = el("items-grid");
    var slice = filtered.slice(shown, shown + PAGE_SIZE);
    var frag = document.createDocumentFragment();
    slice.forEach(function (i) { frag.appendChild(itemCard(i)); });
    grid.appendChild(frag);
    // Cards are rendered after page load, so power.js' own scan misses them.
    // No-op until the user consents to Wowhead (it then scans the page itself).
    if (window.$WowheadPower && typeof window.$WowheadPower.refreshLinks === "function") {
      try { window.$WowheadPower.refreshLinks(); } catch (e) { /* tooltips optional */ }
    }
    shown += slice.length;
    el("items-more").classList.toggle("d-none", shown >= filtered.length);
  }

  function init() {
    fetch("/assets/json/items_index.json")
      .then(function (r) { return r.json(); })
      .then(function (data) {
        all = data || [];
        buildSlotOptions();
        buildQualityOptions();
        buildSourceOptions();
        // After the options exist, so SLOT_BY_SLUG / SOURCE_BY_SLUG can resolve ?slot=/?source=.
        applyParamsToControls(readParams());
        applyFilters();
        el("item-search").addEventListener("input", debounce(applyFilters, 200));
        el("slot-filter").addEventListener("change", applyFilters);
        el("quality-filter").addEventListener("change", applyFilters);
        el("source-filter").addEventListener("change", applyFilters);
        el("sort-by").addEventListener("change", applyFilters);
        el("items-more").addEventListener("click", renderMore);
        // Only fires when the user navigates back to an earlier URL of this page;
        // our own replaceState writes never trigger it, so there is no loop.
        window.addEventListener("popstate", function () {
          applyParamsToControls(readParams());
          applyFilters();
        });
      })
      .catch(function () {
        el("items-empty").textContent = "Could not load item list.";
        el("items-empty").classList.remove("d-none");
      });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
