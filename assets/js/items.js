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

  // The rarity to colour/filter a card by: the most-used variant's bonus-id
  // resolved quality when present, else the base item quality. Mirrors the item
  // page header (item.quality_override). quality_override is only emitted when it
  // differs from the base, so it stays absent for the common case.
  function effQuality(i) {
    return i.quality_override != null ? i.quality_override : i.quality;
  }

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
  // Armor-type filter (single-select). Each item's manifest `armor` token
  // ("cloth"/"leather"/"mail"/"plate"/"shield", absent for slots with no weight)
  // is both the <option> value and the ?armor= slug, so no map is needed. Order
  // mirrors the character sheet's weight progression.
  var ARMOR_NAMES = { cloth: "Cloth", leather: "Leather", mail: "Mail", plate: "Plate", shield: "Shield" };
  var ARMOR_ORDER = ["cloth", "leather", "mail", "plate", "shield"];
  // Class filter (multi-select), grouped by class with per-spec options, driven by
  // each item's manifest `specs` (the spec ids that actually equipped it). Tokens
  // are "c:<classSlug>" (whole class) and "s:<specId>" (one spec); the URL carries
  // readable slugs instead. Both maps are filled by buildClassOptions.
  var CLASS_TOKEN_BY_SLUG = {}; // "retribution-paladin" -> "s:70", "paladin" -> "c:paladin"
  var CLASS_SLUG_BY_TOKEN = {}; // "s:70" -> "retribution-paladin"
  function classSlug(name) { return String(name).toLowerCase().replace(/\s+/g, "-"); }

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

  // The class filter is a multi-select too, so its value is an array of tokens.
  function getClassTokens() {
    if (window.jQuery && window.jQuery.fn.selectpicker) {
      var v = window.jQuery("#class-filter").selectpicker("val");
      return v == null ? [] : (Array.isArray(v) ? v : [v]);
    }
    var out = [], opts = el("class-filter").options;
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
    all.forEach(function (i) { var q = effQuality(i); if (q != null) qualities[q] = true; });
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

  // Same dropdown markup as iconContent, but for a spec's SpellIconFileId, which
  // is stored as <id>.jpg (not a name-slug + .png). Mirrors specIcon in item.js.
  function specIconContent(iconFileId, label) {
    if (!iconFileId) return null;
    return "<span class='dropdown-icon-item'><img src='/data/icons/" + iconFileId +
      ".jpg' class='dropdown-icon' alt='' style='width:20px;height:20px;border-radius:4px;" +
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
      // "All Dungeons" (synthetic d:* token), mirroring each raid's "All of <Raid>";
      // matched specially in applyFilters against any of the item's d: tokens.
      addSourceOption(dog, "d:*", "All Dungeons", "all-dungeons", null);
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
        return { enc: enc, name: b.name || ("Boss " + enc), slug: b.slug || ("boss-" + enc),
                 icon: b.icon };
      }).sort(function (a, b) {
        return String(a.name).localeCompare(String(b.name));
      }).forEach(function (b) {
        // Boss portrait icon (data/icons/boss_<enc>.png from fetchRaidData); a boss
        // with no resolved creature display id has no icon and falls back to text.
        addSourceOption(og, "b:" + r.id + ":" + b.enc, b.name, r.slug + "--" + b.slug,
          iconContent(b.icon, b.name));
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

  // Armor type is a plain single-select (like Slot/Quality). Only weights present
  // in the current data are offered, in character-sheet order.
  function buildArmorOptions() {
    var present = {};
    all.forEach(function (i) { if (i.armor) present[i.armor] = true; });
    var sel = el("armor-filter");
    var allOpt = document.createElement("option");
    allOpt.value = ""; allOpt.textContent = "All armor";
    sel.appendChild(allOpt);
    ARMOR_ORDER.forEach(function (tok) {
      if (!present[tok]) return;
      var o = document.createElement("option");
      o.value = tok; o.textContent = ARMOR_NAMES[tok];
      sel.appendChild(o);
    });
    refreshPicker("armor-filter");
  }

  // Class filter: one optgroup per class (only classes present in the data), each
  // with an "All <Class>" option plus one option per spec that actually equipped
  // an item. Membership comes from window.specs_map, never a who-can-wear table.
  function buildClassOptions() {
    var specsPresent = {}; // spec_id (string) -> true
    all.forEach(function (i) {
      (i.specs || []).forEach(function (sid) { specsPresent[String(sid)] = true; });
    });

    // Group present specs by class, using specs_map for names/icons/colour.
    var classes = {}; // classSlug -> { name, specs: [{id, name, icon}] }
    Object.keys(specsPresent).forEach(function (sid) {
      var sp = SPECS[sid];
      if (!sp) return; // spec absent from specs_map (stale data) — drop it, don't crash
      var cslug = classSlug(sp.className);
      var c = classes[cslug] || (classes[cslug] = { name: sp.className, specs: [] });
      c.specs.push({ id: sid, name: sp.name, icon: sp.icon });
    });

    var sel = el("class-filter");
    sel.innerHTML = ""; // multi-select: empty selection means "all", no reset option
    Object.keys(classes).map(function (cslug) {
      return { slug: cslug, name: classes[cslug].name, specs: classes[cslug].specs };
    }).sort(function (a, b) {
      return String(a.name).localeCompare(String(b.name));
    }).forEach(function (c) {
      var og = document.createElement("optgroup");
      og.label = c.name;
      addClassOption(og, "c:" + c.slug, "All " + c.name, c.slug, null);
      c.specs.sort(function (a, b) { return String(a.name).localeCompare(String(b.name)); })
        .forEach(function (s) {
          addClassOption(og, "s:" + s.id, s.name, classSlug(s.name) + "-" + c.slug,
            specIconContent(s.icon, s.name));
        });
      sel.appendChild(og);
    });
    refreshPicker("class-filter");
  }

  function addClassOption(parent, token, label, slug, content) {
    var o = document.createElement("option");
    o.value = token; o.textContent = label;
    o.setAttribute("data-tokens", label); // enables live-search by name
    if (content) o.setAttribute("data-content", content);
    parent.appendChild(o);
    CLASS_TOKEN_BY_SLUG[slug] = token;
    CLASS_SLUG_BY_TOKEN[token] = slug;
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
    var armor = (sp.get("armor") || "").toLowerCase();
    return {
      q: sp.get("q") || "",
      // Slugging first means a raw display name (?slot=Held%20In%20Off-hand) resolves too.
      slot: SLOT_BY_SLUG[slotSlug(slot)] || "",
      quality: QUALITY_BY_SLUG[quality] || (/^\d+$/.test(quality) ? quality : ""),
      // Armor token is its own slug; keep it only if it's one we know about.
      armor: ARMOR_NAMES[armor] ? armor : "",
      // Comma-separated list of readable source slugs (a lone legacy dungeon slug
      // still resolves). Raw tokens (d:.., r:.., b:.., crafted, tier, pvp, other) pass through.
      source: parseSourceParam(source),
      // Comma-separated readable class/spec slugs ("retribution-paladin", "paladin").
      "class": parseClassParam(sp.get("class") || ""),
      sort: sort === "name" || sort === "runs" ? sort : "runs",
    };
  }

  // "retribution-paladin,paladin" -> ["s:70","c:paladin"]. Unknown slugs drop.
  function parseClassParam(raw) {
    if (!raw) return [];
    return raw.split(",").map(function (s) { return s.trim(); }).filter(Boolean)
      .map(function (s) {
        if (CLASS_TOKEN_BY_SLUG[s]) return CLASS_TOKEN_BY_SLUG[s]; // readable slug
        if (CLASS_SLUG_BY_TOKEN[s]) return s;                     // already a token
        return null;
      }).filter(Boolean);
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
    setSelect("armor-filter", p.armor);
    setSelect("quality-filter", p.quality);
    setSelect("source-filter", p.source);
    setSelect("class-filter", p["class"]);
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
    var armor = el("armor-filter").value;
    var quality = el("quality-filter").value;
    var sourceTokens = getSourceTokens();
    var classTokens = getClassTokens();
    var sort = el("sort-by").value;
    if (slot) sp.set("slot", slotSlug(slot));
    if (armor) sp.set("armor", armor);
    if (quality) sp.set("quality", (QUALITY_NAMES[quality] || quality).toLowerCase());
    if (sourceTokens.length) {
      sp.set("source", sourceTokens.map(function (t) { return SLUG_BY_TOKEN[t] || t; }).join(","));
    }
    if (classTokens.length) {
      sp.set("class", classTokens.map(function (t) { return CLASS_SLUG_BY_TOKEN[t] || t; }).join(","));
    }
    if (sort && sort !== "runs") sp.set("sort", sort);
    if (q) sp.set("q", q);
    var qs = sp.toString();
    window.history.replaceState(null, "", window.location.pathname + (qs ? "?" + qs : ""));
  }

  // True if any selected class/spec token matches a spec that equipped the item.
  // "s:<id>" is a direct spec-id hit; "c:<slug>" hits when any equipping spec
  // belongs to that class (resolved via specs_map, not a who-can-wear table).
  function itemMatchesClass(item, classTokens) {
    var specsList = item.specs || [];
    if (!specsList.length) return false;
    return classTokens.some(function (t) {
      if (t.charAt(0) === "s") {
        var id = parseInt(t.slice(2), 10);
        return specsList.indexOf(id) !== -1;
      }
      var cslug = t.slice(2); // "c:<classSlug>"
      return specsList.some(function (sid) {
        var sp = SPECS[String(sid)];
        return sp && classSlug(sp.className) === cslug;
      });
    });
  }

  function applyFilters() {
    var q = el("item-search").value.trim().toLowerCase();
    var slot = el("slot-filter").value;
    var armor = el("armor-filter").value;
    var quality = el("quality-filter").value;
    var sourceTokens = getSourceTokens();
    var classTokens = getClassTokens();
    var sort = el("sort-by").value;

    filtered = all.filter(function (i) {
      if (q && i.name.toLowerCase().indexOf(q) === -1) return false;
      if (slot && i.slot !== slot) return false;
      if (armor && i.armor !== armor) return false;
      if (quality && String(effQuality(i)) !== quality) return false;
      // OR across selected sources: keep the item if it carries any selected token.
      // "d:*" (All Dungeons) matches any item carrying at least one d: token.
      if (sourceTokens.length) {
        var toks = i.sources || [];
        var srcOk = sourceTokens.some(function (t) {
          if (t === "d:*") return toks.some(function (x) { return x.indexOf("d:") === 0; });
          return toks.indexOf(t) !== -1;
        });
        if (!srcOk) return false;
      }
      // OR across selected class/spec tokens: keep the item if any selected spec
      // (s:<id>) or class (c:<slug>, any of its specs) actually equipped it.
      if (classTokens.length && !itemMatchesClass(i, classTokens)) return false;
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
    img.className = "border-quality-" + effQuality(item);
    var meta = document.createElement("div");
    meta.className = "meta flex-grow-1";
    var name = document.createElement("div");
    name.className = "name item-quality-" + effQuality(item);
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
        buildArmorOptions();
        buildQualityOptions();
        buildSourceOptions();
        buildClassOptions();
        // After the options exist, so the *_BY_SLUG maps can resolve ?slot=/?armor=/?source=/?class=.
        applyParamsToControls(readParams());
        applyFilters();
        el("item-search").addEventListener("input", debounce(applyFilters, 200));
        el("slot-filter").addEventListener("change", applyFilters);
        el("armor-filter").addEventListener("change", applyFilters);
        el("quality-filter").addEventListener("change", applyFilters);
        el("source-filter").addEventListener("change", applyFilters);
        el("class-filter").addEventListener("change", applyFilters);
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
