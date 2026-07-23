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

  function applyFilters() {
    var q = el("item-search").value.trim().toLowerCase();
    var slot = el("slot-filter").value;
    var quality = el("quality-filter").value;
    var sort = el("sort-by").value;

    filtered = all.filter(function (i) {
      if (q && i.name.toLowerCase().indexOf(q) === -1) return false;
      if (slot && i.slot !== slot) return false;
      if (quality && String(i.quality) !== quality) return false;
      return true;
    });
    if (sort === "name") filtered.sort(function (a, b) { return a.name.localeCompare(b.name); });
    else filtered.sort(function (a, b) { return b.runs - a.runs; });

    shown = 0;
    el("items-grid").innerHTML = "";
    el("items-empty").classList.toggle("d-none", filtered.length > 0);
    renderMore();
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
        applyFilters();
        el("item-search").addEventListener("input", debounce(applyFilters, 200));
        el("slot-filter").addEventListener("change", applyFilters);
        el("quality-filter").addEventListener("change", applyFilters);
        el("sort-by").addEventListener("change", applyFilters);
        el("items-more").addEventListener("click", renderMore);
      })
      .catch(function () {
        el("items-empty").textContent = "Could not load item list.";
        el("items-empty").classList.remove("d-none");
      });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
