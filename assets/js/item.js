/* Individual item page (item.html), served at /items/<slug>.
 *
 * The page is fully server-rendered (global view) for SEO. This script hydrates
 * the interactive bits from the inline JSON payload (#item-data):
 *   - the spec switcher re-scopes the page global <-> per-spec (no reload),
 *     honoring an optional ?spec=<id> on load for links from spec pages;
 *   - the key-level histogram / dropdown filters the dungeon breakdown.
 *
 * The "Other popular items in this slot" card is server-rendered and left as-is.
 */
(function () {
  "use strict";

  var SPECS = window.specs_map || {};
  var DUNGEONS = window.dungeons_map || {};

  var dataEl = document.getElementById("item-data");
  if (!dataEl) return;
  var DATA;
  try { DATA = JSON.parse(dataEl.textContent); } catch (e) { return; }

  function el(id) { return document.getElementById(id); }
  function iconUrl(icon) { return "/data/icons/" + icon + ".png"; }
  function fmt(n) { return (n || 0).toLocaleString(); }
  function specIcon(spec) {
    return spec && spec.icon ? "/data/icons/" + spec.icon + ".jpg" : "/data/icons/inv_misc_questionmark.png";
  }
  function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); }
  function basePath() { return window.location.pathname; }
  function qsSpec() { return new URLSearchParams(window.location.search).get("spec"); }

  // Re-scope the page to a spec (or null = global) without a full navigation.
  // Keeps any deep-link fragment so re-scoping doesn't drop the shared view.
  function setScope(specId) {
    var url = (specId ? "?spec=" + specId : basePath()) + window.location.hash;
    window.history.replaceState(null, "", url);
    renderDetail(DATA, specId);
  }

  // An <a> that re-scopes on click instead of navigating.
  function scopeLink(specId) {
    var a = document.createElement("a");
    a.href = specId ? "?spec=" + specId : basePath();
    a.addEventListener("click", function (e) { e.preventDefault(); setScope(specId); });
    return a;
  }

  // Map an item-level track tag to its tier colour class. Must mirror the
  // tier_class() macro in item.html so SSR and hydrated badges match.
  function tierClass(tag) {
    var t = (tag || "").toLowerCase();
    if (t.indexOf("raid finder") >= 0) return "tier-raidfinder";
    if (t.indexOf("mythic") >= 0) return "tier-mythic";
    if (t.indexOf("heroic") >= 0) return "tier-heroic";
    if (t.indexOf("standard") >= 0) return "tier-standard";
    return "tier-other";
  }

  // A combined spec chip: re-scopes on click, shows adoption + SIM/TOP badges
  // (mirrors the spec_overview_card macro).
  function specOverviewCard(s, active) {
    var spec = SPECS[String(s.spec_id)] || {};
    var a = scopeLink(s.spec_id);
    a.className = "spec-card" + (active ? " active" : "");
    var tip = (spec.name || "Spec " + s.spec_id) + (spec.className ? " " + spec.className : "");
    if (s.adoption != null) tip += " · " + s.adoption + "% adoption";
    if (s.is_sim) tip += " · SimulationCraft best-in-slot";
    if (s.is_top) tip += " · used by " + s.top_pct + "% of top players";
    a.title = tip;
    var img = document.createElement("img");
    img.className = "spec-icon"; img.src = specIcon(spec); img.alt = "";
    var meta = document.createElement("div");
    meta.className = "spec-card-meta";
    var nm = document.createElement("div");
    nm.className = "spec-card-name"; nm.textContent = spec.name || ("Spec " + s.spec_id);
    if (spec.color) nm.style.color = spec.color;
    var sub = document.createElement("div");
    sub.className = "spec-card-sub d-flex align-items-center gap-1";
    if (s.adoption != null) { var pv = document.createElement("span"); pv.textContent = s.adoption + "%"; sub.appendChild(pv); }
    if (s.is_sim) { var bi = document.createElement("span"); bi.className = "badge rec-badge rec-badge-sim"; bi.textContent = "SIM"; sub.appendChild(bi); }
    if (s.is_top) { var bt = document.createElement("span"); bt.className = "badge rec-badge rec-badge-top"; bt.textContent = "TOP"; sub.appendChild(bt); }
    meta.appendChild(nm); meta.appendChild(sub);
    a.appendChild(img); a.appendChild(meta);
    return a;
  }

  // (Re)build a sortable, scrollable DataTable: destroy any existing instance,
  // rebuild the tbody via buildRows, then re-init. jQuery + DataTables required;
  // degrades to a plain (scrollable) table when they're unavailable.
  function mountTable(id, buildRows, order, colDefs) {
    var $ = window.jQuery;
    var tableEl = el(id);
    if (!tableEl) return;
    var hasDT = $ && $.fn && $.fn.dataTable;
    if (hasDT && $.fn.dataTable.isDataTable(tableEl)) $(tableEl).DataTable().destroy();
    var tbody = tableEl.querySelector("tbody");
    if (!tbody) return;
    clear(tbody);
    buildRows(tbody);
    if (hasDT) {
      try {
        $(tableEl).DataTable({
          paging: false, searching: false, info: false, autoWidth: false,
          order: order || [], columnDefs: colDefs || [], language: { emptyTable: "—" },
        });
      } catch (e) { /* sortable table is optional */ }
    }
  }

  // A right-aligned numeric cell carrying a data-order for DataTables sorting.
  function numCell(order, text) {
    var c = document.createElement("td");
    c.className = "text-center";
    c.setAttribute("data-order", order == null ? 0 : order);
    var s = document.createElement("span");
    s.className = "text-xs font-weight-bold";
    s.textContent = text == null ? "" : text;
    c.appendChild(s);
    return c;
  }

  // One enhancement / gem option as a DataTable row (mirrors the opt_row macro).
  function optRow(tbody, o, kind) {
    var tr = document.createElement("tr");
    var c1 = document.createElement("td");
    var a = document.createElement("a");
    a.className = "dt-name";
    a.href = (kind === "enchant" && o.spellId)
      ? "https://www.wowhead.com/spell=" + o.spellId
      : "https://www.wowhead.com/item=" + o.id;
    a.target = "_blank"; a.rel = "noopener";
    if (kind !== "enchant") a.setAttribute("data-wowhead", "item=" + o.id);
    var img = document.createElement("img");
    img.className = "gem-icon"; img.src = iconUrl(o.icon); img.alt = "";
    var nm = document.createElement("span"); nm.textContent = o.name || ("#" + o.id);
    // Proper text colour: enchant green for enchants, item-quality colour for
    // gems / embellishments / missives (mirrors opt_name_class in item.html).
    if (kind === "enchant") nm.className = "enchant-name";
    else if (o.quality != null) nm.className = "item-quality-" + o.quality;
    a.appendChild(img); a.appendChild(nm); c1.appendChild(a);
    tr.appendChild(c1);
    tr.appendChild(numCell(o.pct || 0, (o.pct != null ? o.pct : 0) + "%"));
    tr.appendChild(numCell(o.runs || 0, o.runs != null ? fmt(o.runs) : ""));
    tbody.appendChild(tr);
  }

  // One spec as a "Used by Specs" DataTable row (mirrors the spec_row macro); the
  // name cell links to that spec's class/role page (not a re-scope).
  function specRow(tbody, s, slot) {
    var spec = SPECS[String(s.spec_id)] || {};
    var tr = document.createElement("tr");
    var c1 = document.createElement("td");
    var a = document.createElement("a");
    a.href = spec.page || ("?spec=" + s.spec_id);
    a.className = "dt-name";
    a.title = s.slot_runs
      ? fmt(s.runs) + " of " + fmt(s.slot_runs) + " " + (slot || "slot") + " runs"
      : fmt(s.runs) + " runs";
    var img = document.createElement("img");
    img.className = "spec-icon"; img.src = specIcon(spec); img.alt = "";
    var nm = document.createElement("span");
    nm.textContent = (spec.name || "Spec " + s.spec_id) + (spec.className ? " " + spec.className : "");
    if (spec.color) nm.style.color = spec.color;
    a.appendChild(img); a.appendChild(nm); c1.appendChild(a);
    tr.appendChild(c1);
    tr.appendChild(numCell(s.adoption || 0, s.adoption != null ? s.adoption + "%" : "—"));
    tr.appendChild(numCell(s.runs || 0, fmt(s.runs)));
    var mk = s.max_timed_key ? "+" + s.max_timed_key
      : (s.max_depleted_key ? "+" + s.max_depleted_key + " (D)" : "—");
    tr.appendChild(numCell(s.max_timed_key || s.max_depleted_key || 0, mk));
    tbody.appendChild(tr);
  }

  // The Wowhead query string for the item at the current scope. Base item only
  // (no bonus track — the header shows the canonical item, not a variant); the
  // spec param just scopes the tooltip when viewing a single spec.
  function whData(data, specId) {
    return "item=" + data.id + (specId ? "&spec=" + specId : "");
  }

  function renderDetail(data, specId) {
    var scope = specId && data.bySpec && data.bySpec[specId] ? data.bySpec[specId] : data.global;
    var scoped = !!(specId && data.bySpec && data.bySpec[specId]);

    // Header links (Wowhead button + tooltip-bearing name & icon) track the scope.
    var wd = whData(data, specId);
    ["item-wowhead", "item-name", "item-icon-link"].forEach(function (id) {
      var e = el(id);
      if (!e) return;
      e.href = "https://www.wowhead.com/" + wd;
      if (id !== "item-wowhead") e.setAttribute("data-wowhead", wd);
    });

    // Total-runs stat box (number + adoption tooltip) and highest-key box.
    var scopeEl = el("item-scope");
    if (scopeEl) scopeEl.textContent = fmt(scope.total_runs);
    var runsBox = el("item-runs-box");
    if (runsBox) {
      runsBox.title = scope.slot_runs
        ? fmt(scope.total_runs) + " of " + fmt(scope.slot_runs) + " runs with a " + (data.slot || "slot") +
          " item (" + (scope.adoption != null ? scope.adoption : "?") + "%)"
        : fmt(scope.total_runs) + " runs";
    }
    var hkBox = el("item-highkey-box"), hk = el("item-highkey");
    if (hkBox && hk) {
      if (scope.max_timed_key) { hk.textContent = "+" + scope.max_timed_key; hkBox.classList.remove("d-none"); }
      else if (scope.max_depleted_key) { hk.textContent = "+" + scope.max_depleted_key + " (D)"; hkBox.classList.remove("d-none"); }
      else { hkBox.classList.add("d-none"); }
    }

    // Scope note under the title: which spec we're viewing + a "view all" link.
    var note = el("item-scope-note");
    if (note) {
      clear(note);
      if (scoped) {
        var spec = SPECS[specId] || {};
        note.appendChild(document.createTextNode("Viewing " + (spec.name || "Spec " + specId) +
          " " + (spec.className || "") + " · "));
        var allLink = scopeLink(null);
        allLink.textContent = "View all specs ›";
        note.appendChild(allLink);
      }
    }

    renderSpecSwitcher(data, specId);
    renderEnhancements(scope);
    // Set pieces are scope-independent; the server-rendered #item-set block is
    // left as-is (no JS rebuild needed).
    renderSpecPopularity(data, scoped);
    setupKeyLevelFilter(scope);
    renderKeyLevels(scope);
    renderDungeons(scope, []);
    renderVariants(scope);

    if (window.$WowheadPower && typeof window.$WowheadPower.refreshLinks === "function") {
      try { window.$WowheadPower.refreshLinks(); } catch (e) { /* tooltips optional */ }
    }
  }

  // Combined spec section (re-scope switcher + SimC/top-player recommendations).
  // Below md the card grid is hidden (items.css) and a native single-select
  // takes over; both are rebuilt here and kept in sync with the current scope.
  function renderSpecSwitcher(data, specId) {
    var wrap = el("spec-switcher"); clear(wrap);
    var card = el("spec-overview-card");
    var specs = data.spec_overview || [];
    if (!specs.length) { if (card) card.classList.add("d-none"); return; }
    if (card) card.classList.remove("d-none");
    var all = scopeLink(null);
    all.className = "spec-card spec-card-all" + (specId ? "" : " active");
    var allMeta = document.createElement("div");
    allMeta.className = "spec-card-meta";
    var allName = document.createElement("div");
    allName.className = "spec-card-name"; allName.textContent = "All specs";
    allMeta.appendChild(allName); all.appendChild(allMeta);
    wrap.appendChild(all);
    specs.forEach(function (s) {
      wrap.appendChild(specOverviewCard(s, String(s.spec_id) === String(specId)));
    });
    renderSpecSelect(specs, specId);
  }

  // One mobile spec-dropdown option: icon + name + adoption — SIM/TOP live in
  // the surrounding optgroup labels (an <option> can't carry badges). Same
  // data-content pattern as the routes page filters. Mirrors the
  // spec_select_option macro in item.html.
  function specSelectOption(s) {
    var spec = SPECS[String(s.spec_id)] || {};
    var o = document.createElement("option");
    o.value = String(s.spec_id);
    var name = spec.name || "Spec " + s.spec_id;
    o.textContent = name + (spec.className ? " " + spec.className : "") +
      (s.adoption != null ? " · " + s.adoption + "%" : "");
    o.setAttribute("data-content",
      "<span class='dropdown-icon-item'>" +
      "<img src='" + specIcon(spec) + "' class='dropdown-icon' alt=''>" +
      "<span class='dropdown-icon-label'>" + name +
      (spec.className
        ? " <small" + (spec.color ? " style='color:" + spec.color + "'" : "") + ">" + spec.className + "</small>"
        : "") +
      "</span>" +
      (s.adoption != null ? "<small class='spec-opt-pct'>" + s.adoption + "%</small>" : "") +
      "</span>");
    return o;
  }

  // Mobile spec dropdown (mirrors the #spec-select markup in item.html). Pages
  // built before the select was added to the template lack the SSR version, so
  // it's created on demand. Rebuilt with the same destroy -> init dance as the
  // key-level filter so options don't duplicate across re-scopes.
  function renderSpecSelect(specs, specId) {
    var sel = el("spec-select");
    if (!sel) {
      var wrapper = document.createElement("div");
      wrapper.className = "spec-select-wrap d-md-none mb-2";
      sel = document.createElement("select");
      sel.id = "spec-select";
      sel.className = "selectpicker form-control";
      sel.setAttribute("data-style", "btn-outline-primary");
      sel.setAttribute("data-width", "100%");
      sel.setAttribute("data-sanitize", "false");
      sel.setAttribute("aria-label", "Select spec");
      wrapper.appendChild(sel);
      var grid = el("spec-switcher");
      grid.parentNode.insertBefore(wrapper, grid);
    }
    var $ = window.jQuery;
    var $s = $ && $.fn && $.fn.selectpicker ? $(sel) : null;
    if ($s) { try { $s.selectpicker("destroy"); } catch (e) { /* not yet initialised */ } }
    clear(sel);
    var optAll = document.createElement("option");
    optAll.value = ""; optAll.textContent = "All specs";
    sel.appendChild(optAll);
    var sim = [], top = [], rest = [];
    specs.forEach(function (s) {
      if (s.is_top) top.push(s);
      else if (s.is_sim) sim.push(s);
      else rest.push(s);
    });
    function addGroup(label, list) {
      if (!list.length) return;
      var og = document.createElement("optgroup");
      og.label = label;
      list.forEach(function (s) { og.appendChild(specSelectOption(s)); });
      sel.appendChild(og);
    }
    if (sim.length || top.length) {
      addGroup("Top players' pick", top);
      addGroup("SimulationCraft best-in-slot", sim);
      addGroup("By adoption", rest);
    } else {
      rest.forEach(function (s) { sel.appendChild(specSelectOption(s)); });
    }
    sel.value = specId ? String(specId) : "";
    // width:"100%" is synchronous (no width:"auto" async-measure crash, see
    // setupKeyLevelFilter); sanitize:false keeps the data-content icon markup.
    if ($s) { try { $s.selectpicker({ width: "100%", style: "btn-outline-primary", sanitize: false }); } catch (e) { /* picker is optional */ } }
    if (!sel.dataset.wired) {
      sel.dataset.wired = "1";
      sel.addEventListener("change", function () { setScope(sel.value || null); });
    }
  }

  // barPct is the fill width (0-100, relative to the row group's max so the
  // ranking is visually readable); countText is the literal right-hand label.
  function usageRow(iconEl, label, barPct, countText) {
    var row = document.createElement("div");
    row.className = "usage-row";
    row.appendChild(iconEl);
    var mid = document.createElement("div");
    var lbl = document.createElement("div");
    lbl.className = "usage-label";
    lbl.textContent = label;
    var bar = document.createElement("div");
    bar.className = "usage-bar";
    var span = document.createElement("span");
    span.style.width = Math.max(2, Math.min(100, barPct)) + "%";
    bar.appendChild(span);
    mid.appendChild(lbl); mid.appendChild(bar);
    row.appendChild(mid);
    var cnt = document.createElement("div");
    cnt.className = "usage-label text-end";
    cnt.textContent = countText;
    row.appendChild(cnt);
    return row;
  }

  // "Used by Specs" sortable/scrollable DataTable (only in the global view).
  function renderSpecPopularity(data, scoped) {
    var col = el("spec-popularity-col");
    if (scoped) { col.classList.add("d-none"); return; }
    col.classList.remove("d-none");
    var specs = data.global.specs || [];
    mountTable("spec-popularity-table", function (tbody) {
      specs.forEach(function (s) { specRow(tbody, s, data.slot); });
    }, [[1, "desc"]], [{ targets: 0, orderable: false }]);
  }

  var curScope = null;

  // Format a set of key levels: runs of 3+ consecutive levels collapse to
  // "18-20"; shorter runs stay as "+18, +19". e.g. [16,18,19,20] -> "+16, 18-20".
  function formatLevels(levels) {
    var s = levels.slice().sort(function (a, b) { return a - b; });
    var out = [], i = 0;
    while (i < s.length) {
      var j = i;
      while (j + 1 < s.length && s[j + 1] === s[j] + 1) j++;
      if (j - i + 1 >= 3) {
        out.push(s[i] + "-" + s[j]);
      } else {
        for (var k = i; k <= j; k++) out.push("+" + s[k]);
      }
      i = j + 1;
    }
    return out.join(", ");
  }

  function $kl() { return window.jQuery ? window.jQuery("#keylevel-filter") : null; }

  // Currently selected key levels (numbers); empty array = all.
  function selectedLevels() {
    var $s = $kl();
    if (!$s || !$s.selectpicker) return [];
    var v = $s.selectpicker("val");
    return (v || []).map(Number);
  }

  // Build the multi-select cleanly (destroy any existing widget first so options
  // aren't duplicated), then wire change + chart-click handlers.
  function setupKeyLevelFilter(scope) {
    curScope = scope;
    var $s = $kl();
    var sel = el("keylevel-filter");
    var levels = scope.keylevels || [];
    var html = levels.map(function (k) {
      return '<option value="' + k.level + '">+' + k.level + "</option>";
    }).join("");
    if ($s && $s.selectpicker) {
      // Tear down any previous widget first so options don't duplicate.
      try { $s.selectpicker("destroy"); } catch (e) { /* not yet initialised */ }
      $s.html(html);
      // Only (re)init the picker when there are options — initialising bootstrap-
      // select on an empty <select> throws in its liHeight() height measurement.
      if (levels.length) {
        // Force width:"fit" (overriding any data-width="auto"): bootstrap-select's
        // width:"auto" path measures the menu asynchronously on its "loaded" event,
        // and liHeight() throws "Cannot read properties of null (reading
        // 'className')" when the menu's parent isn't attached yet at that tick.
        // "fit" sizes the button to its content synchronously, no async measure.
        try { $s.selectpicker({ width: "fit" }); } catch (e) { /* picker is optional */ }
        $s.off("changed.bs.select").on("changed.bs.select", function () { applySelection(); });
      }
    } else if (sel) {
      sel.innerHTML = html;
    }
    // Hide the (now empty) filter control when there's nothing to filter.
    if (sel) {
      var wrapper = sel.closest(".btn-group") || sel;
      if (wrapper && wrapper.classList) wrapper.classList.toggle("d-none", !levels.length);
    }
  }

  // Render the dungeon breakdown for the current selection and sync bar highlights.
  function applySelection() {
    var levels = selectedLevels();
    renderDungeons(curScope, levels);
    el("item-keylevels").querySelectorAll(".key-bar").forEach(function (b) {
      b.classList.toggle("selected", levels.indexOf(Number(b.getAttribute("data-level"))) >= 0);
    });
    if (window.MythiLink) window.MythiLink.sync();
  }

  // The key-level filter rewrites the whole dungeon breakdown, so it belongs in a
  // shared link. Registered at parse time; deep-link.js resolves one tick after
  // DOMContentLoaded, by which point init() has built the picker.
  if (window.MythiLink) {
    window.MythiLink.registerState("keys", {
      read: function () {
        var levels = selectedLevels();
        return levels.length ? levels.join(",") : null;
      },
      apply: function (value) {
        var levels = String(value).split(",")
          .map(Number)
          .filter(function (n) { return !isNaN(n); })
          .map(String);
        var $s = $kl();
        // selectpicker("val") fires changed.bs.select -> applySelection.
        if ($s && $s.selectpicker) $s.selectpicker("val", levels);
        else applySelection();
      }
    });
  }

  function toggleLevel(level) {
    var levels = selectedLevels();
    var i = levels.indexOf(level);
    if (i >= 0) levels.splice(i, 1); else levels.push(level);
    var $s = $kl();
    if ($s) $s.selectpicker("val", levels.map(String)); // fires changed.bs.select -> applySelection
    else applySelection();
  }

  // Adoption rate per key level: % of runs at that key level that use the item.
  // Bars are clickable to toggle that level in the filter.
  function renderKeyLevels(scope) {
    var box = el("item-keylevels"); clear(box);
    var levels = (scope.keylevels || []).filter(function (k) { return k.adoption != null; });
    if (!levels.length) return;
    var maxAdopt = Math.max.apply(null, levels.map(function (k) { return k.adoption; })) || 1;
    var hist = document.createElement("div");
    hist.className = "key-histogram mt-2 mb-3";
    levels.forEach(function (k) {
      var bar = document.createElement("div");
      bar.className = "key-bar";
      bar.setAttribute("data-level", String(k.level));
      bar.style.height = Math.max(6, k.adoption / maxAdopt * 100) + "%";
      bar.title = "+" + k.level + ": " + k.adoption + "% of runs (" + fmt(k.runs) + ") — click to filter";
      bar.addEventListener("click", function () { toggleLevel(k.level); });
      var num = document.createElement("span");
      num.className = "key-num"; num.textContent = k.level;
      bar.appendChild(num);
      hist.appendChild(bar);
    });
    box.appendChild(hist);
  }

  // Adoption per dungeon. With nothing selected, shows each dungeon's overall
  // adoption. With key levels selected, sums the item's runs over those levels
  // and divides by the same levels' total runs (global view); in the spec view
  // the per-spec/per-key-level denominator isn't available, so it shows run
  // counts instead.
  function renderDungeons(scope, levels) {
    var box = el("item-dungeons"); clear(box);
    var dungeons = scope.dungeons || [];
    if (!dungeons.length) {
      box.innerHTML = '<p class="text-sm opacity-6 mb-0">No per-dungeon usage recorded.</p>';
      return;
    }
    levels = levels || [];
    var rows = dungeons.map(function (d) {
      if (!levels.length) return { d: d, adoption: d.adoption, runs: d.runs };
      var runs = 0, denom = 0, hasDenom = false;
      levels.forEach(function (lvl) {
        var key = String(lvl);
        runs += (d.by_key && d.by_key[key]) || 0;
        if (d.by_key_total && d.by_key_total[key] != null) { denom += d.by_key_total[key]; hasDenom = true; }
      });
      var adoption = (hasDenom && denom > 0) ? Math.round(Math.min(100, runs / denom * 100) * 10) / 10 : null;
      return { d: d, adoption: adoption, runs: runs };
    }).filter(function (r) { return levels.length ? r.runs > 0 : true; });

    if (!rows.length) {
      box.innerHTML = '<p class="text-sm opacity-6 mb-0">Not used at the selected key levels.</p>';
      return;
    }
    rows.sort(function (a, b) { return (b.adoption || 0) - (a.adoption || 0) || b.runs - a.runs; });
    var maxAdopt = Math.max.apply(null, rows.map(function (r) { return r.adoption || 0; })) || 1;
    var maxRuns = Math.max.apply(null, rows.map(function (r) { return r.runs || 0; })) || 1;
    var suffix = levels.length ? " runs at " + formatLevels(levels) : " runs";
    rows.forEach(function (r) {
      var d = r.d, dung = DUNGEONS[String(d.id)] || {};
      var img = document.createElement("img");
      img.className = "spec-icon";
      img.src = dung.icon ? "/data/icons/" + dung.icon : "/data/icons/inv_misc_questionmark.png";
      img.alt = "";
      var hasPct = r.adoption != null;
      var right = (hasPct ? r.adoption + "% · " : "") + fmt(r.runs) +
        (levels.length ? suffix : suffix + " · max +" + d.max_key);
      var barPct = hasPct ? (r.adoption / maxAdopt * 100) : (r.runs / maxRuns * 100);
      box.appendChild(usageRow(img, dung.name || "Dungeon " + d.id, barPct, right));
    });
  }

  function renderVariants(scope) {
    var box = el("item-variants"); clear(box);
    var variants = scope.variants || [];
    if (!variants.length) {
      box.innerHTML = '<p class="text-sm opacity-6 mb-0">No item-level variants recorded.</p>';
      return;
    }
    variants.forEach(function (v) {
      var row = document.createElement("div");
      row.className = "mb-2 pb-2 border-bottom border-secondary";
      var head = document.createElement("div");
      head.className = "d-flex justify-content-between";
      var left = document.createElement("div");
      (v.tags || []).forEach(function (t) {
        var b = document.createElement("span");
        b.className = "tier-badge " + tierClass(t) + " variant-tag";
        b.textContent = t;
        left.appendChild(b);
      });
      if (v.ilvl) {
        var ib = document.createElement("span");
        ib.className = "tier-badge tier-ilvl variant-tag";
        ib.textContent = "ilvl " + v.ilvl;
        left.appendChild(ib);
      }
      if (v.sockets) {
        var sb = document.createElement("span");
        sb.className = "badge bg-secondary variant-tag";
        sb.textContent = "+" + v.sockets + " socket" + (v.sockets > 1 ? "s" : "");
        left.appendChild(sb);
      }
      (v.crafted_stats || []).forEach(function (cs) {
        var b = document.createElement("span");
        b.className = "badge stat-" + cs + " variant-tag";
        b.textContent = cs;
        left.appendChild(b);
      });
      if (!left.childNodes.length) {
        var std = document.createElement("span");
        std.className = "tier-badge tier-standard variant-tag";
        std.textContent = "Standard";
        left.appendChild(std);
      }
      var right = document.createElement("span");
      right.className = "usage-label";
      right.textContent = v.pct + "% · " + fmt(v.runs);
      head.appendChild(left); head.appendChild(right);
      row.appendChild(head);
      box.appendChild(row);
    });
  }

  // Enhancements & gems: one DataTable per type (enchant / gems / embellishment /
  // missive), each listing every option used, with its share. Gems sit next to
  // enchants in the same responsive grid. Hides empty cards and the wrapper.
  function renderEnhancements(scope) {
    var groups = [
      { table: "enchant-table", card: "enchant-card", list: scope.enchants, kind: "enchant" },
      { table: "gems-table", card: "gems-col", list: scope.gems, kind: "item" },
      { table: "embellishment-table", card: "embellishment-card", list: scope.embellishments, kind: "item" },
      { table: "missive-table", card: "missive-card", list: scope.missives, kind: "item" },
    ];
    var any = false;
    groups.forEach(function (grp) {
      var card = el(grp.card);
      var list = grp.list || [];
      if (!list.length) { if (card) card.classList.add("d-none"); return; }
      if (card) card.classList.remove("d-none");
      mountTable(grp.table, function (tbody) {
        list.forEach(function (o) { optRow(tbody, o, grp.kind); });
      }, [[1, "desc"]], [{ targets: 0, orderable: false }]);
      any = true;
    });
    var wrap = el("enhance-wrap");
    if (wrap) wrap.classList.toggle("d-none", !any);
  }

  function init() {
    // SSR already shows the global view; re-render to wire interactivity and to
    // apply an incoming ?spec= scope from a spec-page link.
    var specId = qsSpec();
    renderDetail(DATA, specId && DATA.bySpec && DATA.bySpec[specId] ? specId : null);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
