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
  function setScope(specId) {
    var url = specId ? "?spec=" + specId : basePath();
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

  // A single spec rendered as a re-scoping card (mirrors the spec_card macro).
  function specCard(specId, sub, active) {
    var spec = SPECS[String(specId)] || {};
    var a = scopeLink(specId);
    a.className = "spec-card" + (active ? " active" : "");
    var img = document.createElement("img");
    img.className = "spec-icon"; img.src = specIcon(spec); img.alt = "";
    var meta = document.createElement("div");
    meta.className = "spec-card-meta";
    var nm = document.createElement("div");
    nm.className = "spec-card-name"; nm.textContent = spec.name || ("Spec " + specId);
    meta.appendChild(nm);
    if (sub) {
      var sb = document.createElement("div");
      sb.className = "spec-card-sub"; sb.textContent = sub;
      meta.appendChild(sb);
    }
    a.appendChild(img); a.appendChild(meta);
    return a;
  }

  // A craft/enchant choice rendered as a card (mirrors the craft_card macro).
  function craftCard(kind, name, icon, href, sub, wowheadId) {
    var a = document.createElement("a");
    a.className = "craft-card"; a.href = href; a.target = "_blank"; a.rel = "noopener";
    if (wowheadId) a.setAttribute("data-wowhead", "item=" + wowheadId);
    if (icon) {
      var im = document.createElement("img");
      im.className = "gem-icon"; im.src = iconUrl(icon); im.alt = "";
      a.appendChild(im);
    }
    var meta = document.createElement("div");
    meta.className = "craft-card-meta";
    var k = document.createElement("div");
    k.className = "craft-card-kind"; k.textContent = kind;
    var nm = document.createElement("div");
    nm.className = "craft-card-name"; nm.textContent = name || "View item";
    meta.appendChild(k); meta.appendChild(nm);
    if (sub) {
      var sb = document.createElement("div");
      sb.className = "craft-card-sub"; sb.textContent = sub;
      meta.appendChild(sb);
    }
    a.appendChild(meta);
    return a;
  }

  // Hide the Enhancements card entirely when this scope has no enchant/craft.
  function toggleEnhance() {
    var card = el("enhance-card");
    if (!card) return;
    var has = el("item-enchant").childNodes.length || el("item-craft").childNodes.length;
    card.classList.toggle("d-none", !has);
  }

  function renderDetail(data, specId) {
    var scope = specId && data.bySpec && data.bySpec[specId] ? data.bySpec[specId] : data.global;
    var scoped = !!(specId && data.bySpec && data.bySpec[specId]);

    // Wowhead link (rebuild so the spec param tracks the current scope).
    var wh = el("item-wowhead");
    if (wh) {
      wh.href = "https://www.wowhead.com/item=" + data.id +
        (data.wowheadBonus ? "&bonus=" + data.wowheadBonus : "") +
        (specId ? "&spec=" + specId : "");
    }

    var scopeEl = el("item-scope"); clear(scopeEl);
    if (scoped) {
      var spec = SPECS[specId] || {};
      var allLink = scopeLink(null);
      allLink.className = "small";
      allLink.textContent = "View all specs ›";
      scopeEl.appendChild(document.createTextNode((spec.name || "Spec " + specId) + " " + (spec.className || "") + " · "));
      scopeEl.appendChild(allLink);
    } else {
      scopeEl.textContent = fmt(scope.total_runs) + " runs";
    }

    renderBis(data);
    renderEnchant(scope);
    renderCraft(scope);
    renderSpecSwitcher(data, specId);
    // Set pieces are scope-independent; the server-rendered #item-set block is
    // left as-is (no JS rebuild needed).
    renderSpecPopularity(data, scoped);
    renderGems(scope);
    setupKeyLevelFilter(scope);
    renderKeyLevels(scope);
    renderDungeons(scope, []);
    renderVariants(scope);

    // If one of the top-row cards (spec popularity / gems) is hidden, let the
    // other span the full width instead of leaving an empty half.
    var specHidden = el("spec-popularity-col").classList.contains("d-none");
    var gemsHidden = el("gems-col").classList.contains("d-none");
    el("spec-popularity-col").classList.toggle("col-lg-12", gemsHidden);
    el("spec-popularity-col").classList.toggle("col-lg-6", !gemsHidden);
    el("gems-col").classList.toggle("col-lg-12", specHidden);
    el("gems-col").classList.toggle("col-lg-6", !specHidden);

    if (window.$WowheadPower && typeof window.$WowheadPower.refreshLinks === "function") {
      try { window.$WowheadPower.refreshLinks(); } catch (e) { /* tooltips optional */ }
    }
  }

  function renderSpecSwitcher(data, specId) {
    var wrap = el("spec-switcher"); clear(wrap);
    var card = el("spec-switch-card");
    var specs = data.global.specs || [];
    if (specs.length < 2) { if (card) card.classList.add("d-none"); return; }
    if (card) card.classList.remove("d-none");
    var all = scopeLink(null);
    all.className = "spec-card spec-card-all" + (specId ? "" : " active");
    var allMeta = document.createElement("div");
    allMeta.className = "spec-card-meta";
    var allName = document.createElement("div");
    allName.className = "spec-card-name"; allName.textContent = "All specs";
    allMeta.appendChild(allName); all.appendChild(allMeta);
    wrap.appendChild(all);
    specs.slice(0, 12).forEach(function (s) {
      var v = s.adoption != null ? s.adoption : s.share_pct;
      wrap.appendChild(specCard(s.spec_id, v + "%", String(s.spec_id) === String(specId)));
    });
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

  function renderSpecPopularity(data, scoped) {
    var col = el("spec-popularity-col");
    var box = el("spec-popularity"); clear(box);
    if (scoped) { col.classList.add("d-none"); return; }
    col.classList.remove("d-none");
    var specs = data.global.specs || [];
    if (!specs.length) {
      box.innerHTML = '<p class="text-sm opacity-6 mb-0">No spec usage recorded.</p>';
      return;
    }
    var top = specs.slice(0, 15);
    var maxAdopt = Math.max.apply(null, top.map(function (s) { return s.adoption || 0; })) || 1;
    top.forEach(function (s) {
      var spec = SPECS[String(s.spec_id)] || {};
      var img = document.createElement("img");
      img.className = "spec-icon"; img.src = specIcon(spec); img.alt = "";
      var link = scopeLink(s.spec_id);
      var pctText = s.adoption != null ? s.adoption + "% of runs" : fmt(s.runs) + " runs";
      link.appendChild(usageRow(
        img, (spec.name || "Spec " + s.spec_id) + " " + (spec.className || ""),
        (s.adoption || 0) / maxAdopt * 100, pctText));
      box.appendChild(link);
    });
  }

  function renderGems(scope) {
    var col = el("gems-col");
    var box = el("item-gems"); clear(box);
    var gems = scope.gems || [];
    if (!gems.length) {
      col.classList.add("d-none");   // hide the whole card for items with no sockets
      return;
    }
    col.classList.remove("d-none");
    gems.forEach(function (g) {
      var img = document.createElement("img");
      img.className = "gem-icon"; img.src = iconUrl(g.icon); img.alt = g.name;
      var a = document.createElement("a");
      a.href = "https://www.wowhead.com/item=" + g.id;
      a.target = "_blank"; a.rel = "noopener";
      a.appendChild(usageRow(img, g.name, g.pct, g.pct + "% · " + fmt(g.runs)));
      box.appendChild(a);
    });
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
    var html = (scope.keylevels || []).map(function (k) {
      return '<option value="' + k.level + '">+' + k.level + "</option>";
    }).join("");
    if ($s) {
      try { $s.selectpicker("destroy"); } catch (e) { /* not yet initialised */ }
      $s.html(html);
      $s.selectpicker();
      $s.off("changed.bs.select").on("changed.bs.select", function () { applySelection(); });
    } else {
      el("keylevel-filter").innerHTML = html;
    }
  }

  // Render the dungeon breakdown for the current selection and sync bar highlights.
  function applySelection() {
    var levels = selectedLevels();
    renderDungeons(curScope, levels);
    el("item-keylevels").querySelectorAll(".key-bar").forEach(function (b) {
      b.classList.toggle("selected", levels.indexOf(Number(b.getAttribute("data-level"))) >= 0);
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

  // Specs the item is best-in-slot (SimC) or a top players' pick for, one card each.
  function renderBis(data) {
    var box = el("item-bis"); clear(box);
    function group(items, badgeText, badgeCls, label, subFn) {
      if (!items || !items.length) return;
      var g = document.createElement("div");
      g.className = "bis-group mb-3";
      var head = document.createElement("div");
      head.className = "d-flex align-items-center gap-2 mb-2";
      var b = document.createElement("span");
      b.className = "badge " + badgeCls; b.textContent = badgeText;
      var lbl = document.createElement("span");
      lbl.className = "text-sm opacity-8"; lbl.textContent = label;
      head.appendChild(b); head.appendChild(lbl);
      var grid = document.createElement("div");
      grid.className = "spec-card-grid";
      items.slice(0, 12).forEach(function (s) {
        grid.appendChild(specCard(s.spec_id, subFn(s), false));
      });
      g.appendChild(head); g.appendChild(grid);
      box.appendChild(g);
    }
    group(data.simc_bis_specs, "SIM", "bg-info", "Best-in-slot (SimulationCraft) for",
      function (s) { return (SPECS[String(s.spec_id)] || {}).className || ""; });
    group(data.top_specs, "TOP", "bg-success", "Top players' pick for",
      function (s) {
        var cn = (SPECS[String(s.spec_id)] || {}).className;
        return (cn ? cn + " · " : "") + s.pct + "%";
      });
    var card = el("recommend-card");
    if (card) card.classList.toggle("d-none", !box.childNodes.length);
  }

  // Commonly-paired enchant for this slot, as a card.
  function renderEnchant(scope) {
    var box = el("item-enchant"); clear(box);
    var e = scope.enchant;
    if (e) {
      box.appendChild(craftCard(
        "Enchant", e.name, e.icon,
        e.spellId ? "https://www.wowhead.com/spell=" + e.spellId : "#",
        e.pct != null ? e.pct + "% of slots" : "", null));
    }
    toggleEnhance();
  }

  // Embellishment / missive on crafted items, as cards.
  function renderCraft(scope) {
    var box = el("item-craft"); clear(box);
    function line(label, c) {
      if (!c) return;
      box.appendChild(craftCard(
        label, c.name, c.icon, "https://www.wowhead.com/item=" + c.id,
        c.pct != null ? c.pct + "% of copies" : "", c.id));
    }
    line("Embellishment", scope.embellishment);
    line("Missive", scope.missive);
    toggleEnhance();
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
