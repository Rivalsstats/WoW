const PAGE_SIZE = 50;
let compRoutesLoaded = false;
let workerReady = false;
let worker = null;
let pendingBuild = false;
let lastResults = { total: 0, results: [] };
// Infinite scroll: how many matches are on the page, whether the in-flight
// worker reply is an append (vs a fresh search), the resolver waiting on that
// append, and the MythiInfinite controller for the sentinel.
let loadedCount = 0;
let pendingAppend = false;
let appendResolve = null;
let routeInfinite = null;

function debounce(fn, ms) {
  let t = null;
  return function (...args) {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

function safeId(str) {
  return str.replace(/[^A-Za-z0-9_-]/g, "_");
}

// Label a spec by its select option ("Restoration Druid"), falling back to the id.
function specLabel(id) {
  const opt = document.querySelector(`#specSelect option[value="${id}"]`);
  const text = opt ? opt.textContent.trim() : "";
  return text || String(id);
}

// A comp with all five specs often has no route recorded at all. Rather than a
// bare "no results", offer the largest spec subset the worker found matches for.
function renderNoRoutes(accordion) {
  const hint = lastResults.relaxHint;
  const chosen = paramsFromForm().specs.map(String);

  if (!hint || !hint.specs || !hint.specs.length) {
    accordion.innerHTML =
      '<p class="text-sm mb-0">No routes found for these filters.</p>';
    return;
  }

  const kept = hint.specs.map(String);
  const dropped = chosen.filter((s) => !kept.includes(s));
  const wrap = document.createElement("div");
  wrap.className = "alert alert-secondary text-sm";
  wrap.innerHTML = `
    <p class="mb-2">No route has been recorded with all ${chosen.length} of these specs.</p>
    <p class="mb-2">Searching without
      <strong>${dropped.map(specLabel).join(", ")}</strong>
      finds <strong>${hint.total}</strong> route${hint.total === 1 ? "" : "s"}.</p>
    <button type="button" class="btn btn-sm btn-primary mb-0" id="relaxSpecsBtn">
      Search with ${kept.length} spec${kept.length === 1 ? "" : "s"} instead
    </button>`;
  accordion.innerHTML = "";
  accordion.appendChild(wrap);

  wrap.querySelector("#relaxSpecsBtn").addEventListener("click", function () {
    $("#specSelect").selectpicker("val", kept);
    currentPage = 1;
    const params = paramsFromForm();
    params.page = 1;
    updateUrlFromParams(params, { replace: false });
    doQuery({ page: 1 });
  });
}

function renderMatches(routes, append = false) {
  const accordion = document.getElementById("routeDungeonAccordion");
  if (!append) accordion.innerHTML = "";
  if (!routes || routes.length === 0) {
    if (!append) renderNoRoutes(accordion);
    return;
  }

  routes.forEach((r) => {
    const slug = dungeons[r.dungeon]?.slug || r.dungeon;
    const runKey = safeId(`${slug}-${r.route_key}-${r.run_id}`);
    const dungeon = dungeons[r.dungeon];
    const englishName = dungeon?.name?.en_US || slug;
    const headerId = `heading-${runKey}`;
    const collapseId = `collapse-${runKey}`;

    const item = document.createElement("div");
    item.className = "accordion-item mb-2";

    const h2 = document.createElement("h2");
    h2.className = "accordion-header";
    h2.id = headerId;

    const btn = document.createElement("button");
    btn.className = "accordion-button collapsed p-0";
    btn.type = "button";
    btn.setAttribute("data-bs-toggle", "collapse");
    btn.setAttribute("data-bs-target", `#${collapseId}`);
    btn.setAttribute("aria-expanded", "false");
    btn.setAttribute("aria-controls", collapseId);

    const bgIcon = dungeon && dungeon.icon ? dungeon.icon : slug + ".jpg";
    btn.style.backgroundImage = `url('/data/icons/${bgIcon}')`;
    btn.style.backgroundSize = "cover";
    btn.style.backgroundPosition = "center";
    btn.style.backgroundRepeat = "no-repeat";
    btn.style.backgroundBlendMode = "overlay";

    const inner = document.createElement("div");
    inner.className = "w-100 row w-100 gx-2 align-items-center py-3 px-4";

    const leftCol = document.createElement("div");
    leftCol.className = "col-12 col-sm-4 text-start";
    leftCol.innerHTML = `<span class="badge bg-secondary text-white rounded px-2 mx-1">${englishName}</span>
                         <span class="badge bg-success text-dark rounded px-2 mx-1">+${r.level}</span>`;

    const centerCol = document.createElement("div");
    centerCol.className = "col-12 col-sm-4 text-center";
    const durationspan = document.createElement("span");
    durationspan.className =
      "badge bg-secondary text-white rounded px-2 mx-2";
    durationspan.textContent = formatDuration(r.duration);
    const timestampspan = document.createElement("span");
    timestampspan.className =
      "timestamp badge bg-secondary text-white rounded px-2 mx-2";
    timestampspan.setAttribute(
      "title",
      new Date(Number(r.timestamp) * 1000).toLocaleString()
    );
    timestampspan.setAttribute("data-bs-toggle", "tooltip");
    timestampspan.setAttribute("data-bs-placement", "top");
    timestampspan.textContent = timeAgo(Number(r.timestamp));
    centerCol.appendChild(durationspan);
    centerCol.appendChild(timestampspan);

    const rightCol = document.createElement("div");
    rightCol.className = "col-12 col-sm-4 text-end";
    const iconBar = document.createElement("div");
    iconBar.className = "d-inline-flex align-items-center";

    ["0", "1", "2"].forEach((role) => {
      (r.specs || []).forEach((sid) => {
        const spec = spec_data[sid];
        if (spec && String(spec.role) === role) {
          const img = document.createElement("img");
          img.src = `/data/icons/${spec.SpellIconFileId}.jpg`;
          img.alt = spec.name || "";
          img.title = spec.name || "";
          img.className = "me-1 img-fluid";
          img.style.width = "24px";
          img.style.height = "24px";
          img.style.objectFit = "cover";
          iconBar.appendChild(img);
        }
      });
    });

    rightCol.appendChild(iconBar);

    inner.appendChild(leftCol);
    inner.appendChild(centerCol);
    inner.appendChild(rightCol);
    btn.appendChild(inner);
    h2.appendChild(btn);
    item.appendChild(h2);

    const collapseDiv = document.createElement("div");
    collapseDiv.id = collapseId;
    // Permalink handle without the run id, which changes on every data refresh
    // (see assets/js/deep-link.js). The search filters already live in the query
    // string, so #route-<slug>-<routeKey> plus those params is a full link.
    collapseDiv.setAttribute("data-share-id", safeId(`route-${slug}-${r.route_key}`));
    collapseDiv.className = "accordion-collapse collapse";
    collapseDiv.setAttribute("aria-labelledby", headerId);
    collapseDiv.setAttribute("data-bs-parent", "#routeDungeonAccordion");

    const body = document.createElement("div");
    body.className = "accordion-body";
    body.innerHTML = `<p>
      <a href="https://raider.io/mythic-plus-runs/${current_season}/${
      r.run_id
    }" target="_blank">
        <img src="/assets/img/logos/RaiderIOLogo.png" alt="RaiderIO run link" title="RaiderIO run link" class="me-1" height="24" width="24">
        View on RaiderIO
      </a>
    </p>
    <div class="iframe-container position-relative">
      <div class="iframe-spinner position-absolute top-50 start-50 translate-middle d-none">
        <div class="spinner-border text-primary" role="status"><span class="visually-hidden">Loading...</span></div>
      </div>
      <iframe loading="lazy" data-name="keystoneGuru"
        data-src="https://keystone.guru/route/${slug}/${
      r.route_key
    }/${slug}/embed"
        class="w-100"
        style="border: none; width:100%; height: calc(80vh - 3rem); display:block;"></iframe>
    </div>
    <div class="mt-2"><small class="text-muted">NPCs: ${
      (r.npcs && r.npcs.length) || 0
    } â€” Spells: ${(r.spells && r.spells.length) || 0}</small></div>`;

    collapseDiv.appendChild(body);
    item.appendChild(collapseDiv);
    accordion.appendChild(item);

    collapseDiv.addEventListener("shown.bs.collapse", function () {
      const iframe = collapseDiv.querySelector("iframe[data-src]");
      if (!iframe) return;
      // Gated on Klaro consent for keystoneGuru. These result panels are built
      // client-side, so Klaro never sees the iframes and cannot hold them back
      // itself â€” setting src here unconditionally loaded the embed even when the
      // visitor had declined. MythiConsent defers until consent is granted, never
      // loads it if it isn't, and owns the spinner either way.
      MythiConsent.loadEmbed(iframe);
    });
  });

  // Results are rebuilt from scratch on every query, so the injected copy-link
  // buttons have to be re-attached to the new panels.
  if (window.MythiLink) window.MythiLink.refresh();
}

function initSearch() {
  if (worker) return;
  worker = new Worker("/assets/js/comp-routes-worker.js");
  worker.onmessage = function (ev) {
    const msg = ev.data;
    if (!msg || !msg.cmd) return;
    if (msg.cmd === "built") {
      workerReady = true;
    } else if (msg.cmd === "result") {
      lastResults.total = msg.total;
      lastResults.relaxHint = msg.relaxHint || null;

      const isAppend = pendingAppend;
      pendingAppend = false;

      if (isAppend) {
        // Next page of the same search: keep what's on screen and add to it.
        currentPage = msg.page;
        loadedCount += msg.results.length;
        renderMatches(msg.results, true);
      } else {
        // Fresh search: replace the list and start counting again.
        currentPage = 1;
        loadedCount = msg.results.length;
        lastResults.results = msg.results;
        renderMatches(msg.results, false);
      }

      updateRouteSummary(loadedCount, msg.total);

      const done = loadedCount >= msg.total;
      if (appendResolve) {
        const resolve = appendResolve;
        appendResolve = null;
        resolve(done);
      }
      // A fresh search re-arms the sentinel (or retires it when the first page
      // already held everything); appends let their onLoadMore promise settle it.
      if (!isAppend && routeInfinite) {
        if (done) routeInfinite.finish();
        else routeInfinite.reset();
      }
    } else if (msg.cmd === "error") {
      console.error("Worker error:", msg.payload);
    }
  };

  fetch("/assets/json/compRoutes.json")
    .then((r) => {
      if (!r.ok) throw new Error("Failed to load compRoutes.json: " + r.status);
      return r.json();
    })
    .then((json) => {
      pendingBuild = true;
      worker.postMessage({ cmd: "build", payload: json });
    })
    .catch((err) => {
      console.error("Failed to load route data:", err);
    });
}

let currentPage = 1;
function doQuery({ page = 1, pageSize = PAGE_SIZE, append = false } = {}) {
  if (!worker) {
    initSearch();
  }
  // The worker reply is matched to this flag so the result handler knows whether
  // to append the page or replace the list. currentPage is advanced there.
  pendingAppend = append;
  const chosenDungeon = $("#dungeonSelect").selectpicker("val") || [];
  const chosenSpecs = $("#specSelect").selectpicker("val") || [];
  const spellsSelected = $("#spellSelect").selectpicker("val") || [];
  const spellsWanted = spellsSelected
    .map((s) => Number(s))
    .filter((n) => !Number.isNaN(n));

  const npcIncludeSelected = $("#npcIncludeSelect").selectpicker("val") || [];
  const npcInclude = npcIncludeSelected
    .map((s) => Number(s))
    .filter((n) => !Number.isNaN(n));

  const npcExcludeSelected = $("#npcExcludeSelect").selectpicker("val") || [];
  const npcExclude = npcExcludeSelected
    .map((s) => Number(s))
    .filter((n) => !Number.isNaN(n));
  // Tank -> healer -> DPS, so relaxing an over-specific comp filter drops DPS first.
  const roleOf = (id) => Number(((window.spec_data || {})[id] || {}).role ?? 2);
  const specsPriority = chosenSpecs.slice().sort((a, b) => roleOf(a) - roleOf(b));

  worker.postMessage({
    cmd: "query",
    payload: {
      dungeons: chosenDungeon,
      specs: chosenSpecs,
      specsPriority: specsPriority,
      spells: spellsWanted,
      npcInclude: npcInclude,
      npcExclude: npcExclude,
      page,
      pageSize,
    },
  });
}

function parseIdList(s) {
  if (!s) return [];
  return s
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x !== "")
    .map((n) => Number(n))
    .filter((n) => !Number.isNaN(n));
}

document.getElementById("compForm").addEventListener("submit", function (e) {
  e.preventDefault();
  const params = paramsFromForm();
  currentPage = 1;
  updateUrlFromParams(params, { replace: false });
  showOverlayUntilAccordionMutates(10000);
  doQuery({ page: 1 });
});

function parseUrlParams() {
  const sp = new URLSearchParams(window.location.search);
  function getList(key) {
    if (!sp.has(key)) return [];
    const v = sp.get(key) || "";
    if (!v) return [];
    return v
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s !== "");
  }
  return {
    dungeons: getList("dungeons"),
    specs: getList("specs"),
    spells: getList("spells"),
    npcInclude: getList("npcInclude"),
    npcExclude: getList("npcExclude"),
  };
}

function paramsFromForm() {
  const chosenDungeon = $("#dungeonSelect").selectpicker
    ? $("#dungeonSelect").selectpicker("val") || []
    : $("#dungeonSelect").val() || [];
  const chosenSpecs = $("#specSelect").selectpicker
    ? $("#specSelect").selectpicker("val") || []
    : $("#specSelect").val() || [];
  const spells = $("#spellSelect").selectpicker
    ? $("#spellSelect").selectpicker("val") || []
    : $("#spellSelect").val() || [];
  const npcInclude = $("#npcIncludeSelect").selectpicker
    ? $("#npcIncludeSelect").selectpicker("val") || []
    : $("#npcIncludeSelect").val() || [];
  const npcExclude = $("#npcExcludeSelect").selectpicker
    ? $("#npcExcludeSelect").selectpicker("val") || []
    : $("#npcExcludeSelect").val() || [];

  return {
    dungeons: Array.isArray(chosenDungeon)
      ? chosenDungeon
      : chosenDungeon
      ? [chosenDungeon]
      : [],
    specs: Array.isArray(chosenSpecs)
      ? chosenSpecs
      : chosenSpecs
      ? [chosenSpecs]
      : [],
    spells: Array.isArray(spells) ? spells : spells ? [spells] : [],
    npcInclude: Array.isArray(npcInclude)
      ? npcInclude
      : npcInclude
      ? [npcInclude]
      : [],
    npcExclude: Array.isArray(npcExclude)
      ? npcExclude
      : npcExclude
      ? [npcExclude]
      : [],
  };
}

function hasAnyParams(obj) {
  return (
    (obj.dungeons && obj.dungeons.length) ||
    (obj.specs && obj.specs.length) ||
    (obj.spells && obj.spells.length) ||
    (obj.npcInclude && obj.npcInclude.length) ||
    (obj.npcExclude && obj.npcExclude.length)
  );
}

function updateUrlFromParams(params, { replace = true } = {}) {
  const sp = new URLSearchParams();
  if (params.dungeons && params.dungeons.length)
    sp.set("dungeons", params.dungeons.join(","));
  if (params.specs && params.specs.length)
    sp.set("specs", params.specs.join(","));
  if (params.spells && params.spells.length)
    sp.set("spells", params.spells.join(","));
  if (params.npcInclude && params.npcInclude.length)
    sp.set("npcInclude", params.npcInclude.join(","));
  if (params.npcExclude && params.npcExclude.length)
    sp.set("npcExclude", params.npcExclude.join(","));

  const newUrl =
    window.location.pathname + (sp.toString() ? "?" + sp.toString() : "");
  if (replace) {
    history.replaceState(params, "", newUrl);
  } else {
    history.pushState(params, "", newUrl);
  }
}

function applyParamsToForm(params) {
  if (params.dungeons && params.dungeons.length)
    $("#dungeonSelect").selectpicker("val", params.dungeons);
  if (params.specs && params.specs.length)
    $("#specSelect").selectpicker("val", params.specs);
  if (params.spells && params.spells.length)
    $("#spellSelect").selectpicker("val", params.spells);
  if (params.npcInclude && params.npcInclude.length)
    $("#npcIncludeSelect").selectpicker("val", params.npcInclude);
  if (params.npcExclude && params.npcExclude.length)
    $("#npcExcludeSelect").selectpicker("val", params.npcExclude);
}

document.addEventListener("DOMContentLoaded", function () {
  function replaceUrlFromForm() {
    const p = paramsFromForm();
    updateUrlFromParams(p, { replace: true });
  }

  const initialParams = parseUrlParams();

  if (hasAnyParams(initialParams)) {
    showOverlayUntilAccordionMutates(10000);
    applyParamsToForm(initialParams);

    currentPage = 1;

    if (!worker) initSearch();
    const waitForWorker = setInterval(() => {
      if (workerReady) {
        clearInterval(waitForWorker);
        doQuery({ page: currentPage, pageSize: PAGE_SIZE });
      }
    }, 150);
    setTimeout(() => clearInterval(waitForWorker), 10000);
  } else {
    initSearch();
    updateUrlFromParams(
      {
        dungeons: [],
        specs: [],
        spells: [],
        npcInclude: [],
        npcExclude: [],
      },
      { replace: true }
    );
  }

  const onSelectChange = debounce(() => replaceUrlFromForm(), 220);
  $(
    "#dungeonSelect, #specSelect, #spellSelect, #npcIncludeSelect, #npcExcludeSelect"
  ).on("changed.bs.select change", onSelectChange);

  window.addEventListener("popstate", function (ev) {
    const state = ev.state || parseUrlParams();
    if (!state) return;
    applyParamsToForm(state);
    currentPage = 1;
    if (hasAnyParams(state)) {
      showOverlayUntilAccordionMutates(10000);
      if (!worker) initSearch();
      const waitForWorker = setInterval(() => {
        if (workerReady) {
          clearInterval(waitForWorker);
          doQuery({ page: currentPage, pageSize: PAGE_SIZE });
        }
      }, 150);
      setTimeout(() => clearInterval(waitForWorker), 10000);
    }
  });
});

function showLoadingOverlay() {
  const el = document.getElementById("route-search-overlay");
  if (!el) return;
  el.style.display = "flex";
  el.setAttribute("aria-hidden", "false");
}

function hideLoadingOverlay() {
  const el = document.getElementById("route-search-overlay");
  if (!el) return;
  el.style.display = "none";
  el.setAttribute("aria-hidden", "true");
}

function showOverlayUntilAccordionMutates(timeoutMs = 10000) {
  showLoadingOverlay();

  const accordion = document.getElementById("routeDungeonAccordion");
  if (!accordion) {
    setTimeout(hideLoadingOverlay, 200);
    return;
  }

  const mo = new MutationObserver((mutations, observer) => {
    if (mutations && mutations.length) {
      try {
        observer.disconnect();
      } catch (e) {}
      hideLoadingOverlay();
    }
  });
  mo.observe(accordion, { childList: true, subtree: true });

  const to = setTimeout(() => {
    try {
      mo.disconnect();
    } catch (e) {}
    hideLoadingOverlay();
  }, timeoutMs);

  const cleanupObserver = new MutationObserver(() => {
    const overlay = document.getElementById("route-search-overlay");
    if (!overlay) return;
    if (
      overlay.style.display === "none" ||
      overlay.getAttribute("aria-hidden") === "true"
    ) {
      clearTimeout(to);
      try {
        mo.disconnect();
      } catch (e) {}
      cleanupObserver.disconnect();
    }
  });

  const overlayEl = document.getElementById("route-search-overlay");
  if (overlayEl) {
    cleanupObserver.observe(overlayEl, {
      attributes: true,
      attributeFilter: ["style", "aria-hidden"],
    });
  } else {
    cleanupObserver.disconnect();
  }
}

// Loaded/total readout above the results, replacing the old numbered pager's
// "Page X of Y" line.
function updateRouteSummary(loaded, total) {
  const summary = document.querySelector(".pagination-summary");
  if (!summary) return;
  if (!total || total <= 0) {
    summary.style.display = "none";
    return;
  }
  summary.style.display = "inline-block";
  summary.textContent = `Showing ${loaded} of ${total} route${
    total === 1 ? "" : "s"
  }`;
}

// Sentinel callback: ask the worker for the page after the last one loaded and
// append it. Resolves true (list finished) once everything matched is on screen.
function loadMoreRoutes() {
  if (loadedCount >= lastResults.total) return true;
  return new Promise((resolve) => {
    appendResolve = resolve;
    doQuery({ page: currentPage + 1, append: true });
  });
}

(function () {
  const sentinel = document.getElementById("route-sentinel");
  if (!sentinel || !window.MythiInfinite) return;
  // Armed immediately; until a search populates lastResults.total it just no-ops
  // (loadMoreRoutes returns true), and each fresh search re-arms it.
  routeInfinite = window.MythiInfinite.create({
    sentinel: sentinel,
    onLoadMore: loadMoreRoutes,
  });
})();
