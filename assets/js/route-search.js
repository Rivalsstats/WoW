// routesSearch.js
// Assumes you have bootstrap, jQuery, selectpicker present (same as before)

// config
const PAGE_SIZE = 50;
let compRoutesLoaded = false;
let workerReady = false;
let worker = null;
let pendingBuild = false;
let lastResults = { total: 0, results: [] };

// small helper debounce
function debounce(fn, ms) {
  let t = null;
  return function (...args) {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

// renderMatches: reuses your previous DOM building code but expects an array of route objects
function renderMatches(routes, append = false) {
  const accordion = document.getElementById("routeDungeonAccordion");
  if (!append) accordion.innerHTML = "";
  if (!routes || routes.length === 0) {
    if (!append) accordion.innerHTML = "<p>No routes found.</p>";
    return;
  }

  // helper safeId
  function safeId(str) {
    return str.replace(/[^A-Za-z0-9_-]/g, "_");
  }

  // create DOM for each route (re-using your earlier logic but lighter)
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
    leftCol.innerHTML = `<span class="badge bg-dark bg-opacity-50 text-white rounded px-2 mx-1">${englishName}</span>
                         <span class="badge bg-dark bg-opacity-50 text-success rounded px-2 mx-1">+${r.level}</span>`;

    const centerCol = document.createElement("div");
    centerCol.className = "col-12 col-sm-4 text-center";
    const durationspan = document.createElement("span");
    durationspan.className =
      "badge bg-dark bg-opacity-50 text-white rounded px-2 mx-2";
    durationspan.textContent = formatDuration(r.duration);
    const timestampspan = document.createElement("span");
    timestampspan.className =
      "timestamp badge bg-dark bg-opacity-50 text-white rounded px-2 mx-2";
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
      <div class="iframe-spinner position-absolute top-50 start-50 translate-middle">
        <div class="spinner-border text-primary" role="status"><span class="visually-hidden">Loading...</span></div>
      </div>
      <iframe src="" loading="lazy" data-name="keystoneGuru"
        data-src="https://keystone.guru/route/${slug}/${
      r.route_key
    }/${slug}/embed"
        class="w-100"
        style="border: none; width:100%; height: calc(80vh - 3rem); display:block;"></iframe>
    </div>
    <div class="mt-2"><small class="text-muted">NPCs: ${
      (r.npcs && r.npcs.length) || 0
    } — Spells: ${(r.spells && r.spells.length) || 0}</small></div>`;

    collapseDiv.appendChild(body);
    item.appendChild(collapseDiv);
    accordion.appendChild(item);

    collapseDiv.addEventListener("shown.bs.collapse", function () {
      const iframe = collapseDiv.querySelector("iframe[data-src]");
      if (!iframe) return;
      if (
        !iframe.src ||
        iframe.src === "" ||
        iframe.src !== iframe.dataset.src
      ) {
        iframe.src = iframe.dataset.src;
        iframe.addEventListener(
          "load",
          () => {
            const spinner = collapseDiv.querySelector(".iframe-spinner");
            if (spinner) spinner.classList.add("d-none");
          },
          { once: true }
        );
      }
    });
  });
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
      lastResults.results = msg.results;
      renderMatches(msg.results, false);

      if (typeof window.__routeRenderPagination === "function") {
        window.__routeRenderPagination(
          msg.total,
          msg.page || currentPage,
          msg.pageSize || PAGE_SIZE
        );
      }
    } else if (msg.cmd === "error") {
      console.error("Worker error:", msg.payload);
    }
  };

  fetch("/data/static/compRoutes.json")
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
function doQuery({ page = 1, pageSize = PAGE_SIZE } = {}) {
  if (!worker) {
    initSearch();
  }
  currentPage = page;
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
  worker.postMessage({
    cmd: "query",
    payload: {
      dungeons: chosenDungeon,
      specs: chosenSpecs,
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
  params.page = 1;
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
    page: sp.has("page") ? Number(sp.get("page")) || 1 : 1,
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
    page: currentPage || 1,
  };
}

function hasAnyParams(obj) {
  return (
    (obj.dungeons && obj.dungeons.length) ||
    (obj.specs && obj.specs.length) ||
    (obj.spells && obj.spells.length) ||
    (obj.npcInclude && obj.npcInclude.length) ||
    (obj.npcExclude && obj.npcExclude.length) ||
    (obj.page && obj.page > 1)
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
  if (params.page && params.page > 1) sp.set("page", String(params.page));

  const newUrl =
    window.location.pathname + (sp.toString() ? "?" + sp.toString() : "");
  if (replace) {
    history.replaceState(params, "", newUrl);
  } else {
    history.pushState(params, "", newUrl);
  }
}

function applyParamsToForm(params) {
  // only set values if there are values present
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
  // Expose a small convenience to update URL from the current form (replace state)
  function replaceUrlFromForm() {
    const p = paramsFromForm();
    updateUrlFromParams(p, { replace: true });
  }

  // parse incoming url and decide whether to run query
  const initialParams = parseUrlParams();

  // If there are any params present, re-apply them to the form and trigger the query
  // If no params, leave the server-rendered content as-is.
  if (hasAnyParams(initialParams)) {
    showOverlayUntilAccordionMutates(10000);
    // apply UI values first (so selects reflect state)
    applyParamsToForm(initialParams);

    // set currentPage from params
    currentPage = initialParams.page || 1;

    // ensure the index exists and then query once ready
    if (!worker) initSearch();
    // Wait until worker is ready before querying; the worker will update UI via postMessage.
    const waitForWorker = setInterval(() => {
      if (workerReady) {
        clearInterval(waitForWorker);
        doQuery({ page: currentPage, pageSize: PAGE_SIZE });
      }
    }, 150);
    // also give a timeout fallback to avoid infinite wait
    setTimeout(() => clearInterval(waitForWorker), 10000);
  } else {
    // still initialize the worker in background so subsequent searches are snappy
    initSearch();
    // ensure the current URL is stored as baseline replaceState (so popstate has state object)
    updateUrlFromParams(
      {
        dungeons: [],
        specs: [],
        spells: [],
        npcInclude: [],
        npcExclude: [],
        page: 1,
      },
      { replace: true }
    );
  }

  const onSelectChange = debounce(() => replaceUrlFromForm(), 220);
  $(
    "#dungeonSelect, #specSelect, #spellSelect, #npcIncludeSelect, #npcExcludeSelect"
  ).on("changed.bs.select change", onSelectChange);

  // handle back/forward navigation
  window.addEventListener("popstate", function (ev) {
    const state = ev.state || parseUrlParams();
    if (!state) return;
    applyParamsToForm(state);
    currentPage = state.page || 1;
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



// ---------- overlay helpers ----------
function showLoadingOverlay() {
  const el = document.getElementById('route-search-overlay');
  if (!el) return; // overlay not present in DOM -> no-op
  el.style.display = 'flex';
  el.setAttribute('aria-hidden', 'false');
}

function hideLoadingOverlay() {
  const el = document.getElementById('route-search-overlay');
  if (!el) return; // overlay not present -> no-op
  el.style.display = 'none';
  el.setAttribute('aria-hidden', 'true');
}

// Show overlay and hide it when accordion content changes (or when timeout hits)
function showOverlayUntilAccordionMutates(timeoutMs = 10000) {
  // show the (HTML-provided) overlay if present
  showLoadingOverlay();

  const accordion = document.getElementById('routeDungeonAccordion');
  if (!accordion) {
    // nothing to observe -> hide shortly to avoid stuck overlay
    setTimeout(hideLoadingOverlay, 200);
    return;
  }

  // hide overlay when accordion DOM changes (childList/subtree)
  const mo = new MutationObserver((mutations, observer) => {
    if (mutations && mutations.length) {
      try { observer.disconnect(); } catch (e) {}
      hideLoadingOverlay();
    }
  });
  mo.observe(accordion, { childList: true, subtree: true });

  // fallback: hide after timeout and disconnect observer
  const to = setTimeout(() => {
    try { mo.disconnect(); } catch (e) {}
    hideLoadingOverlay();
  }, timeoutMs);

  // cleanup observer: watch overlay attributes and cancel fallback timer if overlay is hidden
  const cleanupObserver = new MutationObserver(() => {
    const overlay = document.getElementById('route-search-overlay');
    if (!overlay) return;
    if (overlay.style.display === 'none' || overlay.getAttribute('aria-hidden') === 'true') {
      clearTimeout(to);
      try { mo.disconnect(); } catch (e) {}
      cleanupObserver.disconnect();
    }
  });

  const overlayEl = document.getElementById('route-search-overlay');
  if (overlayEl) {
    cleanupObserver.observe(overlayEl, { attributes: true, attributeFilter: ['style', 'aria-hidden'] });
  } else {
    // overlay missing -> rely on fallback timeout only
    cleanupObserver.disconnect();
  }
}

(function () {

// Render the pagination based on metadata
function renderPagination(total, page = 1, pageSize = PAGE_SIZE) {
  const root = document.getElementById("route-pagination");
  const list = root.querySelector('.pagination');
  const summary = document.querySelector('.pagination-summary');

  // validate inputs
  if (!list || typeof total !== 'number' || total <= 0) {
    if (root) root.style.display = 'none';
    return;
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  page = Math.max(1, Math.min(page, totalPages));
  list.innerHTML = '';

  // helper to create page item
  function mkItem(labelHtml, cls = '', disabled = false, ariaLabel = null, pageNum = null) {
    const li = document.createElement('li');
    li.className = `page-item ${cls} ${disabled ? 'disabled' : ''}`.trim();
    const a = document.createElement('a');
    a.className = 'page-link';
    a.href = 'javascript:void(0);';
    a.setAttribute('role', 'button');
    if (ariaLabel) a.setAttribute('aria-label', ariaLabel);
    a.innerHTML = labelHtml;
    if (!disabled && typeof pageNum === 'number') {
      a.addEventListener('click', (ev) => {
        ev.preventDefault();
        goToPage(pageNum, { push: true });
      });
    }
    li.appendChild(a);
    return li;
  }

  // small windowing function
  function pageWindow(current, totalP, maxButtons = 7) {
    if (totalP <= maxButtons) return Array.from({ length: totalP }, (_, i) => i + 1);
    const half = Math.floor(maxButtons / 2);
    let start = Math.max(1, current - half);
    let end = Math.min(totalP, current + half);
    if (current - start < half) end = Math.min(totalP, start + maxButtons - 1);
    if (end - current < half) start = Math.max(1, end - maxButtons + 1);
    return Array.from({ length: end - start + 1 }, (_, i) => start + i);
  }

  // Use material icon HTML for prev/next
  const prevHtml = `<i class="material-symbols-rounded" aria-hidden="true" style="vertical-align:middle">arrow_back_ios</i>`;
  const nextHtml = `<i class="material-symbols-rounded" aria-hidden="true" style="vertical-align:middle">arrow_forward_ios</i>`;

  // Prev
  list.appendChild(mkItem(prevHtml, '', page <= 1, 'Previous page', Math.max(1, page - 1)));

  // page numbers window
  const pages = pageWindow(page, totalPages, 7);
  if (pages[0] > 1) {
    list.appendChild(mkItem('1', '', false, 'Page 1', 1));
    if (pages[0] > 2) {
      const ell = document.createElement('li');
      ell.className = 'page-item disabled';
      ell.innerHTML = `<span class="page-link">…</span>`;
      list.appendChild(ell);
    }
  }

  pages.forEach((pn) => {
    const activeClass = pn === page ? 'active' : '';
    const li = mkItem(String(pn), activeClass, false, `Page ${pn}`, pn);
    if (pn === page) li.classList.add('active');
    list.appendChild(li);
  });

  if (pages[pages.length - 1] < totalPages) {
    if (pages[pages.length - 1] < totalPages - 1) {
      const ell = document.createElement('li');
      ell.className = 'page-item disabled';
      ell.innerHTML = `<span class="page-link">…</span>`;
      list.appendChild(ell);
    }
    list.appendChild(mkItem(String(totalPages), '', false, `Page ${totalPages}`, totalPages));
  }

  // Next
  list.appendChild(mkItem(nextHtml, '', page >= totalPages, 'Next page', Math.min(totalPages, page + 1)));

  // show summary
  if (summary) {
    summary.style.display = 'inline-block';
    summary.textContent = `Page ${page} of ${totalPages} (${total} Routes)`;
  }

  // expose data attributes & show root
  root.dataset.totalPages = String(totalPages);
  root.dataset.currentPage = String(page);
  root.style.display = 'flex';
}

  
// navigate programmatically 
function goToPage(n, { push = false } = {}) {
  n = Math.max(1, Math.floor(n || 1));
  currentPage = n;

  // update URL params from current form + new page
  const params = paramsFromForm();
  params.page = currentPage;
  updateUrlFromParams(params, { replace: !push });

  // scroll to top of the accordion so users see new content
  try {
    const target =
      document.getElementById("routeDungeonAccordion") ||
      document.querySelector("main") ||
      document.body;

    let offset = 20;
    const navbar =
      document.querySelector(".navbar") ||
      document.querySelector(".navbar-expand") ||
      document.querySelector(".main-nav");
    if (navbar) {
      try {
        const nbRect = navbar.getBoundingClientRect();
        if (nbRect && nbRect.height) offset = Math.round(nbRect.height) + 12;
      } catch (e) {
        /* ignore */
      }
    }

    const rect = target.getBoundingClientRect();
    const top = Math.max(0, rect.top + window.scrollY - offset);

    window.scrollTo({ top, behavior: "smooth" });

    try {
      target.setAttribute("tabindex", "-1");
      target.focus({ preventScroll: true });
    } catch (e) {}
  } catch (e) {
    console.warn("scroll-to-results failed", e);
  }

  // show overlay and request query
  showOverlayUntilAccordionMutates(10000);
  if (!worker) initSearch();

  const waitForWorker = setInterval(() => {
    if (workerReady) {
      clearInterval(waitForWorker);
      doQuery({ page: currentPage, pageSize: PAGE_SIZE });
    }
  }, 100);
  setTimeout(() => clearInterval(waitForWorker), 10000);
}


  // Listen for worker results so we can render pagination when metadata arrives
  function attachWorkerPaginationListener() {
    if (!window.worker) return;
    if (window.__pager_attached) return;
    window.__pager_attached = true;
    window.worker.addEventListener("message", (ev) => {
      try {
        const data = ev.data;
        if (!data) return;
        if (
          data.type === "results" ||
          data.type === "search-results" ||
          data.type === "routes-results"
        ) {
          const total =
            typeof data.total === "number"
              ? data.total
              : data.meta && data.meta.total
              ? data.meta.total
              : null;
          const page =
            typeof data.page === "number"
              ? data.page
              : data.meta && data.meta.page
              ? data.meta.page
              : currentPage || 1;
          if (typeof total === "number")
            renderPagination(total, page, PAGE_SIZE);
        } else if (data.meta && typeof data.meta.total === "number") {
          renderPagination(
            data.meta.total,
            data.meta.page || currentPage,
            PAGE_SIZE
          );
        }
      } catch (e) {
        console.warn("pager worker message error", e);
      }
    });
  }

  // If worker already ready, attach now; else attach once it's ready via existing workerReady checks
  if (window.worker && window.workerReady) attachWorkerPaginationListener();
  else {
    // fallback: poll for worker creation
    const poll = setInterval(() => {
      if (window.worker) {
        attachWorkerPaginationListener();
        clearInterval(poll);
      }
    }, 150);
    setTimeout(() => clearInterval(poll), 10000);
  }

  window.__routeRenderPagination = renderPagination;
})();