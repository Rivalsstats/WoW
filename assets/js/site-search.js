(function () {
  "use strict";

  const input = document.getElementById("site-search-input");
  const resultsMenu = document.getElementById("site-search-results");
  const resultsInner = document.getElementById("site-search-results-inner");
  const noResults = document.getElementById("site-search-noresults");
  const clearBtn = document.getElementById("site-search-clear");
  const allResultsLink = document.getElementById("site-search-all-results");

  if (!input || !resultsMenu || !resultsInner) {
    console.warn("[site-search] required elements not found — aborting init");
    return;
  }

  // default closed
  resultsMenu.setAttribute("data-open", "false");
  resultsMenu.classList.remove("show");
  resultsMenu.style.display = "none";

  // page root where we search / highlight
  const PAGE_ROOT =
    document.querySelector("main .container-fluid") ||
    document.querySelector("main") ||
    document.querySelector("article") ||
    document.querySelector(".ms-404-content") ||
    document.body;

  let fuse = null;
  let indexLoaded = false;
  let indexData = [];

  const DEBOUNCE_MS = 180;
  let debounceTimer = null;
  let focusedIndex = -1;
  const MAX_RESULTS = 8;
  const MIN_QUERY_LENGTH = 3;

  // ----------------- Load Fuse index (if present) -----------------
  async function loadIndex() {
    try {
      const res = await fetch("/assets/json/search_index.json", {
        cache: "no-store",
      });
      if (!res.ok) {
        console.warn("[site-search] index fetch status", res.status);
        return;
      }
      indexData = await res.json();
      const options = {
        includeMatches: true,
        threshold: 0.35,
        keys: [
          { name: "title", weight: 0.6 },
          { name: "tags", weight: 0.3 },
          { name: "content", weight: 0.1 },
        ],
      };
      fuse = new Fuse(indexData, options);
      indexLoaded = true;
      console.debug("[site-search] index loaded items=", indexData.length);
    } catch (err) {
      console.warn("[site-search] index load failed", err);
      indexLoaded = false;
    }
  }
  loadIndex();

  // ----------------- visibility helpers -----------------
  function showResultsMenu() {
    resultsMenu.setAttribute("data-open", "true");
    resultsMenu.classList.add("show"); // useful if any bootstrap styles rely on it
    resultsMenu.style.display = "block";
    if (typeof window.__siteSearchPositionResults === "function")
      window.__siteSearchPositionResults();
  }
  function hideResultsMenu() {
    resultsMenu.setAttribute("data-open", "false");
    resultsMenu.classList.remove("show");
    resultsMenu.style.display = "none";
  }

  // --------------- dropdown positioning (position-only) ----------------
  (function setupResultsPositioningAnchorRight() {
    if (!resultsMenu || !input) return;

    // ensure panel is appended to body so it's not clipped
    if (resultsMenu.parentElement !== document.body)
      document.body.appendChild(resultsMenu);

    const DROPDOWN_MAX_W = 520; // px
    const PAGE_MARGIN = 12; // px margin from screen edges and sidebars

    function positionResultsMenuAnchorRight() {
      const rect = input.getBoundingClientRect();

      // compute right-side limit (avoid right sidebar if present)
      const rightSidebar = document.getElementById("sidenav-right");
      let rightLimit = window.innerWidth - PAGE_MARGIN;
      if (rightSidebar) {
        const rs = rightSidebar.getBoundingClientRect();
        rightLimit = Math.min(
          rightLimit,
          Math.max(PAGE_MARGIN, rs.left - PAGE_MARGIN)
        );
      }

      // available width to the left of input.right
      const maxAvailable = rect.right - PAGE_MARGIN;
      const availBeforeSidebar = Math.min(
        maxAvailable,
        rightLimit - PAGE_MARGIN
      );

      // pick width (bounded)
      const width = Math.min(DROPDOWN_MAX_W, Math.max(160, availBeforeSidebar));

      // right-align: left = input.right - width
      let left = rect.right - width;
      if (left < PAGE_MARGIN) left = PAGE_MARGIN;
      if (left + width > rightLimit)
        left = Math.max(PAGE_MARGIN, rightLimit - width);

      const top = rect.bottom + window.scrollY + 6;

      // Only update geometry (don't change visibility here)
      Object.assign(resultsMenu.style, {
        position: "absolute",
        left: `${Math.round(left)}px`,
        top: `${Math.round(top)}px`,
        width: `${Math.round(width)}px`,
      });
    }

    // throttle repositioning via rAF
    let scheduled = false;
    function schedule() {
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(() => {
        positionResultsMenuAnchorRight();
        scheduled = false;
      });
    }

    window.__siteSearchPositionResults = positionResultsMenuAnchorRight;
    window.addEventListener("resize", schedule);
    window.addEventListener("orientationchange", schedule);
    window.addEventListener("scroll", schedule, { passive: true });

    // initial position
    positionResultsMenuAnchorRight();
  })();

  // ----------------- Helpers -----------------
  function escapeHtml(s) {
    if (!s) return "";
    return s.replace(
      /[&<>"']/g,
      (m) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        }[m])
    );
  }

  // clear any in-page highlights we created
  function clearInPageHighlights() {
    try {
      const spans = PAGE_ROOT.querySelectorAll("span.search-highlight");
      spans.forEach((sp) => {
        const txt = document.createTextNode(sp.textContent);
        sp.parentNode.replaceChild(txt, sp);
      });
      // normalize to merge adjacent text nodes
      PAGE_ROOT.normalize();
    } catch (e) {
      console.warn("[site-search] clearInPageHighlights error", e);
    }
  }

  // find a match across text nodes; returns a Range (or null)
  function findFirstMatchRange(root, query) {
    if (!query || query.length < MIN_QUERY_LENGTH) return null;
    const q = query.toLowerCase();

    const walker = document.createTreeWalker(
      root,
      NodeFilter.SHOW_TEXT,
      {
        acceptNode: function (node) {
          if (!node.nodeValue || !node.nodeValue.trim())
            return NodeFilter.FILTER_REJECT;
          let el = node.parentElement;
          while (el && el !== root) {
            const tag = el.tagName && el.tagName.toLowerCase();
            if (
              tag === "nav" ||
              tag === "footer" ||
              el.classList.contains("breadcrumb") ||
              el.classList.contains("toc") ||
              el.classList.contains("sidebar")
            ) {
              return NodeFilter.FILTER_REJECT;
            }
            el = el.parentElement;
          }
          return NodeFilter.FILTER_ACCEPT;
        },
      },
      false
    );

    const nodes = [];
    let n;
    while ((n = walker.nextNode())) nodes.push(n);

    for (let i = 0; i < nodes.length; i++) {
      let combined = nodes[i].nodeValue;
      let j = i;
      while (combined.length < q.length && j + 1 < nodes.length && j - i < 6) {
        j++;
        combined += nodes[j].nodeValue || "";
      }

      for (let end = i; end <= j; end++) {
        const part = nodes
          .slice(i, end + 1)
          .map((x) => x.nodeValue || "")
          .join("");
        const partLower = part.toLowerCase();
        const idx = partLower.indexOf(q);
        if (idx !== -1) {
          let acc = 0;
          let startNode = null,
            startOffset = 0;
          for (let k = i; k <= end; k++) {
            const len = (nodes[k].nodeValue || "").length;
            if (acc + len > idx) {
              startNode = nodes[k];
              startOffset = idx - acc;
              break;
            }
            acc += len;
          }
          let matchLen = q.length;
          let endNode = startNode,
            endOffset = startOffset;
          let remaining = matchLen;
          for (
            let k = startNode ? nodes.indexOf(startNode) : i;
            k <= end;
            k++
          ) {
            const seg = nodes[k].nodeValue || "";
            const avail =
              k === (startNode ? nodes.indexOf(startNode) : i)
                ? seg.length - startOffset
                : seg.length;
            if (remaining <= avail) {
              endNode = nodes[k];
              endOffset =
                (k === (startNode ? nodes.indexOf(startNode) : i)
                  ? startOffset
                  : 0) + remaining;
              break;
            } else {
              remaining -= avail;
            }
          }

          try {
            const range = document.createRange();
            range.setStart(startNode, startOffset);
            range.setEnd(endNode, endOffset);
            return range;
          } catch (e) {
            console.warn("[site-search] error creating range for match", e);
            return null;
          }
        }
      }
    }
    return null;
  }

  // wrap a Range in a span and scroll it into view
  function highlightRange(range) {
    if (!range) return null;
    try {
      const frag = range.extractContents();
      const wrap = document.createElement("span");
      wrap.className = "search-highlight";
      wrap.setAttribute("data-site-search-highlight", "1");
      wrap.appendChild(frag);
      range.insertNode(wrap);
      try {
        wrap.scrollIntoView({ behavior: "smooth", block: "center" });
      } catch (e) {}
      return wrap;
    } catch (e) {
      console.warn("[site-search] highlightRange failed", e);
      return null;
    }
  }

  // ----------------- render dropdown results (no result-highlighting) -----------------
  function renderResults(results) {
    resultsInner.innerHTML = "";
    focusedIndex = -1;

    if (!results || results.length === 0) {
      noResults.classList.remove("d-none");
      hideResultsMenu();
      return;
    }
    noResults.classList.add("d-none");

    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      const idx = document.createElement("a");
      idx.className =
        "list-group-item list-group-item-action d-flex align-items-start gap-2";
      idx.href = r.url;
      idx.role = "option";
      idx.setAttribute("data-index", i);

      idx.innerHTML = `
        <div class="me-2" style="width:42px;">
          <img src="${
            r.icon || "/assets/img/favicon/favicon-96x96.png"
          }" width="36" height="36" class="rounded" alt="">
        </div>
        <div class="flex-grow-1">
          <div class="d-flex justify-content-between align-items-start">
            <div class="fw-bold text-sm text-body-emphasis">${escapeHtml(
              r.title
            )}</div>
            <small class="text-muted">${
              r.last_modified ? r.last_modified : ""
            }</small>
          </div>
          <div class="text-muted small mt-1 d-none d-lg-block">${escapeHtml(
            r.excerpt || (r.content || "").slice(0, 120)
          )}</div>
        </div>
      `;
      resultsInner.appendChild(idx);
    }

    // show the menu in a single, controlled place
    showResultsMenu();
  }

  // ----------------- main search function -----------------
  function doSearch(q) {
    q = (q || "").trim();
    if (allResultsLink)
      allResultsLink.href = "/pages/search?q=" + encodeURIComponent(q);

    if (!q) {
      hideResultsMenu();
      clearBtn.classList.add("d-none");
      clearInPageHighlights();
      return;
    }
    clearBtn.classList.remove("d-none");

    // 1) in-page search (find first real match)
    clearInPageHighlights();
    const pageRange = findFirstMatchRange(PAGE_ROOT, q);
    if (pageRange) {
      highlightRange(pageRange);
    }

    // 2) index search fallback / augmentation
    if (indexLoaded && fuse) {
      const fuseRes = fuse.search(q, { limit: MAX_RESULTS });
      const mapped = fuseRes.map((r) => {
        const item = r.item;
        let excerpt = item.excerpt || "";
        if (!excerpt && r.matches && r.matches.length) {
          const m = r.matches.find((x) => x.key === "content") || r.matches[0];
          if (m && m.indices && m.indices.length) {
            const start = Math.max(0, m.indices[0][0] - 40);
            excerpt = item.content
              ? item.content.substr(start, 120).replace(/\s+/g, " ").trim()
              : "";
          }
        }
        return {
          title: item.title,
          url: item.url,
          excerpt:
            excerpt || item.excerpt || (item.content || "").slice(0, 120),
          icon: item.icon,
          last_modified: item.last_modified,
        };
      });

      const combined = mapped.slice(0, MAX_RESULTS);
      renderResults(combined);
    } else {
      // no index: show a "page only" result if we found something
      if (pageRange) {
        renderResults([
          {
            title: document.title || window.location.pathname,
            url: window.location.pathname,
            excerpt: document.title || "",
          },
        ]);
      } else {
        renderResults([]);
      }
    }
  }

  // ----------------- events -----------------
  input.addEventListener("input", (e) => {
    const q = e.target.value;
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => doSearch(q), DEBOUNCE_MS);
  });

  clearBtn.addEventListener("click", () => {
    input.value = "";
    clearBtn.classList.add("d-none");
    hideResultsMenu();
    input.focus();
    clearInPageHighlights();
  });

  input.addEventListener("keydown", (e) => {
    const items = resultsInner.querySelectorAll("[data-index]");
    if (items.length === 0) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      focusedIndex = Math.min(items.length - 1, focusedIndex + 1);
      updateFocus(items);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      focusedIndex = Math.max(0, focusedIndex - 1);
      updateFocus(items);
    } else if (e.key === "Enter") {
      if (focusedIndex >= 0 && items[focusedIndex]) {
        window.location = items[focusedIndex].href;
      } else if (items.length > 0) {
        window.location = items[0].href;
      }
    } else if (e.key === "Escape") {
      hideResultsMenu();
      input.blur();
    }
  });

  function updateFocus(items) {
    items.forEach((el, i) => {
      if (i === focusedIndex) {
        el.classList.add("active");
        el.focus();
      } else {
        el.classList.remove("active");
      }
    });
  }

  document.addEventListener("click", (ev) => {
    const isOpen = resultsMenu.getAttribute("data-open") === "true";
    if (!isOpen) return;
    if (!resultsMenu.contains(ev.target) && ev.target !== input) {
      hideResultsMenu();
    }
  });

  // expose for debugging
  window.__siteSearchPositionResults && window.__siteSearchPositionResults();
  window.__siteSearch_doSearch = doSearch;
})();
