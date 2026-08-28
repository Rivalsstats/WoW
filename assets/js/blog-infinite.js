/* Blog infinite scroll — appends the posts after page 1 from blog_index.json.
 *
 * Page 1 is server-rendered into #blog-posts by generateBlogPage.py (SEO, first
 * paint, no-JS). Every later post lives in /assets/json/blog_index.json, the same
 * client-rendered-from-JSON pattern the items and routes pages use. As the
 * sentinel nears the fold, MythiInfinite calls in here to append the next batch.
 *
 * DRY: the card markup is NOT written as an HTML string here. #blog-card-template
 * holds one card rendered from the very same Jinja macro the server-rendered
 * page-1 cards use, so both share a single definition. Each post clones that
 * template and fills the [data-blog="…"] hooks.
 */
(function () {
  "use strict";

  var postsRow = document.getElementById("blog-posts");
  var sentinel = document.getElementById("blog-sentinel");
  var tpl = document.getElementById("blog-card-template");
  if (!postsRow || !sentinel || !tpl || !window.MythiInfinite) return;

  var INDEX_URL = "/assets/json/blog_index.json";
  var BATCH = 12;
  var posts = null; // filled on first load
  var cursor = 0;
  var loadPromise = null;

  function loadIndex() {
    if (loadPromise) return loadPromise;
    loadPromise = fetch(INDEX_URL)
      .then(function (r) {
        if (!r.ok) throw new Error("blog_index fetch failed: " + r.status);
        return r.json();
      })
      .then(function (data) {
        posts = Array.isArray(data) ? data : [];
        return posts;
      });
    return loadPromise;
  }

  // The appended cards missed the one-shot timestamp pass in
  // javascript_imports.html, so re-run the same "<label> <relative time>" fill
  // (and title) that it does, scoped to the freshly added node.
  function initTimestamps(root) {
    root.querySelectorAll(".timestamp").forEach(function (elm) {
      var ts = elm.getAttribute("data-timestamp");
      elm.textContent = (elm.textContent + " " + timeAgo(Number(ts))).trim();
      elm.setAttribute("title", new Date(Number(ts) * 1000).toLocaleString());
    });
  }

  function initTooltips(root) {
    if (!window.bootstrap || !window.bootstrap.Tooltip) return;
    root.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(function (elm) {
      window.bootstrap.Tooltip.getOrCreateInstance(elm);
    });
  }

  // Clone the shared template and populate the hooks from one post object.
  function buildCard(post) {
    var col = tpl.content.cloneNode(true).querySelector(".col-lg-6");

    var image = col.querySelector('[data-blog="image"]');
    if (post.image) {
      var link = image.querySelector('[data-blog="image-link"]');
      if (link) link.setAttribute("href", post.link || "#");
      var img = image.querySelector("img");
      if (img) {
        img.setAttribute("src", "/data/social/" + post.image);
        img.setAttribute("alt", post.title || "");
      }
    } else if (image) {
      image.remove();
    }

    var badge = col.querySelector('[data-blog="badge"]');
    if (badge) {
      (post.badge_class || "").split(/\s+/).forEach(function (c) {
        if (c) badge.classList.add(c);
      });
      badge.textContent = post.type_label || "";
    }

    var ts = col.querySelector(".timestamp");
    if (ts) ts.setAttribute("data-timestamp", Number(post.timestamp || 0) / 1000);

    var title = col.querySelector('[data-blog="title"]');
    if (title) title.textContent = post.title || "";

    var paragraphs = col.querySelector('[data-blog="paragraphs"]');
    var proto = paragraphs && paragraphs.querySelector('[data-blog="paragraph"]');
    if (paragraphs && proto) {
      paragraphs.innerHTML = "";
      (post.paragraphs || []).forEach(function (text) {
        var p = proto.cloneNode(true);
        p.textContent = text;
        paragraphs.appendChild(p);
      });
    }

    var viewLink = col.querySelector('[data-blog="link"]');
    if (viewLink) viewLink.setAttribute("href", post.link || "#");

    return col;
  }

  window.MythiInfinite.create({
    sentinel: sentinel,
    onLoadMore: function () {
      return loadIndex()
        .then(function () {
          if (cursor >= posts.length) return true;
          var slice = posts.slice(cursor, cursor + BATCH);
          cursor += slice.length;
          var frag = document.createDocumentFragment();
          var added = [];
          slice.forEach(function (post) {
            var card = buildCard(post);
            added.push(card);
            frag.appendChild(card);
          });
          postsRow.appendChild(frag);
          added.forEach(function (node) {
            initTimestamps(node);
            initTooltips(node);
          });
          return cursor >= posts.length;
        })
        .catch(function (err) {
          // A missing/broken feed is a real build error: log it and stop asking
          // rather than retrying the same failing fetch on every scroll.
          console.error("blog-infinite: could not load more posts", err);
          return true;
        });
    },
  });
})();
