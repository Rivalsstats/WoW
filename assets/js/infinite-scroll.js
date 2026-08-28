/* Infinite scroll — shared sentinel loader for the blog, items and routes pages.
 *
 * An IntersectionObserver watches a sentinel element sitting at the end of a
 * list. Whenever the sentinel enters view (plus a generous rootMargin so the
 * next batch is fetched before the visitor actually reaches the bottom) it calls
 * onLoadMore(), which appends the next batch and returns — or resolves to — true
 * once the list is exhausted. After that the sentinel is hidden and stops firing.
 *
 * The observer is re-armed after every batch rather than left running: a plain
 * IntersectionObserver only fires on a state change, so a sentinel that stays on
 * screen (a short list on a tall window) would never load a second batch.
 * unobserve + observe forces a fresh callback on the next frame, so loading
 * continues until the sentinel is finally pushed below the fold or the list ends.
 *
 * Pages that rebuild their list from scratch (a new filter or search) call
 * reset() to clear the done latch and re-arm the observer, or finish() to mark
 * the list complete without another callback (e.g. a search that found nothing).
 */
window.MythiInfinite = (function () {
  "use strict";

  function create(opts) {
    opts = opts || {};
    var sentinel = opts.sentinel;
    var onLoadMore = opts.onLoadMore;
    var rootMargin = opts.rootMargin || "800px";

    if (!sentinel || typeof onLoadMore !== "function") {
      throw new Error("MythiInfinite.create needs a sentinel element and an onLoadMore function");
    }

    var loading = false;
    var done = false;

    var observer = new IntersectionObserver(function (entries) {
      for (var i = 0; i < entries.length; i++) {
        if (entries[i].isIntersecting) {
          trigger();
          break;
        }
      }
    }, { rootMargin: rootMargin });

    function trigger() {
      if (loading || done) return;
      loading = true;
      // Stop watching while the batch loads so a scroll mid-load can't stack a
      // second call; re-observing below is what asks for the next one.
      observer.unobserve(sentinel);
      Promise.resolve()
        .then(onLoadMore)
        .then(function (isDone) {
          loading = false;
          if (isDone) {
            done = true;
            sentinel.classList.add("d-none");
          } else {
            observer.observe(sentinel);
          }
        })
        .catch(function (err) {
          // Fail loudly in the console, but leave the observer armed so a later
          // scroll can retry rather than wedging the list permanently.
          loading = false;
          console.error("infinite-scroll: load failed", err);
          observer.observe(sentinel);
        });
    }

    observer.observe(sentinel);

    return {
      // Start over for a fresh list. Clears the done latch and re-arms the
      // observer, which reloads the first extra batch if the sentinel is visible.
      reset: function () {
        done = false;
        loading = false;
        sentinel.classList.remove("d-none");
        observer.unobserve(sentinel);
        observer.observe(sentinel);
      },
      // Mark the list finished from outside without another onLoadMore call.
      finish: function () {
        done = true;
        loading = false;
        observer.unobserve(sentinel);
        sentinel.classList.add("d-none");
      },
      disconnect: function () {
        observer.disconnect();
      }
    };
  }

  return { create: create };
})();
