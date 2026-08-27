/**
 * Persists dismissals of dismissible site notifications across pages and reloads
 * (per browser, via localStorage).
 *
 * Each dismissible notification carries a stable, content-derived key in its
 * `data-notification-key` attribute (an explicit `id` from notifications.json,
 * otherwise message+link, stamped at build time in templates/notifications.html).
 * The key is identical on every page for the same notification, so a dismissal on
 * one page hides it everywhere.
 *
 * This script is loaded synchronously at the tail of templates/notifications.html,
 * the shared include every page composes, immediately after the notification
 * markup (NOT deferred, NOT in javascript_imports.html): a blocking classic script
 * at that DOM position runs before the browser paints the markup above it, so
 * already-dismissed notifications are removed without a visible
 * flash-then-disappear.
 *
 * Remembering a dismissal is first-party functional storage, treated exactly like
 * the light/dark theme preference (localStorage 'theme', set in
 * assets/js/material-dashboard.js and read inline in header_imports.html): it is
 * strictly-necessary for the feature the visitor is actively using, so it is NOT
 * gated behind the Klaro consent banner (which governs third-party embeds). All
 * localStorage access is wrapped in try/catch, so private mode / disabled storage
 * degrades gracefully to the previous behaviour (notifications simply reappear).
 */
(function () {
    "use strict";

    var STORAGE_KEY = "mythistone.dismissedNotifications";

    function readDismissed() {
        try {
            var raw = window.localStorage.getItem(STORAGE_KEY);
            if (!raw) {
                return [];
            }
            var parsed = JSON.parse(raw);
            return Array.isArray(parsed) ? parsed : [];
        } catch (e) {
            return [];
        }
    }

    function recordDismissed(key) {
        try {
            var list = readDismissed();
            if (list.indexOf(key) === -1) {
                list.push(key);
                window.localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
            }
        } catch (e) {
            /* private mode / storage disabled: nothing to persist, degrade quietly */
        }
    }

    // Only notifications that render a Bootstrap dismiss control are persistable.
    function isDismissible(el) {
        return !!el.querySelector('[data-bs-dismiss="alert"]');
    }

    // Hide already-dismissed notifications. Remove the node outright rather than
    // setting display:none, because the alert carries Bootstrap's `d-flex`
    // (display:flex !important), which an inline display:none cannot override.
    var dismissed = readDismissed();
    var alerts = document.querySelectorAll(".page-notification[data-notification-key]");
    Array.prototype.forEach.call(alerts, function (el) {
        if (!isDismissible(el)) {
            return;
        }
        var key = el.getAttribute("data-notification-key");
        if (key && dismissed.indexOf(key) !== -1) {
            el.parentNode.removeChild(el);
        }
    });

    // Record the key when the user dismisses. Bootstrap fires close.bs.alert (which
    // bubbles to document) before it removes the element. Bootstrap loads later in
    // the page, but registering the listener now is fine: it only fires on a click.
    document.addEventListener("close.bs.alert", function (e) {
        var el = e.target;
        if (!el || !el.classList || !el.classList.contains("page-notification")) {
            return;
        }
        var key = el.getAttribute("data-notification-key");
        if (key) {
            recordDismissed(key);
        }
    });
})();
