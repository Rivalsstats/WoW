/*
 * chart-theme.js — shared Chart.js helpers for MythiStone.
 *
 * The site's charts were each configured inline in their template with copied
 * colors, tooltip options, icon-preloading and the patch-release annotation
 * builder. This module centralizes the reusable pieces so charts stay visually
 * consistent and new charts (e.g. the key-level breakdown) don't re-copy the
 * boilerplate. It only DEFINES helpers — it does not touch the DOM or require
 * Chart.js to be loaded yet, so it is safe to include globally before the
 * per-page chart scripts run.
 *
 * Exposed as window.MythiChart.
 */
(function (global) {
  "use strict";

  // Neutral axis/grid/tooltip palette matching the current dark theme.
  var colors = {
    grid: "#e5e5e5",
    gridDark: "rgba(255,255,255,0.08)",
    axisText: "#9ca3af",
    tickText: "#737373",
    legendText: "#cbd5e1",
    patchLine: "rgba(255, 255, 255, 0.75)",
    tooltipBg: "rgba(0, 0, 0, 0.75)",
  };

  // rgba string from a {r,g,b} class-color object (the shape class_lookup uses).
  function rgba(color, alpha) {
    if (!color) return "rgba(128,128,128," + (alpha == null ? 1 : alpha) + ")";
    return (
      "rgba(" +
      (color.r | 0) + ", " + (color.g | 0) + ", " + (color.b | 0) + ", " +
      (alpha == null ? 1 : alpha) + ")"
    );
  }

  // Preload icon URLs into Image objects, resolving null for any that fail so a
  // broken icon never rejects the whole batch. Mirrors the pattern every chart
  // that draws spec/dungeon icons used inline.
  function loadIcons(urls) {
    return Promise.all(
      (urls || []).map(function (src) {
        return new Promise(function (resolve) {
          var img = new Image();
          img.src = src;
          img.onload = function () { resolve(img); };
          img.onerror = function () { resolve(null); };
        });
      })
    );
  }

  // Build Chart.js annotation config for the vertical patch-release lines shown
  // on the weekly time-series charts. `annotations` is [{x, label}, ...].
  function buildPatchAnnotations(annotations) {
    return Object.fromEntries(
      (annotations || []).map(function (p, i) {
        return [
          "patch" + i,
          {
            type: "line",
            xMin: p.x,
            xMax: p.x,
            borderColor: colors.patchLine,
            borderWidth: 2,
            borderDash: [6, 4],
            label: {
              display: true,
              content: p.label,
              position: "end",
              backgroundColor: colors.tooltipBg,
              borderRadius: 4,
              color: "#ffffff",
              font: { size: 12, weight: "bold" },
              padding: { x: 6, y: 3 },
            },
          },
        ];
      })
    );
  }

  /*
   * Parameterized icon-label plugin. Draws preloaded icons next to the ticks of
   * one axis — replacing the two near-identical inline copies (spec bars draw on
   * the x-axis bottom; dungeon bars draw on the y-axis left with short labels).
   *
   * opts:
   *   axis: "x" | "y"            which axis the ticks belong to (default "x")
   *   icons: (Image|null)[]      preloaded, tick-indexed
   *   size: number               icon px size (default 28)
   *   offset: number             gap from the chart edge (default 4)
   *   labels: string[]           optional text drawn beside each icon (y-axis)
   *   textGap, fontSize, fontFace, color  text styling (y-axis labels)
   */
  function makeIconLabelsPlugin(pluginId) {
    return {
      id: pluginId || "iconLabels",
      afterDraw: function (chart, args, opts) {
        if (!opts || !opts.enabled || !opts.icons) return;
        var ctx = chart.ctx;
        var area = chart.chartArea;
        var size = opts.size || 28;
        var axis = opts.axis || "x";

        if (axis === "x") {
          var xScale = chart.scales.x;
          var offsetY = opts.offset == null ? 4 : opts.offset;
          opts.icons.forEach(function (img, i) {
            if (!img) return;
            var xPos = xScale.getPixelForTick(i);
            ctx.drawImage(img, xPos - size / 2, area.bottom + offsetY, size, size);
          });
          return;
        }

        // y-axis: icon (and optional short label) left of the plot area.
        var yScale = chart.scales.y;
        var iconOffset = opts.offset == null ? 8 : opts.offset;
        var textGap = opts.textGap == null ? 4 : opts.textGap;
        var labels = opts.labels || [];
        ctx.textBaseline = "middle";
        if (labels.length) {
          ctx.font = (opts.fontSize || 12) + "px " + (opts.fontFace || "Arial");
          ctx.fillStyle = opts.color || "#fff";
        }
        opts.icons.forEach(function (img, i) {
          var yPos = yScale.getPixelForTick(i);
          var iconX = area.left - iconOffset - size;
          if (img) ctx.drawImage(img, iconX, yPos - size / 2, size, size);
          if (labels[i] != null) {
            ctx.textAlign = "right";
            ctx.fillText(labels[i], iconX - textGap, yPos);
          }
        });
      },
    };
  }

  global.MythiChart = {
    colors: colors,
    rgba: rgba,
    loadIcons: loadIcons,
    buildPatchAnnotations: buildPatchAnnotations,
    makeIconLabelsPlugin: makeIconLabelsPlugin,
  };
})(window);
