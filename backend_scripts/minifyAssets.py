"""In-place minifier for first-party CSS/JS in the assembled site.

Run against the assets tree that has already been copied into ``_site`` by the
buildPages.yml ``assemble`` job. Every ``*.css``/``*.js`` under the given root is
minified in place (same filename), so templates keep referencing the readable
source path (for example ``/assets/css/material-dashboard.css``) while the bytes
actually deployed are minified.

Vendor libraries that ship pre-minified from upstream are named ``*.min.css`` /
``*.min.js`` and are skipped, as are source maps (``*.map``). That naming rule
covers exactly the first-party sources we edit and nothing else.

Fail loudly: any read, minify, or write error aborts the whole run with a
non-zero exit so the build fails rather than deploying unminified or broken
bytes.

Usage:
    python backend_scripts/minifyAssets.py _site/assets
"""

import os
import sys
import argparse

import rcssmin
import rjsmin


def _iter_targets(root):
    """Yield first-party .css/.js paths under root, skipping .min.* and .map."""
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            lower = name.lower()
            if lower.endswith(".min.css") or lower.endswith(".min.js"):
                continue
            if lower.endswith(".map"):
                continue
            if lower.endswith(".css") or lower.endswith(".js"):
                yield os.path.join(dirpath, name)


def _minify_file(path):
    """Minify one file in place, returning (before_bytes, after_bytes)."""
    with open(path, "r", encoding="utf-8") as fh:
        source = fh.read()
    if path.lower().endswith(".css"):
        minified = rcssmin.cssmin(source, keep_bang_comments=True)
    else:
        minified = rjsmin.jsmin(source, keep_bang_comments=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(minified)
    return len(source.encode("utf-8")), len(minified.encode("utf-8"))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("assets_root", help="Assets dir to minify in place, e.g. _site/assets")
    args = parser.parse_args(argv)

    root = args.assets_root
    if not os.path.isdir(root):
        raise SystemExit(f"minifyAssets: assets root not found: {root}")

    total_before = 0
    total_after = 0
    count = 0
    for path in sorted(_iter_targets(root)):
        try:
            before, after = _minify_file(path)
        except Exception as exc:
            # Fail loudly: never let a bad asset ship as-is.
            raise SystemExit(f"minifyAssets: failed on {path}: {exc}") from exc
        total_before += before
        total_after += after
        count += 1
        rel = os.path.relpath(path, root)
        print(f"  {rel}: {before} -> {after} bytes")

    if count == 0:
        raise SystemExit(f"minifyAssets: no first-party .css/.js found under {root}")

    saved = total_before - total_after
    print(f"minifyAssets: minified {count} files, {total_before} -> {total_after} bytes ({saved} saved)")


if __name__ == "__main__":
    main()
