import os
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
from generateSpecPages import (
    LOOKUP_DIR,
    load_json,
)

# ------------------------------- CONFIG ------------------------------------
# Edit these constants as needed. The script will scan each directory listed
# in SITE_DIRS that actually exists in the repo/workspace.
SITE_DIRS = ["."]

# File name written into each found site dir and into repo root
OUTPUT_FILENAME = "search_index.json"

# Patterns / names to skip
EXCLUDE_DIRS = {
    ".git",
    ".github",
    ".vscode",
    "assets",
    "backend_scripts",
    "templates",
    "data",
}
SKIP_FILES = {"404.html", "impressum.html", "privacy.html", "about.html"}

# Optional path -> tag overrides (prefix match)
PATH_TAG_OVERRIDES = {
    "classes/dps": ["class", "dps"],
    "classes/tank": ["class", "tank"],
    "classes/healer": ["class", "healer"],
}

SPEC_LOOKUP_PATH = "data/specs.json"
CLASS_LOOKUP_PATH = "data/classes.json"

spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))

spec_id_lookup = {
    f"{spec_lookup[s]['name']}_{class_lookup[spec_lookup[s]['classID']]['name']}": s
    for s in spec_lookup
}

# Cap content length (keeps index size reasonable)
MAX_CONTENT_CHARS = 60_000
# ---------------------------------------------------------------------------


def textify_html(html):
    soup = BeautifulSoup(html, "html.parser")
    for s in soup(["script", "style", "noscript", "svg", "img"]):
        s.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    if len(text) > MAX_CONTENT_CHARS:
        return text[:MAX_CONTENT_CHARS]
    return text


def get_meta(soup, name):
    # Checks <meta name="..."> and <meta property="...">
    tag = soup.find("meta", attrs={"name": name})
    if tag and tag.get("content"):
        return tag["content"].strip()
    tag = soup.find("meta", attrs={"property": name})
    if tag and tag.get("content"):
        return tag["content"].strip()
    return None


def path_to_url(relpath):
    rel = relpath.replace(os.sep, "/")
    # always strip html -> pretty URLs
    if rel.endswith(".html"):
        url = "/" + rel[: -len(".html")]
    else:
        url = "/" + rel
    url = re.sub(r"/+", "/", url)
    return url


def infer_tags_from_path(relpath):
    parts = relpath.replace(os.sep, "/").split("/")
    tags = set()
    for p in parts[:-1]:
        if p and p not in EXCLUDE_DIRS:
            tags.add(p)
    basename = os.path.splitext(parts[-1])[0]
    if basename and basename not in ("index", "404"):
        tags.add(basename)
    # overrides
    for prefix, more in PATH_TAG_OVERRIDES.items():
        if relpath.startswith(prefix):
            tags.update(more)
    return sorted(tags)


def choose_icon(relpath):
    name = os.path.splitext(os.path.basename(relpath))[0]
    spec, cls = name.split("_") if "_" in name else (None, None)
    s_id = spec_id_lookup.get(f"{spec}_{cls}") if spec and cls else None
    if not s_id:
        return None
    if spec_lookup[str(s_id)]["SpellIconFileId"]:
        return f"/data/icons/{spec_lookup[str(s_id)]['SpellIconFileId']}.jpg"
    return None


def make_excerpt(content, maxlen=220):
    if not content:
        return ""
    m = re.search(r"([^.?!]{50,}?[.?!])\s", content)
    if m:
        excerpt = m.group(1).strip()
    else:
        excerpt = content.strip()[:maxlen].rsplit(" ", 1)[0]
    return excerpt


def collect_html_files(site_roots):
    """
    Walk each directory in site_roots, collect .html files (skip excluded).
    Returns list of tuples (abs_path, relpath).
    """
    files = []
    for root in site_roots:
        if not os.path.isdir(root):
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            # prune directories
            dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
            for fname in filenames:
                if not fname.lower().endswith(".html"):
                    continue
                if fname in SKIP_FILES:
                    continue
                abs_path = os.path.join(dirpath, fname)
                relpath = os.path.relpath(abs_path, root)
                files.append((abs_path, relpath.replace(os.sep, "/"), root))
    return files


def build_index_for_paths(collected):
    """
    Build index items from collected file tuples.
    Deduplicate by URL: first seen wins.
    """
    items_by_url = {}
    for abs_path, relpath, site_root in collected:
        # read file
        try:
            with open(abs_path, "r", encoding="utf-8") as fh:
                html = fh.read()
        except Exception as e:
            print(f"Warning: cannot read {abs_path}: {e}")
            continue
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else None
        if not title:
            title = get_meta(soup, "og:title") or get_meta(soup, "title")

        description = get_meta(soup, "description") or get_meta(soup, "og:description")
        main = soup.find("main") or soup.find("article") or soup.body or soup
        content_text = textify_html(str(main))
        if not description:
            description = make_excerpt(content_text, 220)

        keywords = get_meta(soup, "keywords") or get_meta(soup, "tags")
        if keywords:
            tags = [t.strip() for t in re.split(r"[,\|;]+", keywords) if t.strip()]
        else:
            tags = infer_tags_from_path(relpath)

        # file modified time
        try:
            mtime = os.path.getmtime(abs_path)
            last_mod = datetime.fromtimestamp(mtime, datetime.timezone.utc).strftime(
                "%Y-%m-%d"
            )
        except Exception:
            last_mod = None

        url = path_to_url(relpath)
        icon = choose_icon(relpath)

        # dedupe by url
        if url in items_by_url:
            # already present (from another site root) -> skip
            continue

        item = {
            "title": title or url,
            "url": url,
            "path": relpath,
            "content": content_text,
            "excerpt": description,
            "tags": tags,
            "last_modified": last_mod,
        }
        if icon:
            item["icon"] = icon

        items_by_url[url] = item

    # sort by url for determinism
    items = [items_by_url[k] for k in sorted(items_by_url.keys())]
    return items


def write_output(items, targets):
    data = items
    json_text = json.dumps(data, ensure_ascii=False, indent=2)
    outpath = os.path.join("assets", "json", OUTPUT_FILENAME)
    try:
        # ensure parent dir exists
        parent = os.path.dirname(outpath)
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        with open(outpath, "w", encoding="utf-8") as fh:
            fh.write(json_text)
        print(f"Wrote {len(items)} entries to {outpath}")
    except Exception as e:
        print(f"Error writing to {outpath}: {e}")


def main():
    # determine which site dirs actually exist
    found_dirs = [d for d in SITE_DIRS if os.path.isdir(d)]
    if not found_dirs:
        print(
            "No site directories found among SITE_DIRS. Falling back to current directory."
        )
        found_dirs = [os.getcwd()]

    # collect files from all found dirs
    collected = collect_html_files(found_dirs)
    if not collected:
        print("No .html files found in the specified site directories.")
    items = build_index_for_paths(collected)

    # targets: write into every found site root, and one copy to repo root (cwd)
    write_targets = list(found_dirs) + [os.path.join(os.getcwd(), OUTPUT_FILENAME)]
    write_output(items, write_targets)


if __name__ == "__main__":
    main()
