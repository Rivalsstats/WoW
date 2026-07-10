"""Image rendering for MythiStone social posts and page previews.

One module per generated image (spec_overview, dungeon_overview, comp_overview,
mplus_run, the tierlists/charts and the SimC tierlist preview) plus shared
helpers (config, pil_helpers, mpl_setup).

This __init__ must stay import-free: the SimC tierlist CI job imports
image_generation.tierlist_preview in an environment that only has jinja2 and
Pillow installed (no matplotlib, no mysql, no openai) and nothing here may
touch the database or filesystem at import time.
"""
