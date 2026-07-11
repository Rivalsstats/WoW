"""Shared constants for the image_generation package. Imports os only —
no side effects at import time (directories are created via ensure_output_dir).
"""

import os

ICON_DIR = os.path.join("data", "icons")
OUTPUT_DIR = os.path.join("data", "social")
FONT_DIR = os.path.join("assets", "fonts")
FONT_FILE = os.path.join(FONT_DIR, "BebasNeue-Regular.ttf")

WIDTH, HEIGHT = 1200, 675
DPI = 100
TITLE_PCT = 0.12  # title = 12% of canvas height
SUBTITLE_PCT = 0.055  # subtitle = 5.5%
SMALL_PCT = 0.035  # small = 3.5%
VERY_SMALL_PCT = 0.02  # very small = 2%
TITLE_SIZE = int(HEIGHT * TITLE_PCT)
SUBTITLE_SIZE = int(HEIGHT * SUBTITLE_PCT)
SMALL_SIZE = int(HEIGHT * SMALL_PCT)
VERY_SMALL_SIZE = int(HEIGHT * VERY_SMALL_PCT)

tier_colors = {
    "S": "#ff8000",  # Legendary
    "A": "#a335ee",  # Epic
    "B": "#0070dd",  # Rare
    "C": "#1eff00",  # Uncommon
    "F": "#9d9d9d",  # Poor
}

# Modern shared palette (matches the SimC tierlist page / og:image preview).
BG = (17, 21, 30)  # dark canvas base
BG_HEX = "#11151e"
TEXT = (233, 236, 242)
TEXT_HEX = "#e9ecf2"
MUTED = (150, 158, 172)
MUTED_HEX = "#969eac"
TRACK = (40, 46, 60)  # empty bar track
DIVIDER = (48, 55, 70)
DIVIDER_HEX = "#303746"
PANEL_FILL = (24, 29, 40, 215)
PANEL_OUTLINE = (48, 55, 70)
BG_ALPHA = 0.15  # background image visibility over BG
PANEL_ART_ALPHA = 0.45  # dungeon-art visibility inside panel cards (vs BG_ALPHA for full canvases)

# Tier badge colours (outline, text) mirroring simc_tierlist.html's CSS.
# Unlike tier_colors above, these are outline/text pairs and include a D tier.
TIER_COLORS = {
    "S": ("#ff7c0a", "#ff9d47"),
    "A": ("#a335ee", "#c77dff"),
    "B": ("#0070dd", "#4da3ff"),
    "C": ("#1eff00", "#52d769"),
    "D": ("#9d9d9d", "#b8b8b8"),
    "F": ("#ff4141", "#ff7b7b"),
}


def ensure_output_dir():
    """Create the social-image output directory (replaces the old import-time
    makedirs); call before rendering into OUTPUT_DIR."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
