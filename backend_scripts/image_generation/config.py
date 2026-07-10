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


def ensure_output_dir():
    """Create the social-image output directory (replaces the old import-time
    makedirs); call before rendering into OUTPUT_DIR."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
