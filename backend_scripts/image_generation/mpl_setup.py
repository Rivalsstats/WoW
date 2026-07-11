"""Matplotlib font/rcParams setup for the chart renderers.

Only chart modules import this, so matplotlib stays out of the import path of
the pure-Pillow renderers. Call init_matplotlib() at the top of every function
that draws with matplotlib; it is idempotent.
"""

from matplotlib import font_manager, rcParams

from image_generation import config

_initialized = False


def init_matplotlib():
    """Register Bebas Neue and apply the global font sizes and the shared dark
    theme (once)."""
    global _initialized
    if _initialized:
        return
    font_manager.fontManager.addfont(config.FONT_FILE)
    custom_font = font_manager.FontProperties(fname=config.FONT_FILE).get_name()
    rcParams["font.family"] = custom_font
    rcParams["font.sans-serif"] = [custom_font]
    rcParams.update(
        {
            "axes.titlesize": config.SUBTITLE_SIZE,
            "axes.labelsize": config.VERY_SMALL_SIZE,
            "xtick.labelsize": config.VERY_SMALL_SIZE,
            "ytick.labelsize": config.VERY_SMALL_SIZE,
            "legend.fontsize": config.VERY_SMALL_SIZE,
            # dark theme matching the modern PIL renderers (config palette)
            "figure.facecolor": config.BG_HEX,
            "axes.facecolor": config.BG_HEX,
            "savefig.facecolor": config.BG_HEX,
            "text.color": config.MUTED_HEX,
            "axes.labelcolor": config.MUTED_HEX,
            "xtick.color": config.MUTED_HEX,
            "ytick.color": config.MUTED_HEX,
            "axes.titlecolor": config.TEXT_HEX,
            "axes.edgecolor": config.DIVIDER_HEX,
            "grid.color": config.DIVIDER_HEX,
            "grid.linestyle": "--",
            "grid.alpha": 0.5,
        }
    )
    _initialized = True
