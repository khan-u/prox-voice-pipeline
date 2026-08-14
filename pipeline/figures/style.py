"""Shared figure style for the PROX-VOICE result gallery.

A restrained palette and a clean axis theme (left/bottom spines only, a faint
grid, filled-circle markers) so every regenerated figure matches the report's
look. Import `PALETTE` for series colours and call `apply_theme()` once before
plotting; `target_line()` draws the dashed-grey target markers.
"""
import matplotlib as mpl
import matplotlib.pyplot as plt

# natstyle palette — one accent per series.
PALETTE = {
    "blue": "#2F6DB5",
    "green": "#2E8B4F",
    "red": "#C0392B",
    "orange": "#DE8317",
    "purple": "#7D549F",
    "teal": "#18928A",
    "gray": "#6E7B8B",
    "ink": "#2B2F36",
}

# Default series order for multi-line plots.
SERIES_ORDER = ["blue", "green", "red", "orange", "purple", "teal"]

TARGET_GRAY = "#6E7B8B"


def apply_theme():
    """Apply the shared rcParams theme. Idempotent; call once per process."""
    mpl.rcParams.update({
        "figure.figsize": (5.2, 3.4),
        "figure.dpi": 140,
        "savefig.dpi": 140,
        "savefig.bbox": "tight",
        "font.size": 10,
        "axes.titlesize": 11,
        "axes.labelcolor": PALETTE["ink"],
        "axes.edgecolor": PALETTE["ink"],
        "text.color": PALETTE["ink"],
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.color": "#DfE3E8",
        "grid.linewidth": 0.6,
        "axes.axisbelow": True,
        "lines.marker": "o",
        "lines.markersize": 4.5,
        "lines.linewidth": 1.6,
        "legend.frameon": False,
    })


def new_axes(xlabel, ylabel, title=None):
    """Return a themed (fig, ax) with labels applied."""
    fig, ax = plt.subplots()
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    return fig, ax


def target_line(ax, value, label, axis="y"):
    """Draw a dashed-grey target line with a label (e.g. the 150 ms KPI)."""
    if axis == "y":
        ax.axhline(value, ls="--", lw=1.1, color=TARGET_GRAY)
        ax.text(0.99, value, f" {label}", va="bottom", ha="right",
                color=TARGET_GRAY, transform=ax.get_yaxis_transform(), fontsize=8)
    else:
        ax.axvline(value, ls="--", lw=1.1, color=TARGET_GRAY)
        ax.text(value, 0.99, f" {label}", va="top", ha="left",
                color=TARGET_GRAY, transform=ax.get_xaxis_transform(), fontsize=8)
