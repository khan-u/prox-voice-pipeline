"""Figure 7 — mobility phase timing: form, teardown, reconnect.

Median per phase with min-max whiskers over the complete cycles. Leaving range
silences the listener quickly; reconnect on return sits near the 2 s target.
Reads the mobility_cycles gold mart.
"""
import os

from pipeline.figures import style
from pipeline.figures.gold_io import read_gold

OUT = os.path.join(os.path.dirname(__file__), "fig07_mobility.png")
PHASES = ["form", "teardown", "reconnect"]


def plot(rows):
    """Point-with-error-bar per phase. `rows` is the single-row mobility_cycles
    mart (median/min/max per phase)."""
    r = rows[0]
    med = [r[f"{p}_median_ms"] for p in PHASES]
    lo = [r[f"{p}_median_ms"] - r[f"{p}_min_ms"] for p in PHASES]
    hi = [r[f"{p}_max_ms"] - r[f"{p}_median_ms"] for p in PHASES]

    style.apply_theme()
    fig, ax = style.new_axes("phase", "time (ms)",
                             "Mobility: phase timing (median, min-max)")
    ax.errorbar(PHASES, med, yerr=[lo, hi], fmt="o", capsize=5,
                color=style.PALETTE["blue"])
    return fig


def main():
    rows = read_gold("mobility_cycles")
    fig = plot(rows)
    fig.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
