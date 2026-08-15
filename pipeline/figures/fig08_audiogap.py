"""Figure 8 — listener-perceived audio gap (out-and-back).

Leaving range silences the listener almost immediately (stop); the gap on
return and the total silence over a 9 s absence are larger. Median with min-max
whiskers. Reads the audiogap gold mart.
"""
import os

from pipeline.figures import style
from pipeline.figures.gold_io import read_gold

OUT = os.path.join(os.path.dirname(__file__), "fig08_audiogap.png")
PHASES = ["stop", "reconnect", "total_silence"]
LABELS = ["stop\n(leaving)", "reconnect\n(return)", "total silence\n(9s absence)"]


def plot(rows):
    """Point-with-error-bar per audio-gap phase. `rows` is the single-row
    audiogap mart (median/min/max per phase)."""
    r = rows[0]
    med = [r[f"{p}_median_ms"] for p in PHASES]
    lo = [r[f"{p}_median_ms"] - r[f"{p}_min_ms"] for p in PHASES]
    hi = [r[f"{p}_max_ms"] - r[f"{p}_median_ms"] for p in PHASES]

    style.apply_theme()
    fig, ax = style.new_axes("phase", "silence (ms)",
                             "Listener audio gap (median, min-max)")
    ax.set_yscale("log")
    ax.errorbar(LABELS, med, yerr=[lo, hi], fmt="o", capsize=5,
                color=style.PALETTE["red"])
    return fig


def main():
    rows = read_gold("audiogap")
    fig = plot(rows)
    fig.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
