"""Figure 12 — grace-window duty-cycle sweep.

With no grace window the link is torn down and pays the full ~2.5 s wakeup on
return; any positive window collapses the reconnect gap roughly tenfold. One
line per absence duration. Reads the gracegap_series gold mart.
"""
import os

from pipeline.figures import style
from pipeline.figures.gold_io import read_gold

OUT = os.path.join(os.path.dirname(__file__), "fig12_grace_window.png")


def plot(rows):
    """Reconnect gap vs grace window, one series per absence duration. `rows`
    are gracegap_series dicts."""
    by_absence = {}
    for r in rows:
        by_absence.setdefault(r["absence_ms"], []).append(r)

    style.apply_theme()
    fig, ax = style.new_axes("grace window W (ms)", "reconnect gap (ms)",
                             "Duty cycling: reconnect gap vs grace window")
    ax.set_yscale("log")
    colors = [style.PALETTE["blue"], style.PALETTE["orange"]]
    for i, absence in enumerate(sorted(by_absence)):
        series = sorted(by_absence[absence], key=lambda r: r["grace_ms"])
        w = [r["grace_ms"] for r in series]
        gap = [r["gap_median_ms"] for r in series]
        ax.plot(w, gap, color=colors[i % len(colors)],
                label=f"{absence // 1000}s absence")
    ax.legend()
    return fig


def main():
    rows = read_gold("gracegap_series")
    fig = plot(rows)
    fig.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
