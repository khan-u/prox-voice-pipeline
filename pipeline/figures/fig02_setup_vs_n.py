"""Figure 2 — connection setup time vs cluster size N (median and p95).

The median stays low and flat; the p95 line climbs orders of magnitude as one
machine saturates under many WebRTC links, so the y-axis is logarithmic.
Reads the kpi_vs_n gold mart.
"""
import os

from pipeline.figures import style
from pipeline.figures.gold_io import read_gold

OUT = os.path.join(os.path.dirname(__file__), "fig02_setup_vs_n.png")


def plot(rows):
    """Plot median and p95 setup vs N on a semilog axis. `rows` are kpi_vs_n
    dicts with n_peers, setup_median_ms, setup_p95_ms."""
    rows = sorted(rows, key=lambda r: r["n_peers"])
    n = [r["n_peers"] for r in rows]
    med = [r["setup_median_ms"] for r in rows]
    p95 = [r["setup_p95_ms"] for r in rows]

    style.apply_theme()
    fig, ax = style.new_axes("cluster size N", "setup time (ms)",
                             "Formation: connection setup vs N")
    ax.set_yscale("log")
    ax.plot(n, med, color=style.PALETTE["blue"], label="median")
    ax.plot(n, p95, color=style.PALETTE["red"], label="p95")
    ax.legend()
    return fig


def main():
    rows = read_gold("kpi_vs_n", order_by="n_peers")
    fig = plot(rows)
    fig.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
