"""Figure 5 — uplink cost vs cluster size N.

Per-link uplink is essentially constant (~24 kbps), but because a speaker
transmits to every neighbour, a node's total upload grows linearly with N-1.
Both series are drawn. Reads the kpi_vs_n gold mart.
"""
import os

from pipeline.figures import style
from pipeline.figures.gold_io import read_gold

OUT = os.path.join(os.path.dirname(__file__), "fig05_uplink.png")


def plot(rows):
    """Plot per-link and total uplink vs N. `rows` are kpi_vs_n dicts; rows with
    no uplink measurement (uplink_kbps is None) are skipped."""
    rows = sorted((r for r in rows if r.get("uplink_kbps") is not None),
                  key=lambda r: r["n_peers"])
    n = [r["n_peers"] for r in rows]
    per_link = [r["uplink_kbps"] for r in rows]
    total = [r["uplink_kbps"] * (r["n_peers"] - 1) for r in rows]

    style.apply_theme()
    fig, ax = style.new_axes("cluster size N", "uplink (kbps)",
                             "Bandwidth: uplink vs N")
    ax.plot(n, per_link, color=style.PALETTE["blue"], label="per link")
    ax.plot(n, total, color=style.PALETTE["orange"], label="node total")
    ax.legend()
    return fig


def main():
    rows = read_gold("kpi_vs_n", order_by="n_peers")
    fig = plot(rows)
    fig.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
