"""Figure 4 — mouth-to-ear latency vs cluster size N.

Latency is flat across N and sits well under the 150 ms conversational target
(drawn as a dashed line). Reads the kpi_vs_n gold mart.
"""
import os

from pipeline.figures import style
from pipeline.figures.gold_io import read_gold

OUT = os.path.join(os.path.dirname(__file__), "fig04_m2e.png")
TARGET_MS = 150


def plot(rows):
    """Plot mouth-to-ear latency vs N with the target line. `rows` are kpi_vs_n
    dicts with n_peers and m2e_ms."""
    rows = sorted(rows, key=lambda r: r["n_peers"])
    n = [r["n_peers"] for r in rows]
    m2e = [r["m2e_ms"] for r in rows]

    style.apply_theme()
    fig, ax = style.new_axes("cluster size N", "mouth-to-ear latency (ms)",
                             "Latency vs N")
    ax.plot(n, m2e, color=style.PALETTE["green"], label="measured")
    ax.set_ylim(0, TARGET_MS * 1.1)
    style.target_line(ax, TARGET_MS, f"{TARGET_MS} ms target")
    ax.legend(loc="lower left")
    return fig


def main():
    rows = read_gold("kpi_vs_n", order_by="n_peers")
    fig = plot(rows)
    fig.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
