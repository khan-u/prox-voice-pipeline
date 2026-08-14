"""Figure 3 — glare (signalling collisions) vs mesh density N(N-1).

Every ordered pair of peers can collide during offer/answer, so glare grows with
the directed link count N(N-1). Reads the kpi_vs_n gold mart.
"""
import os

from pipeline.figures import style
from pipeline.figures.gold_io import read_gold

OUT = os.path.join(os.path.dirname(__file__), "fig03_glare.png")


def plot(rows):
    """Plot total glare against mesh density N(N-1). `rows` are kpi_vs_n dicts."""
    rows = sorted(rows, key=lambda r: r["n_peers"])
    density = [r["n_peers"] * (r["n_peers"] - 1) for r in rows]
    glare = [r["glare_total"] for r in rows]

    style.apply_theme()
    fig, ax = style.new_axes("mesh density  N(N-1)", "glare collisions",
                             "Self-organisation: glare vs density")
    ax.plot(density, glare, color=style.PALETTE["purple"])
    return fig


def main():
    rows = read_gold("kpi_vs_n", order_by="n_peers")
    fig = plot(rows)
    fig.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
