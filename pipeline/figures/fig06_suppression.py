"""Figure 6 — sensor report-suppression by scenario.

A static idle node stays ~91% quiet; suppression falls as real events pick up
(walk, churn, conversation). Reads the sensing_suppression gold mart.
"""
import os

from pipeline.figures import style
from pipeline.figures.gold_io import read_gold

OUT = os.path.join(os.path.dirname(__file__), "fig06_suppression.png")
# Scenario order from idlest to busiest.
ORDER = ["idle", "walk", "churn", "conv"]


def plot(rows):
    """Bar chart of suppression median by scenario. `rows` are
    sensing_suppression dicts with scenario and suppression_median_pct."""
    by = {r["scenario"]: r["suppression_median_pct"] for r in rows}
    scenarios = [s for s in ORDER if s in by]
    values = [by[s] for s in scenarios]

    style.apply_theme()
    fig, ax = style.new_axes("scenario", "suppression (%)",
                             "Sensing: suppression by scenario")
    ax.bar(scenarios, values, color=style.PALETTE["teal"], width=0.6)
    ax.set_ylim(0, 100)
    return fig


def main():
    rows = read_gold("sensing_suppression")
    fig = plot(rows)
    fig.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
