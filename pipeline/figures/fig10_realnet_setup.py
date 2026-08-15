"""Figure 10 — real-network connection setup time, direct vs forced-relay.

Unlike RTT, the cost of forcing a relay shows up in negotiation: on cellular the
relay path adds over a second to setup, while on home Wi-Fi it is small. Grouped
bars per access network. Reads the realnet_conditions gold mart.
"""
import os
import statistics as st

from pipeline.figures import style
from pipeline.figures.gold_io import read_gold

OUT = os.path.join(os.path.dirname(__file__), "fig10_realnet_setup.png")
ACCESS = ["home", "cellular"]
PATHS = ["direct", "relay"]


def _mean_by(rows, metric):
    groups = {}
    for r in rows:
        groups.setdefault((r["access"], r["path"]), []).append(r[metric])
    return {k: round(st.mean(v)) for k, v in groups.items()}


def plot(rows):
    """Grouped bar chart of mean setup time by access and path."""
    means = _mean_by(rows, "setup_median_ms")
    style.apply_theme()
    fig, ax = style.new_axes("access network", "setup time (ms)",
                             "Real network: setup direct vs relay")
    width = 0.36
    xs = range(len(ACCESS))
    for i, path in enumerate(PATHS):
        offs = [x + (i - 0.5) * width for x in xs]
        vals = [means.get((a, path), 0) for a in ACCESS]
        ax.bar(offs, vals, width=width, label=path,
               color=style.PALETTE["green" if path == "direct" else "red"])
    ax.set_xticks(list(xs))
    ax.set_xticklabels(ACCESS)
    ax.legend()
    return fig


def main():
    rows = read_gold("realnet_conditions")
    fig = plot(rows)
    fig.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
