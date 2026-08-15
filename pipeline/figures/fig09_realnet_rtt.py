"""Figure 9 — real-network candidate-pair RTT, direct vs forced-relay.

On both home Wi-Fi and cellular the mesh connects directly; forcing a relay adds
only a small, access-independent RTT penalty. Grouped bars per access network.
Reads the realnet_conditions gold mart.
"""
import os
import statistics as st

from pipeline.figures import style
from pipeline.figures.gold_io import read_gold

OUT = os.path.join(os.path.dirname(__file__), "fig09_realnet_rtt.png")
ACCESS = ["home", "cellular"]
PATHS = ["direct", "relay"]


def _mean_by(rows, metric):
    """Mean of `metric` grouped by (access, path)."""
    groups = {}
    for r in rows:
        groups.setdefault((r["access"], r["path"]), []).append(r[metric])
    return {k: round(st.mean(v)) for k, v in groups.items()}


def plot(rows):
    """Grouped bar chart of mean RTT by access and path. `rows` are
    realnet_conditions dicts."""
    means = _mean_by(rows, "rtt_median_ms")
    style.apply_theme()
    fig, ax = style.new_axes("access network", "candidate-pair RTT (ms)",
                             "Real network: RTT direct vs relay")
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
