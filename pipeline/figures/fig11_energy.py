"""Figure 11 — device energy/CPU cost of one voice link (two panels).

Measured on two real devices: one voice link adds ~937 mW on the MacBook and
~4.3 CPU points on the iPhone. Each panel contrasts the device baseline against
running with voice. Reads the manually-recorded energy_readings.csv (there is no
gold mart — these come from on-device instrumentation, not the telemetry stream).
"""
import csv
import os

from pipeline.figures import style

INPUT = os.path.join(os.path.dirname(__file__), "inputs", "energy_readings.csv")
OUT = os.path.join(os.path.dirname(__file__), "fig11_energy.png")


def load(path=INPUT):
    """Read the energy readings CSV into typed dict rows."""
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["baseline"] = float(r["baseline"])
        r["with_voice"] = float(r["with_voice"])
    return rows


def plot(rows):
    """Two panels (one per device): baseline vs with-voice bars."""
    import matplotlib.pyplot as plt

    style.apply_theme()
    fig, axes = plt.subplots(1, len(rows), figsize=(7.0, 3.4))
    if len(rows) == 1:
        axes = [axes]
    for ax, r in zip(axes, rows):
        ax.bar(["baseline", "with voice"], [r["baseline"], r["with_voice"]],
               color=[style.PALETTE["gray"], style.PALETTE["blue"]], width=0.6)
        ax.set_title(f"{r['device']} {r['metric']}")
        ax.set_ylabel(r["unit"])
    fig.tight_layout()
    return fig


def main():
    rows = load()
    fig = plot(rows)
    fig.savefig(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
