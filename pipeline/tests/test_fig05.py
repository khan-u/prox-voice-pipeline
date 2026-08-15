import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig05_uplink as fig05


def test_plot_uplink_per_link_and_total_skips_nulls():
    rows = [
        {"n_peers": 2, "uplink_kbps": 24.0},
        {"n_peers": 3, "uplink_kbps": 24.0},
        {"n_peers": 4, "uplink_kbps": 24.0},
        {"n_peers": 5, "uplink_kbps": None},   # skipped
    ]
    fig = fig05.plot(rows)
    ax = fig.axes[0]
    per_link = [ln for ln in ax.get_lines() if ln.get_label() == "per link"][0]
    total = [ln for ln in ax.get_lines() if ln.get_label() == "node total"][0]
    # null row dropped -> three points
    assert list(per_link.get_xdata()) == [2, 3, 4]
    assert list(per_link.get_ydata()) == [24.0, 24.0, 24.0]
    # total = per-link * (N-1): 24, 48, 72
    assert list(total.get_ydata()) == [24.0, 48.0, 72.0]
