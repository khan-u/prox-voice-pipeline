import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig09_realnet_rtt as fig09


def _rows():
    return [
        {"access": "cellular", "path": "relay", "rtt_median_ms": 131},
        {"access": "cellular", "path": "relay", "rtt_median_ms": 115},
        {"access": "cellular", "path": "direct", "rtt_median_ms": 101},
        {"access": "cellular", "path": "direct", "rtt_median_ms": 107},
        {"access": "home", "path": "relay", "rtt_median_ms": 80},
        {"access": "home", "path": "direct", "rtt_median_ms": 68},
    ]


def test_mean_by_groups_access_path():
    means = fig09._mean_by(_rows(), "rtt_median_ms")
    assert means[("cellular", "relay")] == 123   # mean(131,115)
    assert means[("cellular", "direct")] == 104  # mean(101,107)
    assert means[("home", "relay")] == 80


def test_plot_has_direct_and_relay_series():
    fig = fig09.plot(_rows())
    ax = fig.axes[0]
    labels = {c.get_label() for c in ax.containers}
    assert labels == {"direct", "relay"}
