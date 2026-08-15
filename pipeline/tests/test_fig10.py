import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig10_realnet_setup as fig10


def test_plot_setup_direct_vs_relay():
    rows = [
        {"access": "cellular", "path": "relay", "setup_median_ms": 6263},
        {"access": "cellular", "path": "relay", "setup_median_ms": 5851},
        {"access": "cellular", "path": "direct", "setup_median_ms": 5715},
        {"access": "home", "path": "relay", "setup_median_ms": 3005},
        {"access": "home", "path": "direct", "setup_median_ms": 3257},
    ]
    means = fig10._mean_by(rows, "setup_median_ms")
    assert means[("cellular", "relay")] == 6057   # mean(6263,5851)
    fig = fig10.plot(rows)
    ax = fig.axes[0]
    labels = {c.get_label() for c in ax.containers}
    assert labels == {"direct", "relay"}
