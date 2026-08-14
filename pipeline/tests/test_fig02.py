import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig02_setup_vs_n as fig02


def test_plot_setup_vs_n_semilog_two_series():
    rows = [
        {"n_peers": 2, "setup_median_ms": 495, "setup_p95_ms": 495},
        {"n_peers": 4, "setup_median_ms": 594, "setup_p95_ms": 597},
        {"n_peers": 8, "setup_median_ms": 769, "setup_p95_ms": 20933},
    ]
    fig = fig02.plot(rows)
    ax = fig.axes[0]
    assert ax.get_yscale() == "log"
    assert len(ax.get_lines()) == 2
    labels = {line.get_label() for line in ax.get_lines()}
    assert labels == {"median", "p95"}
    assert ax.get_xlabel() == "cluster size N"
