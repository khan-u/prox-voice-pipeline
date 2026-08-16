import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig12_grace_window as fig12


def test_plot_grace_window_one_series_per_absence():
    rows = [
        {"absence_ms": 6000, "grace_ms": 0, "gap_median_ms": 2117},
        {"absence_ms": 6000, "grace_ms": 1000, "gap_median_ms": 203},
        {"absence_ms": 6000, "grace_ms": 10000, "gap_median_ms": 152},
        {"absence_ms": 12000, "grace_ms": 0, "gap_median_ms": 2617},
        {"absence_ms": 12000, "grace_ms": 1000, "gap_median_ms": 206},
    ]
    fig = fig12.plot(rows)
    ax = fig.axes[0]
    assert ax.get_yscale() == "log"
    labels = {ln.get_label() for ln in ax.get_lines()}
    assert labels == {"6s absence", "12s absence"}
    six = [ln for ln in ax.get_lines() if ln.get_label() == "6s absence"][0]
    # sorted by grace window; the W=0 point carries the full wakeup gap
    assert list(six.get_xdata()) == [0, 1000, 10000]
    assert list(six.get_ydata())[0] == 2117
