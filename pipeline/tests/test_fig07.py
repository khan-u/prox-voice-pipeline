import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig07_mobility as fig07


def test_plot_mobility_phases_with_error_bars():
    rows = [{
        "complete_cycles": 4,
        "form_median_ms": 1951, "form_min_ms": 1717, "form_max_ms": 1982,
        "teardown_median_ms": 5116, "teardown_min_ms": 5092, "teardown_max_ms": 5429,
        "reconnect_median_ms": 2433, "reconnect_min_ms": 2375, "reconnect_max_ms": 2688,
    }]
    fig = fig07.plot(rows)
    ax = fig.axes[0]
    # the errorbar plots a central marker line whose y-values are the medians
    ys = [ln.get_ydata() for ln in ax.get_lines() if len(ln.get_ydata()) == 3]
    assert [1951, 5116, 2433] in [list(y) for y in ys]
    labels = [t.get_text() for t in ax.get_xticklabels()]
    assert labels == ["form", "teardown", "reconnect"]
