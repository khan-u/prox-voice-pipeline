import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig08_audiogap as fig08


def test_plot_audiogap_phases_log_scale():
    rows = [{
        "valid_runs": 10,
        "stop_median_ms": 20, "stop_min_ms": 1, "stop_max_ms": 25,
        "reconnect_median_ms": 2518, "reconnect_min_ms": 2356, "reconnect_max_ms": 3091,
        "total_silence_median_ms": 12729, "total_silence_min_ms": 12559,
        "total_silence_max_ms": 13377,
    }]
    fig = fig08.plot(rows)
    ax = fig.axes[0]
    assert ax.get_yscale() == "log"
    ys = [list(ln.get_ydata()) for ln in ax.get_lines() if len(ln.get_ydata()) == 3]
    assert [20, 2518, 12729] in ys
