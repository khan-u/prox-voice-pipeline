import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig04_m2e as fig04


def test_plot_m2e_has_target_line_and_series():
    rows = [
        {"n_peers": 2, "m2e_ms": 74},
        {"n_peers": 4, "m2e_ms": 68},
        {"n_peers": 8, "m2e_ms": 76},
    ]
    fig = fig04.plot(rows)
    ax = fig.axes[0]
    # measured series present
    measured = [ln for ln in ax.get_lines() if ln.get_label() == "measured"][0]
    assert list(measured.get_ydata()) == [74, 68, 76]
    # a horizontal target line at 150 ms exists
    target_ys = [ln.get_ydata()[0] for ln in ax.get_lines()
                 if len(set(ln.get_ydata())) == 1]
    assert fig04.TARGET_MS in target_ys
    # y-axis capped near the target so the margin is visible
    assert ax.get_ylim()[1] <= fig04.TARGET_MS * 1.2
