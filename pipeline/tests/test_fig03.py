import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig03_glare as fig03


def test_plot_glare_vs_density():
    rows = [
        {"n_peers": 2, "glare_total": 0},
        {"n_peers": 3, "glare_total": 2},
        {"n_peers": 4, "glare_total": 6},
    ]
    fig = fig03.plot(rows)
    ax = fig.axes[0]
    line = ax.get_lines()[0]
    xs = list(line.get_xdata())
    # density is N(N-1): 2, 6, 12
    assert xs == [2, 6, 12]
    assert list(line.get_ydata()) == [0, 2, 6]
