import matplotlib
matplotlib.use("Agg")

from pipeline.figures import fig11_energy as fig11


def test_load_reads_device_readings():
    rows = fig11.load()
    by = {r["device"]: r for r in rows}
    assert by["MacBook"]["with_voice"] - by["MacBook"]["baseline"] == 937.0
    # iPhone CPU delta ~4.3 points
    assert round(by["iPhone"]["with_voice"] - by["iPhone"]["baseline"], 1) == 4.3


def test_plot_has_one_panel_per_device():
    rows = fig11.load()
    fig = fig11.plot(rows)
    assert len(fig.axes) == len(rows)
    # each panel has two bars (baseline, with voice)
    for ax in fig.axes:
        assert len(ax.patches) == 2
